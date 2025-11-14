import os
import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, window, avg, stddev, date_format
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, TimestampType
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bitcoin-data")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# PostgreSQL JDBC URL
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

def create_spark_session():
    """Create Spark session with S3A configuration for MinIO"""
    logger.info("Creating Spark session for Batch Aggregation...")
    
    spark = SparkSession.builder \
        .appName("BitcoinBatchAggregator") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created successfully")
    return spark

def define_parquet_schema():
    """
    Defines the exact schema of the Parquet files being written
    by the spark_streaming.py (Stream 1) job.
    This prevents the "Unable to infer schema" error on empty directories.
    """
    return StructType([
        StructField("Timestamp", TimestampType(), True),
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Volume", DoubleType(), True),
        StructField("event_time", StringType(), True),
        StructField("producer_time", StringType(), True),
        StructField("date", StringType(), True) # This is the partition column
    ])

def calculate_window_aggregations(input_df, window_duration, slide_duration):
    """
    Calculates aggregations on a BATCH DataFrame.
    Returns a batch DataFrame with window boundaries and aggregates.
    """
    table_suffix = window_duration.replace(" ", "")
    
    # We rename the 'window' column immediately to avoid conflicts
    agg_df = input_df \
        .groupBy(window(col("Timestamp"), window_duration, slide_duration).alias(f"window_{table_suffix}")) \
        .agg(
            avg("Close").alias(f"price_ma_{table_suffix}"),
            stddev("Close").alias(f"price_volatility_{table_suffix}"),
            avg("Volume").alias(f"volume_btc_ma_{table_suffix}")
        )
    return agg_df

def main():
    logger.info("Starting Gold Table Batch Aggregation Job...")
    
    spark = create_spark_session()
    
    minio_raw_directory = "streaming_raw"
    minio_input_path = f"s3a://{MINIO_BUCKET}/{minio_raw_directory}"
    
    try:
        logger.info(f"Reading raw data from {minio_input_path}")
        
        # Define the schema manually
        schema = define_parquet_schema()
        
        # Read all raw data from MinIO
        raw_df = spark.read.format("parquet").schema(schema).load(minio_input_path)
        
        if raw_df.isEmpty():
            logger.info("Raw data directory is empty. No data to process. Sleeping.")
            spark.stop()
            return
            
        # --- OPTIMIZATION START ---
        # 1. Add "window key" columns to the raw DataFrame.
        # This lets us use a fast "equi-join" instead of a slow "range-join".
        logger.info("Generating window keys on raw data...")
        raw_with_keys = raw_df \
            .withColumn("window_key_5m", window(col("Timestamp"), "5 minutes", "1 minute")) \
            .withColumn("window_key_10m", window(col("Timestamp"), "10 minutes", "1 minute")) \
            .withColumn("window_key_30m", window(col("Timestamp"), "30 minutes", "1 minute"))
        
        # 2. Calculate aggregations
        logger.info("Calculating 5-minute aggregates...")
        agg_5min_df = calculate_window_aggregations(raw_with_keys, "5 minutes", "1 minute")
        
        logger.info("Calculating 10-minute aggregates...")
        agg_10min_df = calculate_window_aggregations(raw_with_keys, "10 minutes", "1 minute")

        logger.info("Calculating 30-minute aggregates...")
        agg_30min_df = calculate_window_aggregations(raw_with_keys, "30 minutes", "1 minute")

        # 3. Join the aggregations using the new keys (Equi-Join)
        logger.info("Joining raw data with aggregates (using equi-join)...")
        
        gold_df = raw_with_keys \
            .join(
                agg_5min_df,
                raw_with_keys["window_key_5m"].start == agg_5min_df["window_5minutes"].start,
                "left"
            ) \
            .join(
                agg_10min_df,
                raw_with_keys["window_key_10m"].start == agg_10min_df["window_10minutes"].start,
                "left"
            ) \
            .join(
                agg_30min_df,
                raw_with_keys["window_key_30m"].start == agg_30min_df["window_30minutes"].start,
                "left"
            )
        # --- OPTIMIZATION END ---

        # Select and clean up columns for the final table
        final_gold_table = gold_df.select(
            "Timestamp",
            "date",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "price_ma_5minutes",
            "price_volatility_5minutes",
            "volume_btc_ma_5minutes",
            "price_ma_10minutes",
            "price_volatility_10minutes",
            "volume_btc_ma_10minutes",
            "price_ma_30minutes",
            "price_volatility_30minutes",
            "volume_btc_ma_30minutes"
        ).orderBy("Timestamp")

        # Write the final table to Postgres, overwriting the old one
        output_table = "bitcoin_gold_data"
        logger.info(f"Writing final Gold table to Postgres: {output_table}")
        
        final_gold_table.write \
            .jdbc(url=POSTGRES_URL, table=output_table, mode="overwrite", 
                  properties=POSTGRES_PROPERTIES)
        
        logger.info("Gold Table Batch Aggregation Job COMPLETED successfully.")

    except Exception as e:
        logger.error(f"Error in batch aggregation job: {e}")
        # Check for the "schema not found" error specifically
        if "UNABLE_TO_INFER_SCHEMA" in str(e) or "Path does not exist" in str(e):
             logger.warning("This error is expected if the streaming job hasn't written any data yet. Retrying in 1 minute.")
        else:
             raise # Re-raise other unexpected errors
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()