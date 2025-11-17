import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import Window as W
from pyspark.sql.functions import (
    col, avg, stddev, max as spark_max, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from datetime import timedelta


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
GOLD_TABLE_NAME = "bitcoin_gold_data"

# Longest lookback window for aggregation
LONGEST_LOOKBACK_MINUTES = 30

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
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created successfully")
    return spark

def get_last_processed_timestamp(spark):
    """
    Connect to Postgres and get the most recent timestamp
    from the gold table to know where to start processing.
    """
    try:
        # Check if table exists
        pg_tables = spark.read \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .option("query", f"SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '{GOLD_TABLE_NAME}')") \
            .load()
        if not pg_tables.first()["exists"]:
            logger.warning(f"Table {GOLD_TABLE_NAME} does not exist. Starting from scratch.")
            return None

        # Table exists, get max timestamp
        max_ts_df = spark.read \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", GOLD_TABLE_NAME) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load() \
            .agg(spark_max("Timestamp").alias("max_ts"))
        max_ts = max_ts_df.first()["max_ts"]
        if max_ts:
            logger.info(f"Last processed timestamp is: {max_ts}")
            return max_ts
        else:
            logger.info("Gold table is empty. Starting from scratch.")
            return None

    except Exception as e:
        logger.error(f"Error checking Postgres for last timestamp: {e}")
        # If we can't read the table, assume we start from scratch
        return None

def define_parquet_schema():
    """
    Define the exact schema of the Parquet files being written
    by the spark_streaming.py (Stream 1) job.
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


def main():
    logger.info("Starting Gold Table Batch Job...")
    spark = create_spark_session()

    # 1. Get the last processed timestamp from Postgres
    last_ts = get_last_processed_timestamp(spark)
    minio_raw_directory = "streaming_raw"
    minio_input_path = f"s3a://{MINIO_BUCKET}/{minio_raw_directory}"

    try:
        schema = define_parquet_schema()

        # 2. Read from MinIO with Partition Pruning
        raw_df_reader = spark.read.format("parquet").schema(schema)
        if last_ts:
            # Start loading data from before the last timestamp to calculate aggregates
            lookback_start_ts = last_ts - timedelta(minutes=LONGEST_LOOKBACK_MINUTES)
            load_start_date_str = lookback_start_ts.strftime('%Y-%m-%d')
            logger.info(f"Loading data partitioned from date {load_start_date_str} onwards for correct aggregates.")
            raw_df = raw_df_reader.load(minio_input_path).filter(col("date") >= lit(load_start_date_str))
        else:
            logger.info("Processing all data (first run).")
            raw_df = raw_df_reader.load(minio_input_path)

        # Cache the loaded data
        raw_df.cache()

        if raw_df.isEmpty():
            logger.info("No raw data found in MinIO. Sleeping.")
            spark.stop()
            return

        logger.info(f"Loaded {raw_df.count()} rows for lookback.")
        window_spec_5m = W.orderBy("Timestamp").rowsBetween(-4, W.currentRow)
        window_spec_10m = W.orderBy("Timestamp").rowsBetween(-9, W.currentRow)
        window_spec_30m = W.orderBy("Timestamp").rowsBetween(-29, W.currentRow)

        # 4. Apply window functions to the loaded df
        logger.info("Calculating all moving averages...")
        gold_df = raw_df \
            .withColumn("price_ma_5minutes", avg("Close").over(window_spec_5m)) \
            .withColumn("price_volatility_5minutes", stddev("Close").over(window_spec_5m)) \
            .withColumn("volume_btc_ma_5minutes", avg("Volume").over(window_spec_5m)) \
            .withColumn("price_ma_10minutes", avg("Close").over(window_spec_10m)) \
            .withColumn("price_volatility_10minutes", stddev("Close").over(window_spec_10m)) \
            .withColumn("volume_btc_ma_10minutes", avg("Volume").over(window_spec_10m)) \
            .withColumn("price_ma_30minutes", avg("Close").over(window_spec_30m)) \
            .withColumn("price_volatility_30minutes", stddev("Close").over(window_spec_30m)) \
            .withColumn("volume_btc_ma_30minutes", avg("Volume").over(window_spec_30m))

        # 5. Filter for the new rows
        if last_ts:
            final_gold_table = gold_df.filter(col("Timestamp") > lit(last_ts))
        else:
            final_gold_table = gold_df
        final_gold_table.cache()
        if final_gold_table.isEmpty():
            logger.info("No new rows to append after calculation. Sleeping.")
            spark.stop()
            return
        logger.info(f"Found {final_gold_table.count()} new rows to write.")

        # 6. Write the new data
        # 6a. Write to MinIO (Gold Parquet)
        minio_gold_directory = "gold_aggregates"
        minio_gold_path = f"s3a://{MINIO_BUCKET}/{minio_gold_directory}"
        logger.info(f"Appending new Gold table data to MinIO: {minio_gold_path}")
        final_gold_table.write \
            .mode("append") \
            .partitionBy("date") \
            .parquet(minio_gold_path)

        # 6b. Write to Postgres
        logger.info(f"Appending new Gold table data to Postgres: {GOLD_TABLE_NAME}")
        final_gold_table.write \
            .jdbc(url=POSTGRES_URL, table=GOLD_TABLE_NAME, mode="append",
                  properties=POSTGRES_PROPERTIES)
        logger.info("Gold Table Batch Job COMPLETED successfully.")

    except Exception as e:
        logger.error(f"Error in batch aggregation job: {e}")
        if "UNABLE_TO_INFER_SCHEMA" in str(e) or "Path does not exist" in str(e):
             logger.warning("This error is expected if the streaming job hasn't written any data yet. Retrying in 10 seconds.")
        else:
             raise # Other unexpected errors
    finally:
        # Unpersist the cached dataframes
        raw_df.unpersist()
        final_gold_table.unpersist()
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()