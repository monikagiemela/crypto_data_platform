import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, date_format
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)
import boto3
from botocore.client import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "bitcoin_data")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bitcoin-data")
# Note: The docker-compose file provides a more specific path
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/ingest")

def create_spark_session():
    """Create Spark session with S3A configuration for MinIO"""
    logger.info("Creating Spark session for KAFKA->MINIO Stream...")

    spark = SparkSession.builder \
        .appName("BitcoinStreamIngest") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created successfully")
    return spark

def define_schema():
    """
    Define the schema for incoming Bitcoin data.
    Use StringType for all fields from JSON to prevent initial parse failures.
    """
    return StructType([
        StructField("Timestamp", StringType(), True),
        StructField("Open", StringType(), True),
        StructField("High", StringType(), True),
        StructField("Low", StringType(), True),
        StructField("Close", StringType(), True),
        StructField("Volume", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("producer_time", StringType(), True)
    ])

def read_from_kafka(spark, kafka_servers, topic):
    """Read streaming data from Kafka"""
    logger.info(f"Reading from Kafka topic: {topic}")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    return df

def process_raw_stream(df, schema):
    """
    Process the RAW streaming data from Kafka:
    1. Parse JSON
    2. Cast to correct data types (Double, Timestamp)
    3. Filter nulls
    4. Add date for partitioning
    """
    logger.info("Setting up RAW stream processing (Kafka -> MinIO)...")

    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Cast to correct types
    typed_df = parsed_df.withColumn(
        "Timestamp",
        to_timestamp(col("Timestamp").cast(DoubleType()).cast(LongType()))
    ).withColumn("Open", col("Open").cast(DoubleType())) \
     .withColumn("High", col("High").cast(DoubleType())) \
     .withColumn("Low", col("Low").cast(DoubleType())) \
     .withColumn("Close", col("Close").cast(DoubleType())) \
     .withColumn("Volume", col("Volume").cast(DoubleType()))

    filtered_df = typed_df.where(
        col("Timestamp").isNotNull() &
        col("Close").isNotNull() &
        col("Volume").isNotNull()
    )

    final_raw_df = filtered_df.withColumn("date", date_format(col("Timestamp"), "yyyy-MM-dd"))

    return final_raw_df

def write_raw_to_minio(batch_df, batch_id, output_path):
    """
    Micro-batch function to write raw data to MinIO, partitioned by date.
    """
    row_count = batch_df.count()
    if row_count == 0:
        logger.info(f"Batch {batch_id} (Raw) is empty, skipping")
        return

    logger.info(f"Writing {row_count} raw rows for batch {batch_id} to {output_path}")

    try:
        batch_df.write \
            .mode("append") \
            .partitionBy("date") \
            .parquet(output_path)
        logger.info(f"Batch {batch_id} (Raw) written successfully to MinIO")
    except Exception as e:
        logger.error(f"Error writing raw batch {batch_id}: {e}")

def ensure_minio_path_exists(endpoint, access_key, secret_key, bucket, path):
    """
    Uses boto3 to create an S3 path (object) to ensure the "directory" exists
    before Spark tries to read from it.
    """
    full_path_key = f"{path}/"
    logger.info(f"Ensuring MinIO path s3a://{bucket}/{full_path_key} exists using boto3...")
    try:
        client = boto3.client(
            's3',
            endpoint_url=f"http://{endpoint}",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4')
        )
        try:
            client.head_object(Bucket=bucket, Key=full_path_key)
            logger.info(f"Path {full_path_key} already exists in bucket {bucket}.")
        except Exception as e:
            logger.warning(f"Path {full_path_key} does not exist. Creating it...")
            client.put_object(Bucket=bucket, Key=full_path_key, Body='')
            logger.info(f"Path {full_path_key} created successfully.")

    except Exception as e:
        logger.error(f"Could not check or create MinIO path. Error: {e}")
        raise

def main():
    logger.info("Bitcoin Spark Streaming Ingest Starting...")

    spark = create_spark_session()
    schema = define_schema()

    kafka_df = read_from_kafka(spark, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    raw_processed_df = process_raw_stream(kafka_df, schema)

    minio_raw_directory = "streaming_raw"
    minio_output_path = f"s3a://{MINIO_BUCKET}/{minio_raw_directory}"

    # Ensure the path exists BEFORE starting the stream.
    # This is for the batch job that might run before this stream writes.
    ensure_minio_path_exists(
        MINIO_ENDPOINT,
        MINIO_ACCESS_KEY,
        MINIO_SECRET_KEY,
        MINIO_BUCKET,
        minio_raw_directory
    )

    query = raw_processed_df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_raw_to_minio(batch_df, batch_id, minio_output_path)) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .trigger(processingTime="5 seconds") \
        .start()

    logger.info(f"Started Stream 1: Raw Kafka -> MinIO ({minio_output_path})")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stream processing interrupted by user")
    except Exception as e:
        logger.error(f"Error in stream processing: {e}")
        raise
    finally:
        query.stop()
        spark.stop()
        logger.info("Spark streaming ingest session stopped")

if __name__ == "__main__":
    main()