import os
import time
import json
import requests
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def connect_to_kafka(bootstrap_servers, max_retries=10, retry_delay=5):
    """Connect to Kafka with retries"""
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info(f"Successfully connected to Kafka at {bootstrap_servers}")
            return producer
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                logger.warning(f"Kafka not available. Retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                logger.error("Failed to connect to Kafka after all retries")
                raise

def load_bitcoin_data(file_path):
    """Load Bitcoin historical data from CSV"""
    try:
        logger.info(f"Loading data from {file_path}")
        df = pd.read_csv(file_path)

        df.columns = df.columns.str.strip()
        # Ensure Timestamp column exists
        if 'Timestamp' not in df.columns:
            logger.error("No Timestamp column found in CSV")
            raise ValueError("CSV must have a Timestamp column")
        # Drop rows with missing critical data
        df = df.dropna(subset=['Timestamp'])
        # Add date column for tracking
        df['date'] = pd.to_datetime(df['Timestamp'], unit='s').dt.date
        logger.info(f"Loaded {len(df)} rows from {df['date'].min()} to {df['date'].max()}")
        logger.info(f"Columns: {df.columns.tolist()}")
        return df
    except FileNotFoundError:
        logger.error(f"Data file not found: {file_path}")
        logger.info("Please download the Bitcoin Historical Data from Kaggle and place it in the ./data directory")
        raise
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

def produce_messages(producer, topic, data_df, delay=0.1):
    """Produce messages to Kafka one row at a time"""
    logger.info(f"Starting to stream {len(data_df)} messages to topic '{topic}'")

    current_date = None
    message_count = 0

    for idx, row in data_df.iterrows():
        try:
            # Track date changes
            row_date = row['date']
            if current_date != row_date:
                if current_date is not None:
                    logger.info(f"Finished streaming date: {current_date} ({message_count} messages)")
                current_date = row_date
                message_count = 0
                logger.info(f"Starting to stream date: {current_date}")

            # Convert row to dictionary, handling NaN values
            message = row.drop('date').to_dict()
            message = {k: (None if pd.isna(v) else float(v) if isinstance(v, (int, float)) else v)
                      for k, v in message.items()}

            # Add metadata
            message['event_time'] = message['Timestamp']
            message['producer_time'] = time.time()

            # Send to Kafka
            future = producer.send(topic, value=message)
            future.get(timeout=10)
            message_count += 1
            if message_count % 100 == 0:
                logger.debug(f"Streamed {message_count} messages for date {current_date}")
            time.sleep(delay)

        except Exception as e:
            logger.error(f"Error producing message at index {idx}: {e}")
            continue

    if current_date:
        logger.info(f"Finished streaming date: {current_date} ({message_count} messages)")

    logger.info(f"Finished streaming all {len(data_df)} messages")


def main():
    # Configuration from environment variables
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    topic = os.getenv('KAFKA_TOPIC', 'bitcoin_data')
    data_file = os.getenv('DATA_FILE', '/data/bitcoin_data.csv')
    delay = float(os.getenv('PRODUCER_DELAY', '0.1'))

    logger.info("Bitcoin Data Producer Starting...")
    logger.info(f"Kafka Servers: {kafka_servers}")
    logger.info(f"Topic: {topic}")
    logger.info(f"Data File: {data_file}")
    logger.info(f"Delay: {delay}s per message")

    # Connect to Kafka
    producer = connect_to_kafka(kafka_servers)

    # Load data
    bitcoin_df = load_bitcoin_data(data_file)

    try:
        # Produce messages
        produce_messages(producer, topic, bitcoin_df, delay)

        # Keep producer alive for a bit to ensure all messages are sent
        logger.info("Waiting for final messages to be delivered...")
        time.sleep(5)

    except KeyboardInterrupt:
        logger.info("Producer interrupted by user")
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
        raise
    finally:
        producer.close()
        logger.info("Producer closed")

if __name__ == "__main__":
    main()