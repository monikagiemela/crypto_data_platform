
import os
import pytest
from unittest.mock import MagicMock, call
import pandas as pd
from kafka.errors import NoBrokersAvailable
import time

from producer.producer import connect_to_kafka, load_bitcoin_data, produce_messages

def test_connect_to_kafka_success(mocker):
    """Test successful connection to Kafka"""
    mock_kafka_producer = mocker.patch('producer.producer.KafkaProducer')
    mock_producer_instance = MagicMock()
    mock_kafka_producer.return_value = mock_producer_instance
    
    producer = connect_to_kafka('fake_server:9092')
    
    mock_kafka_producer.assert_called_once_with(
        bootstrap_servers='fake_server:9092',
        value_serializer=mocker.ANY,
        acks='all',
        retries=3
    )
    assert producer == mock_producer_instance

def test_connect_to_kafka_retry(mocker):
    """Test Kafka connection with retries"""
    mock_kafka_producer = mocker.patch('producer.producer.KafkaProducer')
    mock_sleep = mocker.patch('time.sleep')
    mock_kafka_producer.side_effect = [NoBrokersAvailable, NoBrokersAvailable, MagicMock()]
    
    connect_to_kafka('fake_server:9092', max_retries=3, retry_delay=1)
    
    assert mock_kafka_producer.call_count == 3
    assert mock_sleep.call_count == 2

def test_connect_to_kafka_failure(mocker):
    """Test Kafka connection failure after all retries"""
    mock_kafka_producer = mocker.patch('producer.producer.KafkaProducer')
    mock_sleep = mocker.patch('time.sleep')
    mock_kafka_producer.side_effect = NoBrokersAvailable
    
    with pytest.raises(NoBrokersAvailable):
        connect_to_kafka('fake_server:9092', max_retries=3, retry_delay=1)
        
    assert mock_kafka_producer.call_count == 3
    assert mock_sleep.call_count == 2

def test_load_bitcoin_data_success():
    """Test loading data from a valid CSV file"""
    # Create a dummy CSV file
    data = {'Timestamp': [1, 2, 3], 'Open': [100, 200, 300]}
    df = pd.DataFrame(data)
    dummy_file_path = 'dummy_data.csv'
    df.to_csv(dummy_file_path, index=False)
    
    loaded_df = load_bitcoin_data(dummy_file_path)
    
    assert len(loaded_df) == 3
    assert 'date' in loaded_df.columns
    
    # Clean up the dummy file
    os.remove(dummy_file_path)

def test_load_bitcoin_data_no_file():
    """Test loading data from a non-existent file"""
    with pytest.raises(FileNotFoundError):
        load_bitcoin_data('non_existent_file.csv')

def test_load_bitcoin_data_no_timestamp():
    """Test loading data from a CSV without a Timestamp column"""
    # Create a dummy CSV file without Timestamp
    data = {'Open': [100, 200, 300]}
    df = pd.DataFrame(data)
    dummy_file_path = 'dummy_data_no_timestamp.csv'
    df.to_csv(dummy_file_path, index=False)
    
    with pytest.raises(ValueError):
        load_bitcoin_data(dummy_file_path)
        
    # Clean up the dummy file
    os.remove(dummy_file_path)

def test_produce_messages(mocker):
    """Test producing messages to Kafka"""
    mock_kafka_producer = mocker.patch('producer.producer.KafkaProducer')
    mock_producer_instance = MagicMock()
    
    data = {'Timestamp': [time.time(), time.time()], 'Value': [1, 2]}
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['Timestamp'], unit='s').dt.date
    
    produce_messages(mock_producer_instance, 'test_topic', df, delay=0)
    
    assert mock_producer_instance.send.call_count == 2
    
    # Check the content of the first message
    first_call_args = mock_producer_instance.send.call_args_list[0]
    topic_sent = first_call_args[0][0]
    message_sent = first_call_args[1]['value']
    
    assert topic_sent == 'test_topic'
    assert message_sent['Value'] == 1
    assert 'event_time' in message_sent
    assert 'producer_time' in message_sent
