
import os
import pytest
from unittest.mock import MagicMock, patch

from spark.spark_streaming import define_schema, write_raw_to_minio, ensure_minio_path_exists, main


def test_define_schema():
    """Test schema definition for Kafka messages"""
    from pyspark.sql.types import StructType, StringType
    schema = define_schema()
    assert isinstance(schema, StructType)
    assert len(schema.fields) == 8
    assert schema.fields[0].name == "Timestamp"
    assert isinstance(schema.fields[0].dataType, StringType)

def test_write_raw_to_minio_empty_batch(mocker):
    """Test that an empty batch is skipped"""
    mock_df = MagicMock()
    mock_df.count.return_value = 0
    mock_logger = mocker.patch('spark.spark_streaming.logger')

    write_raw_to_minio(mock_df, 1, "fake_path")

    mock_logger.info.assert_any_call("Batch 1 (Raw) is empty, skipping")
    assert not mock_df.write.mode.called

def test_write_raw_to_minio(mocker):
    """Test writing a batch to MinIO"""
    mock_df = MagicMock()
    mock_df.count.return_value = 1

    write_raw_to_minio(mock_df, 1, "fake_path")

    assert mock_df.write.mode.called
    assert mock_df.write.mode.return_value.partitionBy.called
    assert mock_df.write.mode.return_value.partitionBy.return_value.parquet.called

def test_ensure_minio_path_exists(mocker):
    """Test the MinIO path existence check"""
    mock_boto_client = mocker.patch('boto3.client')
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    # Simulate path not existing
    mock_s3.head_object.side_effect = Exception("Not Found")
    ensure_minio_path_exists("endpoint", "access", "secret", "bucket", "path")
    mock_s3.put_object.assert_called_once_with(Bucket='bucket', Key='path/', Body='')

    # Reset and simulate path existing
    mock_s3.reset_mock()
    mock_s3.head_object.side_effect = None
    ensure_minio_path_exists("endpoint", "access", "secret", "bucket", "path")
    assert not mock_s3.put_object.called

@patch('spark.spark_streaming.create_spark_session')
@patch('spark.spark_streaming.read_from_kafka')
@patch('spark.spark_streaming.process_raw_stream')
@patch('spark.spark_streaming.ensure_minio_path_exists')
def test_main(mock_ensure_path, mock_process_stream, mock_read_kafka, mock_create_session):
    """Integration test for the main streaming job"""
    mock_spark = MagicMock()
    mock_create_session.return_value = mock_spark
    mock_kafka_df = MagicMock()
    mock_read_kafka.return_value = mock_kafka_df
    mock_processed_df = MagicMock()
    mock_process_stream.return_value = mock_processed_df

    # Mock the streaming query
    mock_query = MagicMock()
    mock_processed_df.writeStream.foreachBatch.return_value.option.return_value.trigger.return_value.start.return_value = mock_query
    mock_query.awaitTermination.side_effect = KeyboardInterrupt

    try:
        main()
    except KeyboardInterrupt:
        pass

    assert mock_create_session.called
    assert mock_read_kafka.called
    assert mock_process_stream.called
    assert mock_ensure_path.called
    assert mock_processed_df.writeStream.foreachBatch.called
    assert mock_query.awaitTermination.called
    assert mock_query.stop.called
    assert mock_spark.stop.called
