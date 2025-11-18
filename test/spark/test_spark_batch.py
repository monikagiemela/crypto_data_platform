
import os
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

from spark.spark_batch import define_parquet_schema


def test_define_parquet_schema():
    """Test Parquet schema definition"""
    from pyspark.sql.types import StructType, TimestampType
    schema = define_parquet_schema()
    assert isinstance(schema, StructType)
    assert len(schema.fields) == 9
    assert schema.fields[0].name == "Timestamp"
    assert isinstance(schema.fields[0].dataType, TimestampType)
