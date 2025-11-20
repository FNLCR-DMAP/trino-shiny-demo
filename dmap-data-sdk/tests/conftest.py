"""Pytest fixtures for DMAP Data SDK tests."""
import pytest
from unittest.mock import Mock, MagicMock


@pytest.fixture
def mock_spark():
    """Mock SparkSession for testing without real Spark."""
    spark = MagicMock()
    spark.sql.return_value = Mock()
    spark.table.return_value = Mock()
    
    # Mock read builder chain
    mock_format = Mock()
    mock_reader = Mock()
    mock_format.return_value = mock_reader
    spark.read.format = mock_format
    
    return spark


@pytest.fixture
def mock_dataframe():
    """Mock DataFrame with common operations."""
    df = MagicMock()
    df.count.return_value = 100
    df.columns = ["id", "name", "value"]
    
    # Mock write builder chain
    mock_write = Mock()
    mock_format = Mock()
    mock_mode = Mock()
    mock_options = Mock()
    
    mock_write.format.return_value = mock_format
    mock_format.mode.return_value = mock_mode
    mock_mode.options.return_value = mock_options
    
    df.write = mock_write
    
    return df


@pytest.fixture
def sample_config(tmp_path):
    """Create a sample pipeline_config.yaml file."""
    config_file = tmp_path / "pipeline_config.yaml"
    config_file.write_text("""
platform:
  type: iceberg
  spark_config:
    spark.sql.adaptive.enabled: "true"
    spark.custom.property: "test_value"

lineage:
  enabled: true
  table: audit.test_lineage

environment: test
""")
    return str(config_file)
