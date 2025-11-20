"""Tests for DataPipeline - lineage tracking logic."""
import pytest
from unittest.mock import Mock, MagicMock
from dmap_data_sdk import DataPipeline, InputLineage, Ref, RunContext, WriteResult


def test_pipeline_multiple_reads_accumulated():
    """MUST HAVE: Multiple reads accumulate in _inputs for lineage."""
    mock_spark = MagicMock()
    mock_df = Mock()
    mock_df.count.return_value = 100
    
    # Create pipeline with mocked Spark (bypass session creation)
    pipeline = DataPipeline.__new__(DataPipeline)
    pipeline.transform_name = "test_transform"
    pipeline.spark = mock_spark
    pipeline._inputs = []
    pipeline.ctx = RunContext(run_id="test")
    
    # Mock platform methods
    from dmap_data_sdk import IcebergSparkPlatform
    platform = IcebergSparkPlatform(mock_spark)
    platform.read_table = Mock(return_value=mock_df)
    platform.resolve_snapshot_id = Mock(side_effect=[111, 222])
    pipeline.platform = platform
    
    # Read two tables
    pipeline.read_table("table1")
    pipeline.read_table("table2")
    
    # Verify inputs were accumulated
    assert len(pipeline._inputs) == 2
    assert pipeline._inputs[0].input_table == "table1"
    assert pipeline._inputs[0].input_snapshot_id == 111
    assert pipeline._inputs[1].input_table == "table2"
    assert pipeline._inputs[1].input_snapshot_id == 222


def test_pipeline_lineage_has_snapshot_ids():
    """MUST HAVE: LineageRecord contains both input and output snapshot IDs."""
    mock_spark = MagicMock()
    mock_df = Mock()
    mock_df.count.return_value = 50
    
    # Setup pipeline
    pipeline = DataPipeline.__new__(DataPipeline)
    pipeline.transform_name = "test_transform"
    pipeline.spark = mock_spark
    pipeline.ctx = RunContext(run_id="test123", dag_id="my_dag")
    pipeline._inputs = [
        InputLineage("input1", Ref(branch="main"), 111),
        InputLineage("input2", Ref(snapshot_id=222), 222)
    ]
    
    # Mock platform and lineage sink
    from dmap_data_sdk import IcebergSparkPlatform, NoopLineageSink
    platform = IcebergSparkPlatform(mock_spark)
    write_result = WriteResult("output_table", 333, Ref(branch="main"), {})
    platform.write_table = Mock(return_value=write_result)
    pipeline.platform = platform
    
    lineage_sink = NoopLineageSink()
    lineage_sink.record = Mock()
    pipeline.lineage_sink = lineage_sink
    
    # Write table (triggers lineage recording)
    pipeline.write_table(mock_df, "output_table")
    
    # Verify lineage record
    recorded = lineage_sink.record.call_args[0][0]
    assert len(recorded.inputs) == 2
    assert recorded.inputs[0].input_snapshot_id == 111
    assert recorded.inputs[1].input_snapshot_id == 222
    assert recorded.target_snapshot_id == 333
    assert recorded.run_id == "test123"
    assert recorded.dag_id == "my_dag"
    assert recorded.transform == "test_transform"


def test_pipeline_set_target_ref():
    """SHOULD HAVE: set_target_ref() updates ctx.target_ref for branch switching."""
    mock_spark = MagicMock()
    
    # Create minimal pipeline
    pipeline = DataPipeline.__new__(DataPipeline)
    pipeline.spark = mock_spark
    pipeline.ctx = RunContext(run_id="test", target_ref=Ref(branch="main"))
    
    # Change target ref
    pipeline.set_target_ref(Ref(branch="feature-x"))
    
    # Verify context updated
    assert pipeline.ctx.target_ref.branch == "feature-x"
    assert pipeline.ctx.run_id == "test"  # Other fields preserved
