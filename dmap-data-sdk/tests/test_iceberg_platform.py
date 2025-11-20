"""Tests for IcebergSparkPlatform - critical read/write/resolve logic."""
import pytest
from unittest.mock import Mock, MagicMock, call
from dmap_data_sdk import IcebergSparkPlatform, Ref, RunContext


class TestReadTable:
    """Tests for read_table() option setting logic."""
    
    def test_read_table_option_precedence(self):
        """MUST HAVE: Snapshot ID > timestamp > branch > tag precedence."""
        mock_spark = MagicMock()
        mock_reader = Mock()
        mock_spark.read.format.return_value = mock_reader
        
        platform = IcebergSparkPlatform(mock_spark)
        
        # Both snapshot_id and timestamp set, but snapshot_id should win
        platform.read_table("my_table", Ref(snapshot_id=999, as_of_ts_millis=1000))
        
        # Should call with snapshot-id, NOT as-of-timestamp
        mock_reader.option.assert_called_with("snapshot-id", 999)
        
    def test_read_table_snapshot_id_option(self):
        """SHOULD HAVE: Verify snapshot-id option is set correctly."""
        mock_spark = MagicMock()
        mock_reader = Mock()
        mock_spark.read.format.return_value = mock_reader
        
        platform = IcebergSparkPlatform(mock_spark)
        platform.read_table("my_table", Ref(snapshot_id=12345))
        
        mock_spark.read.format.assert_called_with("iceberg")
        mock_reader.option.assert_called_with("snapshot-id", 12345)
    
    def test_read_table_branch_option(self):
        """SHOULD HAVE: Verify branch option set when Ref has branch."""
        mock_spark = MagicMock()
        mock_reader = Mock()
        mock_spark.read.format.return_value = mock_reader
        
        platform = IcebergSparkPlatform(mock_spark)
        platform.read_table("my_table", Ref(branch="feature-x"))
        
        mock_reader.option.assert_called_with("branch", "feature-x")


class TestWriteTable:
    """Tests for write_table() branching and property logic."""
    
    def test_write_table_branch_main_no_option(self):
        """MUST HAVE: Critical bug fix - main branch needs no option."""
        mock_spark = MagicMock()
        mock_df = Mock()
        mock_writer = Mock()
        
        # Setup write chain
        mock_df.write.format.return_value.mode.return_value.options.return_value = mock_writer
        
        platform = IcebergSparkPlatform(mock_spark)
        ctx = RunContext(run_id="test", target_ref=Ref(branch="main"))
        
        platform.write_table(mock_df, "my_table", ctx)
        
        # Verify branch option was NOT called
        mock_writer.option.assert_not_called()
    
    def test_write_table_branch_feature_sets_option(self):
        """MUST HAVE: Feature branches must set branch option."""
        mock_spark = MagicMock()
        mock_df = Mock()
        mock_writer = Mock()
        
        mock_df.write.format.return_value.mode.return_value.options.return_value = mock_writer
        
        platform = IcebergSparkPlatform(mock_spark)
        ctx = RunContext(run_id="test", target_ref=Ref(branch="feature-x"))
        
        platform.write_table(mock_df, "my_table", ctx)
        
        # Verify branch option WAS set
        mock_writer.option.assert_called_with("branch", "feature-x")
    
    def test_write_table_snapshot_properties(self):
        """SHOULD HAVE: Verify snapshot properties stamped in metadata."""
        mock_spark = MagicMock()
        mock_df = Mock()
        
        platform = IcebergSparkPlatform(mock_spark)
        ctx = RunContext(
            run_id="run123",
            dag_id="my_dag",
            task_id="my_task",
            code_sha="abc123",
            code_branch="main",
            params={"key": "value"}
        )
        
        platform.write_table(mock_df, "my_table", ctx)
        
        # Capture the options() call
        call_args = mock_df.write.format().mode().options.call_args
        props = call_args[1] if call_args else {}
        
        # Verify property formatting
        assert props.get("snapshot-property.run_id") == "run123"
        assert props.get("snapshot-property.dag_id") == "my_dag"
        assert props.get("snapshot-property.task_id") == "my_task"
        assert props.get("snapshot-property.code_sha") == "abc123"
        assert props.get("snapshot-property.code_branch") == "main"
        assert '"key": "value"' in props.get("snapshot-property.params", "")


class TestResolveSnapshotId:
    """Tests for resolve_snapshot_id() SQL query logic."""
    
    def test_resolve_snapshot_id_branch_sql(self):
        """MUST HAVE: Verify SQL query construction for branch lookup."""
        mock_spark = Mock()
        mock_result = Mock()
        mock_result.first.return_value = [54321]
        mock_spark.sql.return_value = mock_result
        
        platform = IcebergSparkPlatform(mock_spark)
        result = platform.resolve_snapshot_id("my_table", Ref(branch="feature-x"))
        
        # Verify SQL query
        expected_sql = "SELECT snapshot_id FROM my_table.refs WHERE name='feature-x'"
        mock_spark.sql.assert_called_with(expected_sql)
        assert result == 54321
    
    def test_resolve_snapshot_id_timestamp_sql(self):
        """SHOULD HAVE: Verify timestamp query correctness."""
        mock_spark = Mock()
        mock_result = Mock()
        mock_result.first.return_value = [99999]
        mock_spark.sql.return_value = mock_result
        
        platform = IcebergSparkPlatform(mock_spark)
        result = platform.resolve_snapshot_id("my_table", Ref(as_of_ts_millis=1700000000000))
        
        # Verify SQL contains timestamp filter
        sql_call = mock_spark.sql.call_args[0][0]
        assert "my_table.history" in sql_call
        assert "timestamp_millis(1700000000000)" in sql_call
        assert "made_current_at <=" in sql_call
        assert result == 99999
    
    def test_resolve_snapshot_id_returns_none(self):
        """SHOULD HAVE: Graceful failure when branch/tag not found."""
        mock_spark = Mock()
        mock_result = Mock()
        mock_result.first.return_value = None  # No matching row
        mock_spark.sql.return_value = mock_result
        
        platform = IcebergSparkPlatform(mock_spark)
        result = platform.resolve_snapshot_id("my_table", Ref(branch="nonexistent"))
        
        assert result is None
