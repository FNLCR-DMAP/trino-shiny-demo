"""Tests for RunContext and ctx_from_airflow - Airflow integration logic."""
import pytest
from dmap_data_sdk import ctx_from_airflow, Ref


def test_ctx_from_airflow_with_params():
    """Test ctx_from_airflow() extracts input_branch/target_branch from Airflow params."""
    airflow_ctx = {
        "run_id": "test_run_123",
        "dag_id": "my_dag",
        "task_id": "my_task",
        "params": {
            "input_branch": "feature-x",
            "target_branch": "dev",
            "code_sha": "abc123",
            "code_branch": "feature-x"
        }
    }
    
    ctx = ctx_from_airflow(airflow_ctx)
    
    assert ctx.run_id == "test_run_123"
    assert ctx.dag_id == "my_dag"
    assert ctx.task_id == "my_task"
    assert ctx.default_input_ref.branch == "feature-x"
    assert ctx.target_ref.branch == "dev"
    assert ctx.code_sha == "abc123"
    assert ctx.code_branch == "feature-x"


def test_ctx_from_airflow_defaults():
    """Test default branch is 'main' when no params provided."""
    ctx = ctx_from_airflow({})
    
    assert ctx.default_input_ref.branch == "main"
    assert ctx.target_ref.branch == "main"
    assert ctx.run_id.startswith("manual-")


def test_ctx_from_airflow_snapshot_id_parsing():
    """Test snapshot_id params are parsed as integers."""
    airflow_ctx = {
        "params": {
            "input_snapshot_id": "12345",
            "input_as_of_ms": "1700000000000"
        }
    }
    
    ctx = ctx_from_airflow(airflow_ctx)
    
    assert ctx.default_input_ref.snapshot_id == 12345
    assert ctx.default_input_ref.as_of_ts_millis == 1700000000000
