# DMAP Data SDK - Unit Tests

## Overview

This test suite covers the **critical logic** in the SDK without mocking actual Spark/Iceberg operations.

### What's Tested

**Must-Have Tests (8):**
1. `test_read_table_option_precedence()` - Snapshot ID > timestamp > branch > tag ordering
2. `test_write_table_branch_main_no_option()` - Main branch needs no option (bug fix)
3. `test_write_table_branch_feature_sets_option()` - Feature branches set option
4. `test_resolve_snapshot_id_branch_sql()` - Branch lookup SQL correctness
5. `test_pipeline_multiple_reads_accumulated()` - Lineage tracks all inputs
6. `test_pipeline_lineage_has_snapshot_ids()` - Input/output snapshot IDs captured
7. `test_ctx_from_airflow_with_params()` - Airflow branch params extracted
8. `test_ref_pretty_formatting()` - Ref.pretty() used in lineage JSON

**Should-Have Tests (5):**
9. `test_write_table_snapshot_properties()` - Run context stamped in metadata
10. `test_resolve_snapshot_id_timestamp_sql()` - Timestamp query correctness
11. `test_read_table_snapshot_id_option()` - Snapshot ID read works
12. `test_read_table_branch_option()` - Branch read works  
13. `test_pipeline_set_target_ref()` - Branch switching works

## Running Tests

```bash
# Install dev dependencies
cd dmap-data-sdk
pip install -e ".[dev]"

# Run all tests
pytest

# Run with coverage
pytest --cov=dmap_data_sdk --cov-report=html

# Run specific test file
pytest tests/test_iceberg_platform.py

# Run specific test
pytest tests/test_iceberg_platform.py::TestWriteTable::test_write_table_branch_main_no_option

# Verbose output
pytest -v
```

## Coverage Target

**Goal: 70%+ coverage** on real logic:
- ✅ Ref formatting and validation
- ✅ RunContext merging and Airflow parsing
- ✅ Iceberg option precedence logic
- ✅ Branch "main" special case handling
- ✅ Snapshot property construction
- ✅ Lineage input accumulation
- ✅ SQL query building

## What's NOT Tested

These require integration tests:
- ❌ Actual Spark DataFrame operations
- ❌ Real Iceberg table reads/writes
- ❌ Hive Metastore connections
- ❌ Trino queries
- ❌ Docker container interactions

See `tests/sdk-integration/` in the parent repo for integration tests.

## Test Structure

```
tests/
├── __init__.py
├── conftest.py              # Pytest fixtures (mock Spark, DataFrame, config)
├── test_ref.py              # Ref class logic (3 tests)
├── test_run_context.py      # Airflow context parsing (3 tests)
├── test_iceberg_platform.py # Platform read/write/resolve (10 tests)
└── test_data_pipeline.py    # Pipeline lineage tracking (3 tests)
```

## Continuous Integration

Add to `.github/workflows/test.yml`:

```yaml
name: Unit Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - run: pip install -e ".[dev]"
      - run: pytest --cov=dmap_data_sdk --cov-report=xml
      - uses: codecov/codecov-action@v3
```

## Writing New Tests

Focus on **real logic**, avoid mock theater:

**✅ Good test** - Tests actual logic:
```python
def test_ref_precedence():
    # Tests real if/elif precedence in code
    ref = Ref(snapshot_id=123, branch="main")
    assert ref.pretty() == "snapshot:123"  # snapshot wins
```

**❌ Bad test** - Just tests the mock:
```python
def test_read_calls_spark(mock_spark):
    # This is useless - we're just verifying our mock works
    platform.read_table("table")
    mock_spark.table.assert_called_once()  # So what?
```

## FAQ

**Q: Why not 100% coverage?**  
A: The SDK is a thin wrapper around Spark/Iceberg. Most of the "code" is just calling Spark APIs, which don't have testable logic without real Spark.

**Q: Why mock Spark at all?**  
A: We mock the Spark *objects*, but test the *logic* around them (option precedence, property construction, SQL building).

**Q: Where are the integration tests?**  
A: In the parent repository at `tests/sdk-integration/`. Run with `make test-sdk-all`.

## Test Output Example

```
tests/test_ref.py::test_ref_pretty_formatting PASSED           [ 7%]
tests/test_ref.py::test_ref_is_unset PASSED                    [14%]
tests/test_run_context.py::test_ctx_from_airflow_with_params PASSED [21%]
tests/test_iceberg_platform.py::TestReadTable::test_read_table_option_precedence PASSED [28%]
tests/test_iceberg_platform.py::TestWriteTable::test_write_table_branch_main_no_option PASSED [35%]
tests/test_data_pipeline.py::test_pipeline_lineage_has_snapshot_ids PASSED [42%]

---------- coverage: platform linux, python 3.10.12 -----------
Name                                    Stmts   Miss  Cover
-----------------------------------------------------------
src/dmap_data_sdk/__init__.py              12      0   100%
src/dmap_data_sdk/data_utils.py          298     89    70%
-----------------------------------------------------------
TOTAL                                     310     89    71%
```
