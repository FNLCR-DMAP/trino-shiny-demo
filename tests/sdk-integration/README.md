# DMAP Data SDK - Integration Testing Guide

This directory contains end-to-end integration tests for the DMAP Data SDK using a real Docker-based Iceberg/Trino/Spark stack.

## Prerequisites

1. **Docker & Docker Compose** running
2. **Infrastructure started**: `make up` or `docker-compose up -d`
3. **SDK installed** in the Spark container (automated by test scripts)

## Quick Start

```bash
# Full test suite (recommended)
make test-sdk-all

# Or run individual steps:
make init-sdk-data    # 1. Hydrate test data
make test-sdk         # 2. Run SDK pipeline test
make validate-sdk     # 3. Validate results
```

## What Gets Tested

### 1. SDK Installation
- Copies SDK package to Spark container
- Installs via pip
- Verifies imports work

### 2. Data Pipeline
- Reads from `iceberg.data_pipeline.full_name_input` (10 test records)
- Transforms data (name parsing, cleaning)
- Writes to `iceberg.data_pipeline.ip_sum_output_sdk_test`

### 3. Lineage Tracking
- Captures input/output table relationships
- Records snapshot IDs for time travel
- Stores metadata in `iceberg.audit.etl_lineage`

### 4. Iceberg Integration
- Table creation via `saveAsTable()`
- Snapshot management
- Metadata properties (transform name, run ID)

## Test Scripts

### `init-test-data.sh`
Hydrates test data needed for SDK integration tests.

**What it does:**
- Creates `iceberg.data_pipeline` schema
- Creates `iceberg.audit` schema (for lineage)
- Creates and populates `full_name_input` table with 10 test records
- Verifies data via Trino

**Usage:**
```bash
./tests/sdk-integration/init-test-data.sh
# Or: make init-sdk-data
```

**Output:**
- ✅ 10 test records in `iceberg.data_pipeline.full_name_input`
- ✅ Schemas ready for lineage tracking

---

### `test-sdk.sh`
Runs the SDK pipeline test in the Spark container.

**What it does:**
1. Copies SDK to container
2. Installs SDK via pip
3. Copies pipeline config
4. Runs example pipeline (`examples/ip_sum_example.py`)
5. Transforms and writes output

**Usage:**
```bash
./tests/sdk-integration/test-sdk.sh
# Or: make test-sdk
```

**Output:**
- ✅ SDK installed
- ✅ Pipeline executed
- ✅ Output table created with transformed data

---

### `validate-sdk.sh`
Validates SDK installation and results with 10 comprehensive checks.

**What it does:**
1. Checks Docker stack health
2. Verifies SDK installation
3. Tests SDK imports
4. Validates input data (count & sample)
5. Validates output data (count & sample)
6. Checks table schema
7. Inspects Iceberg snapshots
8. Verifies snapshot metadata
9. Checks lineage table existence
10. Validates lineage records

**Usage:**
```bash
./tests/sdk-integration/validate-sdk.sh
# Or: make validate-sdk
```

**Output:**
- ✅ 10/10 validation checks passed
- Detailed results for each check

## Manual Testing

### Query Results with Trino

```bash
# Connect to Trino CLI
docker exec -it trino-cli trino --server http://trino:8080

# View input data
SELECT * FROM iceberg.data_pipeline.full_name_input LIMIT 10;

# View output data
SELECT * FROM iceberg.data_pipeline.ip_sum_output_sdk_test LIMIT 10;

# Check lineage
SELECT 
    recorded_at,
    transform,
    target_table,
    target_snapshot_id,
    inputs_json
FROM iceberg.audit.etl_lineage
ORDER BY recorded_at DESC;

# View Iceberg snapshots
SELECT snapshot_id, operation, committed_at, summary
FROM iceberg.data_pipeline."ip_sum_output_sdk_test$snapshots"
ORDER BY committed_at DESC;
```

### Run Custom Pipeline

```bash
# Copy your pipeline to Spark container
docker cp my_pipeline.py spark-iceberg:/opt/my_pipeline.py

# Run with SDK
docker exec spark-iceberg spark-submit \
  --master local[*] \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 \
  /opt/my_pipeline.py
```

## Test Data Schema

### Input: `full_name_input`
```sql
id BIGINT
full_name VARCHAR
```

10 sample records with various name formats for testing.

### Output: `ip_sum_output_sdk_test`
```sql
id BIGINT
original_full_name VARCHAR
cleaned_name VARCHAR
name_parts_count INTEGER
name_length INTEGER
processed_at TIMESTAMP
```

Transformed data with name parsing and analysis.

### Lineage: `etl_lineage`
```sql
recorded_at TIMESTAMP
transform VARCHAR
run_id VARCHAR
dag_id VARCHAR
task_id VARCHAR
code_branch VARCHAR
target_table VARCHAR
target_snapshot_id BIGINT
inputs_json VARCHAR
platform VARCHAR
```

## Troubleshooting

### Test Fails: "Spark container not running"
```bash
docker-compose up -d spark
docker ps | grep spark
```

### Test Fails: "Trino not responding"
```bash
docker-compose restart trino
sleep 10
docker exec trino trino --execute "SELECT 1"
```

### Data Already Exists
```bash
# Clean and reinitialize
docker exec trino trino --execute "DROP TABLE IF EXISTS iceberg.data_pipeline.ip_sum_output_sdk_test"
docker exec trino trino --execute "DROP TABLE IF EXISTS iceberg.audit.etl_lineage"
make init-sdk-data
```

### SDK Installation Issues
```bash
# Reinstall SDK
docker cp ./dmap-data-sdk spark-iceberg:/opt/dmap-data-sdk
docker exec spark-iceberg pip install --force-reinstall /opt/dmap-data-sdk
```

### View Container Logs
```bash
docker logs spark-iceberg --tail 50
docker logs trino --tail 50
docker logs hive-metastore --tail 50
```

## Expected Results

After running `make test-sdk-all`, you should see:

```
✅ Test Data Initialized
   • 10 records in full_name_input
   • Schemas created

✅ SDK Test Completed
   • SDK installed in container
   • Pipeline executed successfully
   • Output table created

✅ Validation Passed (10/10 checks)
   • Docker stack healthy
   • SDK imports working
   • Data integrity verified
   • Lineage captured
   • Snapshots recorded
```

## CI/CD Integration

Add to your CI pipeline:

```yaml
# .github/workflows/test-sdk.yml
- name: Start Docker Stack
  run: docker-compose up -d
  
- name: Wait for Services
  run: sleep 30
  
- name: Run SDK Integration Tests
  run: make test-sdk-all
  
- name: Cleanup
  run: docker-compose down -v
```

## Next Steps

1. **Modify test pipeline**: Edit `dmap-data-sdk/examples/ip_sum_example.py`
2. **Add more tests**: Create additional test scripts in this directory
3. **Test with branches**: Modify tests to use Iceberg branches
4. **Performance testing**: Add larger datasets for scale testing

## Related Documentation

- **SDK Usage**: `dmap-data-sdk/README.md`
- **SDK Validation**: `dmap-data-sdk/VALIDATION.md`
- **Main Project**: `../README.md`

---

**Last Updated**: November 19, 2025  
**SDK Version**: 1.0.0
