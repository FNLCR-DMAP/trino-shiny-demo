# SDK Validation Guide

## Quick Validation
Run the comprehensive validation script:
```bash
./scripts/validate-sdk.sh
```

This checks all 10 validation steps automatically.

---

## Manual Step-by-Step Validation

### 1. Check Docker Stack Health
```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```
**Expected**: 5+ containers running (postgres, hive-metastore, trino, spark-iceberg)

---

### 2. Verify SDK Installation
```bash
docker exec spark-iceberg pip3 list | grep dmap
```
**Expected**: `dmap-data-sdk 1.0.0`

---

### 3. Test SDK Imports
```bash
docker exec spark-iceberg python3 -c "from dmap_data_sdk import DataPipeline, Ref, PlatformFactory; print('✅ Success')"
```
**Expected**: `✅ Success`

---

### 4. Check Input Data
```bash
docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.data_pipeline.full_name_input"
```
**Expected**: `"10"`

---

### 5. View Input Data Sample
```bash
docker exec trino trino --execute "SELECT * FROM iceberg.data_pipeline.full_name_input LIMIT 3"
```
**Expected**: 3 rows with id and full_name columns

---

### 6. Check Output Table
```bash
docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.data_pipeline.ip_sum_output_sdk_test"
```
**Expected**: `"10"` (or more if run multiple times)

---

### 7. View Output Data Sample
```bash
docker exec trino trino --execute "SELECT id, original_full_name, name_parts_count, name_length FROM iceberg.data_pipeline.ip_sum_output_sdk_test LIMIT 3"
```
**Expected**: Transformed data with name analysis (parts count, length, etc.)

---

### 8. Check Table Schema
```bash
docker exec trino trino --execute "DESCRIBE iceberg.data_pipeline.ip_sum_output_sdk_test"
```
**Expected Columns**:
- `id` (bigint)
- `original_full_name` (varchar)
- `cleaned_name` (varchar)
- `name_parts_count` (integer)
- `name_length` (integer)
- `processed_at` (timestamp)

---

### 9. View Iceberg Snapshots
```bash
docker exec trino trino --execute 'SELECT snapshot_id, operation, committed_at FROM iceberg.data_pipeline."ip_sum_output_sdk_test$snapshots" ORDER BY committed_at DESC'
```
**Expected**: List of snapshots with IDs, operation type (append/overwrite), and timestamps

---

### 10. Check Snapshot Metadata (Custom Properties)
```bash
docker exec trino trino --execute 'SELECT snapshot_id, summary FROM iceberg.data_pipeline."ip_sum_output_sdk_test$snapshots" ORDER BY committed_at DESC LIMIT 1'
```
**Expected**: Summary map containing:
- `transform=ip_sum`
- `run_id=manual-...`
- `output_records=10`
- `added-records=10`

---

### 11. Verify Lineage Schema
```bash
docker exec trino trino --execute "SHOW TABLES IN iceberg.audit"
```
**Expected**: `"etl_lineage"`

---

### 12. Check Lineage Records
```bash
docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.audit.etl_lineage"
```
**Expected**: 1 or more records

---

### 13. View Lineage Details
```bash
docker exec trino trino --execute "SELECT transform, target_table, target_snapshot_id, inputs_json FROM iceberg.audit.etl_lineage ORDER BY recorded_at DESC LIMIT 1"
```
**Expected**: Lineage record showing:
- `transform`: `"ip_sum"`
- `target_table`: `"iceberg.data_pipeline.ip_sum_output_sdk_test"`
- `target_snapshot_id`: (numeric snapshot ID)
- `inputs_json`: JSON array with input table, ref, and snapshot ID

---

### 14. Trace Full Data Lineage
```bash
docker exec trino trino --execute "SELECT 
    recorded_at,
    transform,
    target_table,
    target_snapshot_id,
    run_id,
    code_branch,
    inputs_json
FROM iceberg.audit.etl_lineage
WHERE target_table = 'iceberg.data_pipeline.ip_sum_output_sdk_test'
ORDER BY recorded_at DESC"
```
**Expected**: Complete lineage history with timestamps, snapshots, and input dependencies

---

### 15. Run Full Pipeline Test
```bash
./scripts/test-sdk-in-docker.sh
```
**Expected**: Script completes with `✅ SDK Test Completed Successfully!`

---

## Key Validation Points

### ✅ SDK is Working If:
1. **SDK imports** without errors
2. **Input data** exists (10 records in full_name_input)
3. **Output table** is created and populated (10+ records)
4. **Table schema** matches expected structure
5. **Snapshots** are recorded in Iceberg metadata
6. **Custom properties** appear in snapshot summary (transform, run_id, output_records)
7. **Lineage table** exists in audit schema
8. **Lineage records** capture input/output relationships with snapshot IDs
9. **Full pipeline test** completes successfully

### ⚠️ Troubleshooting

**SDK import fails**:
```bash
# Reinstall SDK
docker cp ./dmap-data-sdk spark-iceberg:/opt/dmap-data-sdk
docker exec spark-iceberg pip3 install --force-reinstall /opt/dmap-data-sdk
```

**No input data**:
```bash
# Initialize demo data
make init-data
```

**No lineage records**:
```bash
# Ensure audit schema exists
docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.audit"
# Rerun pipeline
./scripts/test-sdk-in-docker.sh
```

**Table doesn't exist**:
```bash
# Run pipeline to create it
./scripts/test-sdk-in-docker.sh
```

---

## Quick Commands Reference

### View all tables in data_pipeline schema
```bash
docker exec trino trino --execute "SHOW TABLES IN iceberg.data_pipeline"
```

### Count records in all SDK tables
```bash
docker exec trino trino --execute "
SELECT 'input' as table_name, COUNT(*) as records FROM iceberg.data_pipeline.full_name_input
UNION ALL
SELECT 'output', COUNT(*) FROM iceberg.data_pipeline.ip_sum_output_sdk_test
UNION ALL
SELECT 'lineage', COUNT(*) FROM iceberg.audit.etl_lineage
"
```

### View latest pipeline run
```bash
docker exec trino trino --execute "
SELECT 
    recorded_at,
    run_id,
    transform,
    target_snapshot_id
FROM iceberg.audit.etl_lineage
ORDER BY recorded_at DESC
LIMIT 1
"
```

### Clean up and retest (WARNING: deletes data)
```bash
# Drop output table
docker exec trino trino --execute "DROP TABLE IF EXISTS iceberg.data_pipeline.ip_sum_output_sdk_test"

# Drop lineage
docker exec trino trino --execute "DROP TABLE IF EXISTS iceberg.audit.etl_lineage"

# Rerun test
./scripts/test-sdk-in-docker.sh
```

---

## Success Criteria Summary

| Check | Command | Expected Output |
|-------|---------|-----------------|
| Docker Stack | `docker ps` | 5+ containers healthy |
| SDK Installed | `docker exec spark-iceberg pip3 list \| grep dmap` | dmap-data-sdk 1.0.0 |
| SDK Imports | `docker exec spark-iceberg python3 -c "from dmap_data_sdk import DataPipeline; print('OK')"` | OK |
| Input Data | `docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.data_pipeline.full_name_input"` | "10" |
| Output Data | `docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.data_pipeline.ip_sum_output_sdk_test"` | "10" or more |
| Snapshots | `docker exec trino trino --execute 'SELECT COUNT(*) FROM iceberg.data_pipeline."ip_sum_output_sdk_test\$snapshots"'` | 1+ |
| Lineage | `docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.audit.etl_lineage"` | 1+ |
| Full Test | `./scripts/test-sdk-in-docker.sh` | ✅ SDK Test Completed Successfully! |

---

## Next Steps After Validation

Once validated, you can:

1. **Use SDK in other pipelines**: Copy the pattern from `examples/ip_sum_example.py`
2. **Migrate existing pipelines**: Replace manual Spark code with SDK calls
3. **Query lineage**: Use Trino/Spark SQL to analyze data dependencies
4. **Publish SDK**: Move to separate Git repository and publish to PyPI
5. **Add to CI/CD**: Run validation script in automated tests

---

**Last Updated**: November 18, 2025  
**SDK Version**: 1.0.0  
**Validation Script**: `scripts/validate-sdk.sh`
