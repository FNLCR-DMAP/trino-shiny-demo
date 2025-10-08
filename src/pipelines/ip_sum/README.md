# IP Sum Pipeline

This directory contains the Iceberg-enabled IP Sum data transformation pipeline.

## Files

- **`init_ip_sum_data.py`** - Python script to create Iceberg tables and load sample data
- **`ip_sum_iceberg_refactor.py`** - Main transformation script (refactored for Iceberg)
- **`init-ip-sum-data.sh`** - Shell script to initialize pipeline data in containers
- **`run-ip-sum-pipeline.sh`** - Shell script to run the transformation pipeline

## Usage

From the project root directory:

```bash
# Initialize tables and sample data
make init-ip-sum-data

# Run the transformation pipeline
make run-ip-sum-pipeline
```

## Pipeline Flow

1. **Input Table**: `iceberg.data_pipeline.full_name_input`
   - Schema: `id (BIGINT), full_name (STRING)`
   - Contains sample names with various formats for testing

2. **Transformation**: 
   - Cleans full names (removes special characters)
   - Counts name parts (splits on spaces)
   - Adds processing timestamp

3. **Output Table**: `iceberg.data_pipeline.ip_sum_output`
   - Schema: `id, original_full_name, processed_full_name, name_parts_count, processing_timestamp`
   - Partitioned by `days(processing_timestamp)`

## Iceberg Features

- **ACID Transactions**: Guaranteed consistency
- **Time Travel**: Query historical versions
- **Schema Evolution**: Add columns without breaking downstream
- **Cross-Engine**: Spark processes, Trino queries
- **Automatic Optimization**: Compaction, file pruning

## Querying Results

Use Trino CLI:
```sql
-- View results
SELECT * FROM iceberg.data_pipeline.ip_sum_output ORDER BY id;

-- Time travel
SELECT * FROM iceberg.data_pipeline."ip_sum_output$snapshots";

-- Metadata
SELECT * FROM iceberg.data_pipeline."ip_sum_output$files";
```