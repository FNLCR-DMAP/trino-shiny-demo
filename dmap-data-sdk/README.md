# DMAP Data SDK

A unified data engineering SDK for **Apache Iceberg**, **Delta Lake**, and **Snowflake** with built-in lineage tracking, time travel, and branch management support.

## Features

✅ **Multi-Platform Support**: Write once, run on Iceberg, Delta Lake, or Snowflake  
✅ **Automatic Lineage Tracking**: Built-in lineage capture with zero boilerplate  
✅ **Time Travel & Branching**: First-class support for table versioning  
✅ **Airflow Integration**: Auto-detects Airflow context and parameters  
✅ **Configuration-Driven**: Initialize pipelines from YAML config files  
✅ **Spark-Submit Ready**: Works seamlessly with `spark-submit` workflows  
✅ **Minimal Boilerplate**: Focus on business logic, not infrastructure

## Installation

### From Local Directory (Current Setup)

```bash
# Install from local directory
pip install /path/to/dmap-data-sdk

# Install in development mode (for SDK contributors)
cd dmap-data-sdk
pip install -e .
```

### From Git (When Published)

```bash
# Install latest from main branch
pip install git+https://github.com/FNLCR-DMAP/dmap-data-sdk.git

# Install specific version/tag
pip install git+https://github.com/FNLCR-DMAP/dmap-data-sdk.git@v1.0.0
```

### From PyPI (Future)

```bash
pip install dmap-data-sdk
```

### With PySpark

```bash
# Install with Spark dependencies
pip install "dmap-data-sdk[spark]"

# Or install PySpark separately
pip install dmap-data-sdk pyspark>=3.3.0
```

## Quick Start

### 1. Create a Configuration File

Create `pipeline_config.yaml` in your project root:

```yaml
platform:
  type: "iceberg"  # or "delta", "snowflake"
  spark_config:
    spark.sql.extensions: "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    spark.sql.catalog.iceberg: "org.apache.iceberg.spark.SparkCatalog"
    spark.sql.catalog.iceberg.type: "hive"
    spark.sql.catalog.iceberg.uri: "thrift://hive-metastore:9083"
    spark.sql.catalog.iceberg.warehouse: "s3://my-bucket/warehouse"

lineage:
  enabled: true
  table: "audit.etl_lineage"

environment: "production"
```

### 2. Write Your Pipeline

```python
from dmap_data_sdk import DataPipeline, Ref

def my_transform(df):
    """Your business logic - focus on this!"""
    return df.groupBy("customer_id").sum("amount")

def main():
    # Initialize pipeline from config - handles everything automatically
    pipeline = DataPipeline.from_config(transform_name="customer_totals")
    
    # Read with automatic lineage tracking
    orders = pipeline.read_table("iceberg.raw.orders")
    
    # Your transformation logic
    result = my_transform(orders)
    
    # Write with automatic lineage and metadata stamping
    pipeline.write_table(result, "iceberg.analytics.customer_totals")
    
    print("✅ Pipeline completed!")

if __name__ == "__main__":
    main()
```

### 3. Run It

```bash
# Standalone
python my_pipeline.py

# With spark-submit
spark-submit my_pipeline.py

# In Docker
docker run my-spark-image python my_pipeline.py
```

## Usage Examples

### Time Travel

```python
from dmap_data_sdk import DataPipeline, Ref

pipeline = DataPipeline.from_config("my_transform")

# Read from yesterday
yesterday_ms = int(time.time() * 1000) - 86400000
df = pipeline.read_table("orders", Ref(as_of_ts_millis=yesterday_ms))

# Read specific snapshot
df = pipeline.read_table("orders", Ref(snapshot_id=12345))
```

### Branch Operations (Iceberg only)

```python
from dmap_data_sdk import DataPipeline, Ref

pipeline = DataPipeline.from_config("feature_test")

# Read from feature branch
pipeline.set_input_ref(Ref(branch="feature-x"))
df = pipeline.read_table("orders")

# Write to feature branch
pipeline.set_target_ref(Ref(branch="feature-x"))
pipeline.write_table(result, "customer_totals")
```

### Airflow Integration

The SDK automatically detects Airflow context:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from dmap_data_sdk import DataPipeline

def run_pipeline(**context):
    # Automatically picks up Airflow context (run_id, dag_id, task_id)
    pipeline = DataPipeline.from_config("my_transform")
    
    # Airflow params override defaults
    # Pass params like: {"target_branch": "feature-x"}
    df = pipeline.read_table("orders")
    result = my_transform(df)
    pipeline.write_table(result, "customer_totals")

with DAG("my_dag", ...) as dag:
    task = PythonOperator(
        task_id="run_transform",
        python_callable=run_pipeline,
        params={"target_branch": "main"}
    )
```

### Low-Level Platform API

For advanced use cases, use the platform API directly:

```python
from dmap_data_sdk import PlatformFactory, Ref, RunContext, millis
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
platform = PlatformFactory.iceberg_spark(spark)

# Read with explicit reference
df = platform.read_table("orders", Ref(branch="main"))

# Custom context
ctx = RunContext(
    run_id="manual-123",
    code_branch="feature-x",
    default_input_ref=Ref(branch="main"),
    target_ref=Ref(branch="feature-x")
)

# Write with context
result = platform.write_table(df, "customer_totals", ctx, mode="overwrite")
print(f"Committed snapshot: {result.committed_snapshot_id}")
```

## Configuration Reference

```yaml
platform:
  type: "iceberg"  # Required: "iceberg", "delta", or "snowflake"
  
  spark_config:  # Optional: Spark configuration overrides
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    # Add any Spark config here
  
  warehouse_path: "s3://my-bucket/warehouse"  # Optional
  metastore_uri: "thrift://metastore:9083"    # Optional

lineage:
  enabled: true  # Optional: default true
  table: "audit.etl_lineage"  # Optional: default "audit.etl_lineage"

environment: "production"  # Optional: for your own tracking
```

The SDK looks for config files in this order:
1. Explicit path: `DataPipeline.from_config(..., config_path="/path/to/config.yaml")`
2. Current directory: `./pipeline_config.yaml`
3. Config subdirectory: `./config/pipeline_config.yaml`
4. Environment variable: `$DATA_PIPELINE_CONFIG`

## Spark-Submit Integration

### Option 1: Pre-installed SDK

```bash
# Install SDK in Spark environment
pip install git+https://github.com/FNLCR-DMAP/dmap-data-sdk.git

# Submit normally
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  my_pipeline.py
```

### Option 2: Include as Dependency

```bash
# Build wheel
cd dmap-data-sdk
pip install build
python -m build

# Submit with wheel
spark-submit \
  --py-files dist/dmap_data_sdk-1.0.0-py3-none-any.whl \
  my_pipeline.py
```

### Docker + Spark

```dockerfile
FROM spark:3.5.0-python3

# Install SDK (adjust path as needed)
COPY dmap-data-sdk /opt/dmap-data-sdk
RUN pip install /opt/dmap-data-sdk

# Copy pipeline code
COPY pipelines/ /app/pipelines/
COPY pipeline_config.yaml /app/

WORKDIR /app
```

**Example Usage in Docker Container**:
```bash
# Copy SDK to container
docker cp ./dmap-data-sdk spark-container:/opt/dmap-data-sdk

# Install in container
docker exec spark-container pip install /opt/dmap-data-sdk

# Run pipeline
docker exec spark-container spark-submit \
  --master local[*] \
  --jars /opt/spark/jars/iceberg-spark-runtime-*.jar \
  /app/pipelines/my_pipeline.py
```

## Querying Lineage

The SDK automatically records lineage to `audit.etl_lineage` (configurable). Query it with Trino/Spark:

```sql
-- View recent pipeline runs
SELECT 
    recorded_at,
    transform,
    target_table,
    target_snapshot_id,
    run_id,
    dag_id,
    code_branch
FROM audit.etl_lineage
ORDER BY recorded_at DESC
LIMIT 10;

-- Trace data dependencies for a specific snapshot
SELECT 
    transform,
    inputs_json,
    recorded_at
FROM audit.etl_lineage
WHERE target_table = 'iceberg.analytics.customer_totals'
  AND target_snapshot_id = 12345;

-- Branch analytics
SELECT 
    code_branch,
    COUNT(*) as runs,
    MAX(recorded_at) as last_run
FROM audit.etl_lineage
WHERE transform = 'customer_totals'
GROUP BY code_branch;
```

## Platform Support Matrix

| Feature | Iceberg | Delta Lake | Snowflake |
|---------|---------|------------|-----------|
| Time Travel (Snapshot ID) | ✅ | ✅ (Version) | ❌ |
| Time Travel (Timestamp) | ✅ | ✅ | ❌ |
| Named Branches | ✅ | ❌ | ❌ |
| Tags | ✅ | ❌ | ❌ |
| Lineage Tracking | ✅ | ✅ | ⚠️ Partial |
| Metadata Stamping | ✅ | ⚠️ Limited | ❌ |
| Auto Table Creation | ✅ | ✅ | ⚠️ Manual |

**Note**: Iceberg branch operations use `saveAsTable()` which automatically creates tables if they don't exist. For the main branch, no explicit branch option is needed.

## API Reference

### DataPipeline (High-Level API)

**Recommended for most users.**

```python
DataPipeline.from_config(transform_name: str, config_path: Optional[str] = None)
DataPipeline.for_iceberg(transform_name: str, **kwargs)
DataPipeline.for_delta(transform_name: str, **kwargs)

pipeline.read_table(table: str, ref: Optional[Ref] = None) -> DataFrame
pipeline.write_table(df: DataFrame, table: str, mode: str = "append") -> WriteResult
pipeline.set_input_ref(ref: Ref) -> None
pipeline.set_target_ref(ref: Ref) -> None
```

### Ref (Version Reference)

```python
Ref(branch: Optional[str] = None,
    tag: Optional[str] = None,
    snapshot_id: Optional[int] = None,
    as_of_ts_millis: Optional[int] = None)
```

### DataPlatform (Low-Level API)

For advanced use cases requiring fine-grained control.

```python
platform.read_table(table: str, ref: Optional[Ref] = None) -> DataFrame
platform.write_table(df: DataFrame, table: str, ctx: RunContext, mode: str = "append") -> WriteResult
platform.resolve_snapshot_id(table: str, ref: Optional[Ref] = None) -> Optional[int]
```

## Development

### Running Tests

```bash
pip install -e ".[dev]"
pytest tests/
```

### Building

```bash
python -m build
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

## License

Apache License 2.0 - See LICENSE file for details.

## Known Issues & Best Practices

### Iceberg Branch Operations
- The SDK automatically handles the "main" branch - no explicit branch option is set when reading/writing to main
- For feature branches, explicitly set: `pipeline.set_target_ref(Ref(branch="feature-x"))`
- Tables are created automatically using `saveAsTable()` when they don't exist

### Lineage Table Setup
- The lineage table (`audit.etl_lineage`) is automatically created on first use
- Ensure the `audit` schema exists in your catalog before running pipelines
- Use Trino/Spark SQL to create it manually if needed: `CREATE SCHEMA IF NOT EXISTS iceberg.audit`

### Spark Configuration
- Iceberg requires `iceberg-spark-runtime` JAR in classpath
- Delta Lake requires `delta-spark` package
- Recommended Spark 3.5.0+ for best Iceberg compatibility

## Support

- **Issues**: https://github.com/FNLCR-DMAP/dmap-data-sdk/issues (when published)
- **Documentation**: https://github.com/FNLCR-DMAP/dmap-data-sdk (when published)
- **Contact**: dmap@fnlcr.nih.gov
- **Current Location**: Part of trino-shiny-demo repository during development
