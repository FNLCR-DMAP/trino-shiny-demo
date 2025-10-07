# Iceberg Branch Querying with Trino

## Overview

While Trino doesn't support **creating** Iceberg branches (like `CREATE BRANCH`), it **does support querying existing branches** that were created by other Iceberg-compatible engines like Spark, Flink, or PyIceberg.

## Key Capabilities

✅ **Query existing branches**: `FOR VERSION AS OF 'branch-name'`  
✅ **Query existing tags**: `FOR VERSION AS OF 'tag-name'`  
✅ **List available branches/tags**: Using `$refs` metadata table  
❌ **Create branches**: Not supported (use Spark/Flink/PyIceberg)  
❌ **Drop branches**: Not supported (use Spark/Flink/PyIceberg)  

## Syntax Examples

### 1. Query Current Data (Main Branch)
```sql
SELECT * FROM iceberg.demo.sample_data;
```

### 2. Query Specific Branch
```sql
SELECT * FROM iceberg.demo.sample_data FOR VERSION AS OF 'feature-branch';
```

### 3. Query Specific Tag
```sql
SELECT * FROM iceberg.demo.sample_data FOR VERSION AS OF 'v1.0.0';
```

### 4. List Available Branches and Tags
```sql
SELECT name, type, snapshot_id 
FROM iceberg.demo."sample_data$refs"
ORDER BY type, name;
```

### 5. Compare Between Branches
```sql
-- Main branch data
SELECT 'main' as branch, COUNT(*) as row_count 
FROM iceberg.demo.sample_data

UNION ALL

-- Feature branch data  
SELECT 'feature-branch' as branch, COUNT(*) as row_count
FROM iceberg.demo.sample_data FOR VERSION AS OF 'feature-branch';
```

## Creating Branches (External Tools)

To create branches that Trino can query, use these tools:

### Using PySpark with Iceberg
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergBranches") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Create a branch
spark.sql("ALTER TABLE iceberg.demo.sample_data CREATE BRANCH feature_branch")

# Create a tag
spark.sql("ALTER TABLE iceberg.demo.sample_data CREATE TAG v1_0_0")
```

### Using PyIceberg
```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("default")
table = catalog.load_table("demo.sample_data")

# Create branch
table.create_branch("feature_branch")

# Create tag
table.create_tag("v1_0_0", table.current_snapshot())
```

## Demo in Shiny App

The Shiny app includes several query types to demonstrate branch functionality:

1. **🎯 Available Branches & Tags**: Lists all branches/tags using `$refs` table
2. **🌿 Branch Querying & Comparison**: Shows how to query branches and compare data
3. **🕐 Time Travel - Snapshots**: Alternative approach using snapshot IDs

## Workflow Summary

1. **Create branches/tags** using Spark, Flink, or PyIceberg
2. **Query branches/tags** using Trino with `FOR VERSION AS OF`
3. **List available branches** using the `$refs` metadata table
4. **Compare data** between different branches/snapshots

This approach gives you the full power of Iceberg's branching capabilities while leveraging Trino's excellent query performance for analytics.