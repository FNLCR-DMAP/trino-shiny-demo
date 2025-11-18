#!/usr/bin/env python3
"""
Example: Clean IP Sum Pipeline using DMAP Data SDK

This demonstrates how to use the dmap_data_sdk to build a simple data pipeline
with automatic lineage tracking, context detection, and platform abstraction.

Before running:
1. Install the SDK: pip install dmap-data-sdk
2. Create a pipeline_config.yaml in your project root (see example below)
3. Ensure your Spark cluster/environment is configured

Example pipeline_config.yaml:
-----------------------------
platform:
  type: "iceberg"
  spark_config:
    spark.sql.extensions: "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    spark.sql.catalog.iceberg: "org.apache.iceberg.spark.SparkCatalog"
    spark.sql.catalog.iceberg.type: "hive"
    spark.sql.catalog.iceberg.uri: "thrift://hive-metastore:9083"
    spark.sql.catalog.iceberg.warehouse: "s3://my-bucket/warehouse"

lineage:
  enabled: true
  table: "audit.etl_lineage"
"""

import sys
from pyspark.sql.functions import (
    regexp_replace, 
    size, 
    split, 
    length, 
    when, 
    col,
    current_timestamp
)

# Clean import - no sys.path manipulation needed!
from dmap_data_sdk import DataPipeline, Ref


def transform_full_names(df):
    """
    Business logic: Process full names by cleaning and analyzing them.
    
    This is your core transformation logic - focus on this!
    The SDK handles all the infrastructure concerns.
    """
    result = df.select(
        col("id"),
        col("full_name").alias("original_full_name"),
        
        # Clean the name - remove special characters
        regexp_replace(col("full_name"), r"[^a-zA-Z\s]", "").alias("cleaned_name"),
        
        # Count name parts
        size(split(col("full_name"), "\\s+")).alias("name_parts_count"),
        
        # Calculate name length
        length(col("full_name")).alias("name_length"),
        
        # Add processing timestamp
        current_timestamp().alias("processed_at")
    )
    
    return result


def main(input_table: str, output_table: str, branch: str = None):
    """
    Main pipeline execution.
    
    Parameters
    ----------
    input_table : str
        Source table (e.g., "iceberg.raw.customers")
    output_table : str
        Destination table (e.g., "iceberg.processed.customer_names")
    branch : str, optional
        Branch to read from and write to (for testing)
    """
    
    # Step 1: Initialize pipeline from configuration
    # This automatically:
    # - Detects platform type from config (Iceberg/Delta/Snowflake)
    # - Creates Spark session with proper configuration
    # - Detects execution context (Airflow vs standalone)
    # - Sets up lineage tracking
    pipeline = DataPipeline.from_config(transform_name="ip_sum")
    
    # Optional: Override branch for testing
    if branch:
        print(f"🔀 Using branch: {branch}")
        pipeline.set_input_ref(Ref(branch=branch))
        pipeline.set_target_ref(Ref(branch=branch))
    
    # Step 2: Read input data
    # Lineage is automatically tracked
    print(f"📖 Reading from: {input_table}")
    df_input = pipeline.read_table(input_table)
    
    print(f"   Found {df_input.count():,} records")
    
    # Step 3: Apply business logic
    print("⚙️  Transforming data...")
    df_output = transform_full_names(df_input)
    
    # Step 4: Write output
    # Lineage is automatically recorded with snapshot metadata
    print(f"💾 Writing to: {output_table}")
    result = pipeline.write_table(df_output, output_table, mode="overwrite")
    
    # Step 5: Done!
    print("✅ Pipeline completed successfully!")
    print(f"   Snapshot ID: {result.committed_snapshot_id}")
    print(f"   Reference: {result.ref_applied.pretty()}")
    
    return result


def example_time_travel():
    """
    Example: Read data from a specific point in time.
    """
    import time
    
    pipeline = DataPipeline.from_config("time_travel_example")
    
    # Read data from 24 hours ago
    yesterday_ms = int(time.time() * 1000) - 86400000
    df = pipeline.read_table(
        "iceberg.raw.customers",
        ref=Ref(as_of_ts_millis=yesterday_ms)
    )
    
    print(f"Rows from yesterday: {df.count()}")


def example_branch_workflow():
    """
    Example: Develop and test on a feature branch.
    """
    pipeline = DataPipeline.from_config("feature_development")
    
    # Work on feature branch
    feature_branch = "feature/new-logic"
    pipeline.set_input_ref(Ref(branch="main"))  # Read from main
    pipeline.set_target_ref(Ref(branch=feature_branch))  # Write to feature
    
    df = pipeline.read_table("iceberg.raw.customers")
    transformed = transform_full_names(df)
    pipeline.write_table(transformed, "iceberg.processed.customer_names")
    
    print(f"✅ Feature branch '{feature_branch}' updated")


def example_airflow_integration():
    """
    Example: How this works in Airflow.
    
    When running in Airflow, the SDK automatically:
    - Detects DAG ID, Task ID, Run ID
    - Picks up branch/version from Airflow params
    - Records lineage with full execution context
    """
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from datetime import datetime
    
    def run_pipeline(**context):
        # SDK auto-detects Airflow context
        pipeline = DataPipeline.from_config("customer_processing")
        
        df = pipeline.read_table("iceberg.raw.customers")
        result = transform_full_names(df)
        pipeline.write_table(result, "iceberg.processed.customer_names")
    
    with DAG(
        "customer_pipeline",
        start_date=datetime(2025, 1, 1),
        schedule_interval="@daily",
        params={
            "target_branch": "main",  # SDK picks this up automatically
            "input_branch": "main"
        }
    ) as dag:
        
        task = PythonOperator(
            task_id="process_customers",
            python_callable=run_pipeline,
        )


if __name__ == "__main__":
    """
    Usage:
        # Basic usage (uses 'main' branch)
        python ip_sum_example.py iceberg.raw.customers iceberg.processed.customer_names
        
        # With feature branch
        python ip_sum_example.py iceberg.raw.customers iceberg.processed.customer_names feature-x
        
        # With spark-submit
        spark-submit ip_sum_example.py iceberg.raw.customers iceberg.processed.customer_names
    """
    
    if len(sys.argv) < 3:
        print("Usage:")
        print("  python ip_sum_example.py <input_table> <output_table> [branch]")
        print()
        print("Examples:")
        print("  python ip_sum_example.py iceberg.raw.customers iceberg.processed.customer_names")
        print("  python ip_sum_example.py raw.customers processed.names feature-branch")
        sys.exit(1)
    
    input_tbl = sys.argv[1]
    output_tbl = sys.argv[2]
    branch_name = sys.argv[3] if len(sys.argv) > 3 else None
    
    main(input_tbl, output_tbl, branch_name)
