#!/usr/bin/env python3
"""
Iceberg-enabled PySpark Transformation Demo
This demonstrates how to refactor traditional Spark transformations to use Apache Iceberg
following the patterns from init-demo-data.sh
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_iceberg_spark_session(app_name="IcebergTransformation"):
    """
    Create Spark session configured for Iceberg, similar to the setup in docker-compose.yml
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.iceberg.warehouse", "file:///data/warehouse") \
        .config("spark.sql.defaultCatalog", "iceberg") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def setup_iceberg_schema_and_tables(spark, schema_name="data_pipeline"):
    """
    Setup Iceberg schema and tables similar to init-demo-data.sh approach
    """
    print(f"📊 Setting up Iceberg schema: {schema_name}")
    
    # Step 1: Create schema (like init-demo-data.sh Step 1)
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS iceberg.{schema_name}")
        print(f"   ✓ Schema iceberg.{schema_name} ready")
    except Exception as e:
        print(f"   ⚠️  Schema creation warning: {e}")
    
    # Step 2: Create input table for Full_Name data (replaces file reading)
    full_name_table = f"iceberg.{schema_name}.full_name_input"
    try:
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_name_table} (
            id BIGINT,
            full_name STRING,
            first_name STRING,
            last_name STRING,
            processed_date TIMESTAMP,
            source_system STRING
        ) USING iceberg
        PARTITIONED BY (days(processed_date))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'gzip'
        )
        """)
        print(f"   ✓ Input table {full_name_table} ready")
    except Exception as e:
        print(f"   ⚠️  Input table setup: {e}")
    
    # Step 3: Create output table for ip_sum results (replaces file writing)
    ip_sum_table = f"iceberg.{schema_name}.ip_sum_output"
    try:
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ip_sum_table} (
            id BIGINT,
            original_full_name STRING,
            processed_full_name STRING,
            name_parts_count INTEGER,
            processing_timestamp TIMESTAMP,
            pipeline_version STRING,
            data_quality_score DOUBLE
        ) USING iceberg
        PARTITIONED BY (days(processing_timestamp))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'gzip'
        )
        """)
        print(f"   ✓ Output table {ip_sum_table} ready")
    except Exception as e:
        print(f"   ⚠️  Output table setup: {e}")
    
    return full_name_table, ip_sum_table

def load_legacy_data_to_iceberg(spark, legacy_path, iceberg_table):
    """
    One-time migration: Load existing parquet/csv/json data into Iceberg table
    This replaces the file reading logic from your original code
    """
    print(f"🔄 Migrating legacy data from {legacy_path} to {iceberg_table}")
    
    try:
        # Determine file format and read (similar to your original logic)
        if os.path.exists(legacy_path):
            files = os.listdir(legacy_path)
            if any(file.lower().endswith('.parquet') for file in files):
                df = spark.read.parquet(legacy_path)
                print(f"   → Read {df.count()} rows from Parquet files")
            elif any(file.lower().endswith('.csv') for file in files):
                df = spark.read.csv(legacy_path, header=True, inferSchema=True)
                print(f"   → Read {df.count()} rows from CSV files")
            elif any(file.lower().endswith('.json') for file in files):
                df = spark.read.json(legacy_path)
                print(f"   → Read {df.count()} rows from JSON files")
            else:
                print(f"   ⚠️  No recognized data files in {legacy_path}")
                return
            
            # Add Iceberg metadata columns
            df_with_metadata = df.withColumn("processed_date", current_timestamp()) \
                                .withColumn("source_system", lit("legacy_migration"))
            
            # Write to Iceberg table (replaces your parquet write)
            df_with_metadata.writeTo(iceberg_table).append()
            print(f"   ✓ Successfully migrated data to Iceberg table")
            
        else:
            print(f"   ⚠️  Legacy path {legacy_path} not found")
            
    except Exception as e:
        print(f"   ❌ Migration failed: {e}")

def ip_sum_transformation_iceberg(spark, input_table, output_table, pipeline_version="v1.0"):
    """
    Refactored transformation logic using Iceberg tables
    This replaces your original ip_sum function and file I/O
    """
    print(f"🔄 Running ip_sum transformation on Iceberg tables")
    
    try:
        # Read from Iceberg input table (replaces file reading)
        print(f"   → Reading from input table: {input_table}")
        df_input = spark.table(input_table)
        input_count = df_input.count()
        print(f"   → Processing {input_count} records")
        
        # Your transformation logic here (example transformation)
        df_transformed = df_input.select(
            col("id"),
            col("full_name").alias("original_full_name"),
            # Example: Clean and process full name
            regexp_replace(col("full_name"), r"[^a-zA-Z\s]", "").alias("processed_full_name"),
            # Count name parts
            size(split(col("full_name"), "\\s+")).alias("name_parts_count"),
            current_timestamp().alias("processing_timestamp"),
            lit(pipeline_version).alias("pipeline_version"),
            # Example: Data quality score based on name completeness
            when(length(col("full_name")) > 0, 1.0).otherwise(0.0).alias("data_quality_score")
        )
        
        # Write to Iceberg output table (replaces parquet file writing)
        print(f"   → Writing results to output table: {output_table}")
        df_transformed.writeTo(output_table).append()
        
        transformed_count = df_transformed.count()
        print(f"   ✓ Successfully processed {transformed_count} records")
        
        # Enable time travel and versioning (Iceberg advantage!)
        print("   → Iceberg benefits enabled:")
        print("     • Time travel queries available")
        print("     • Schema evolution supported")
        print("     • ACID transactions guaranteed")
        print("     • Snapshot isolation for concurrent reads")
        
        return df_transformed
        
    except Exception as e:
        print(f"   ❌ Transformation failed: {e}")
        raise

def demonstrate_iceberg_features(spark, output_table):
    """
    Demonstrate Iceberg's advanced features (like init-demo-data.sh Step 8)
    """
    print("🚀 Demonstrating Iceberg Advanced Features:")
    
    try:
        # Time Travel - Show snapshots (like init-demo-data.sh)
        print("   → Available snapshots for time travel:")
        snapshots_df = spark.table(f"{output_table}.snapshots")
        snapshots_df.select("committed_at", "operation", "summary").orderBy(desc("committed_at")).limit(5).show(truncate=False)
        
        # Show current data count
        current_count = spark.table(output_table).count()
        print(f"   → Current record count: {current_count}")
        
        # Schema Evolution Example
        print("   → Demonstrating schema evolution:")
        spark.sql(f"ALTER TABLE {output_table} ADD COLUMN data_lineage STRING")
        print("     ✓ Added data_lineage column without breaking existing queries")
        
        # Metadata tables (like init-demo-data.sh Step 8f)
        print("   → Exploring metadata tables:")
        print("     • Files:")
        spark.table(f"{output_table}.files").select("file_format", "record_count", "file_size_in_bytes").show(5)
        
        print("     • History:")
        spark.table(f"{output_table}.history").select("made_current_at", "snapshot_id").orderBy(desc("made_current_at")).show(3)
        
    except Exception as e:
        print(f"   ⚠️  Feature demonstration warning: {e}")

def main(args):
    """
    Main function - refactored to use Iceberg throughout
    """
    if len(args) < 2:
        print("Usage: python iceberg-transformation-demo.py <legacy_input_path> <schema_name>")
        sys.exit(1)
    
    legacy_input_path = args[0]
    schema_name = args[1] if len(args) > 1 else "data_pipeline"
    
    print("🎯 Starting Iceberg-Enabled Data Transformation")
    print(f"   Legacy input: {legacy_input_path}")
    print(f"   Iceberg schema: {schema_name}")
    
    # Create Iceberg-enabled Spark session
    spark = create_iceberg_spark_session("IcebergTransformationDemo")
    
    try:
        # Setup Iceberg infrastructure
        input_table, output_table = setup_iceberg_schema_and_tables(spark, schema_name)
        
        # One-time: Migrate legacy data to Iceberg (if needed)
        if os.path.exists(legacy_input_path):
            load_legacy_data_to_iceberg(spark, legacy_input_path, input_table)
        
        # Run transformation using Iceberg tables
        result_df = ip_sum_transformation_iceberg(spark, input_table, output_table)
        
        # Demonstrate Iceberg's advanced features
        demonstrate_iceberg_features(spark, output_table)
        
        print("✅ Iceberg transformation complete!")
        print(f"🔍 Query your results: SELECT * FROM {output_table}")
        print(f"⏰ Time travel example: SELECT * FROM {output_table} FOR TIMESTAMP AS OF '2025-10-07 12:00:00'")
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == '__main__':
    main(sys.argv[1:])

"""
🎯 Iceberg Transformation Benefits vs Traditional Approach:

BEFORE (Traditional Spark):
- Read files (parquet/csv/json) → Transform → Write parquet files
- No time travel or versioning
- Schema changes break downstream consumers
- No ACID guarantees
- Manual partitioning and optimization

AFTER (Iceberg-enabled):
- Read Iceberg tables → Transform → Write Iceberg tables  
- Full time travel and snapshot capabilities
- Schema evolution without breaking changes
- ACID transactions guaranteed
- Automatic optimization and maintenance
- Cross-engine compatibility (Spark, Trino, etc.)

MIGRATION PATTERN:
1. Setup Iceberg catalog and tables (setup_iceberg_schema_and_tables)
2. One-time migrate existing files to Iceberg (load_legacy_data_to_iceberg)
3. Update transformation logic to use table reads/writes
4. Leverage Iceberg features (time travel, schema evolution, etc.)

OPERATIONAL BENEFITS:
- Data versioning and rollback capabilities
- Better data governance and lineage
- Improved performance with automatic optimization
- Cross-platform data sharing (Spark + Trino)
"""