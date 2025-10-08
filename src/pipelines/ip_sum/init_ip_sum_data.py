#!/usr/bin/env python3
"""
Initialize Iceberg tables for ip_sum_iceberg_refactor.py
This creates the input and output tables with the correct schema and loads sample data
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import *

def create_iceberg_spark_session():
    """
    Create Spark session configured for Iceberg - same as ip_sum_iceberg_refactor.py
    """
    return SparkSession.builder \
        .appName("init_ip_sum_data") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.iceberg.warehouse", "file:///data/warehouse") \
        .config("spark.sql.defaultCatalog", "iceberg") \
        .getOrCreate()

def setup_input_table(spark, schema_name="data_pipeline"):
    """
    Create input table matching the expected schema from ip_sum function
    ip_sum expects: df.columns[0] = id, df.columns[1] = full_name
    """
    input_table = f"iceberg.{schema_name}.full_name_input"
    
    print(f"📊 Creating input table: {input_table}")
    
    # Create schema - matches what ip_sum function expects
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS iceberg.{schema_name}")
    
    # Create input table with schema that ip_sum function can process
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {input_table} (
        id BIGINT,
        full_name STRING
    ) USING iceberg
    TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'gzip'
    )
    """)
    
    print(f"   ✓ Input table created: {input_table}")
    return input_table

def setup_output_table(spark, schema_name="data_pipeline"):
    """
    Create output table matching what ip_sum function produces
    Based on the ip_sum function output: id, original_full_name, processed_full_name, name_parts_count, processing_timestamp
    """
    output_table = f"iceberg.{schema_name}.ip_sum_output"
    
    print(f"📊 Creating output table: {output_table}")
    
    # Create output table matching ip_sum function output schema
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        id BIGINT,
        original_full_name STRING,
        processed_full_name STRING,
        name_parts_count INTEGER,
        processing_timestamp TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (days(processing_timestamp))
    TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'gzip'
    )
    """)
    
    print(f"   ✓ Output table created: {output_table}")
    return output_table

def load_sample_data(spark, input_table):
    """
    Load sample data into input table for testing ip_sum transformation
    """
    print(f"📋 Loading sample data into: {input_table}")
    
    # Check if data already exists
    existing_count = spark.table(input_table).count()
    
    if existing_count == 0:
        # Create sample data matching the expected schema (id, full_name)
        sample_data = [
            (1, "John Michael Doe"),
            (2, "Jane Elizabeth Smith-Wilson"),
            (3, "Carlos Rodriguez Martinez"),
            (4, "Emma Wilson"),
            (5, "Yuki Tanaka Sato"),
            (6, "Dr. Sarah Connor-Johnson PhD"),
            (7, "Muhammad Ali Hassan"),
            (8, "Maria Garcia Lopez"),
            (9, "David Kim"),
            (10, "Anna-Marie O'Sullivan")
        ]
        
        # Create DataFrame with correct schema
        schema = StructType([
            StructField("id", LongType(), True),
            StructField("full_name", StringType(), True)
        ])
        
        df = spark.createDataFrame(sample_data, schema)
        
        # Write to Iceberg table
        df.writeTo(input_table).append()
        
        print(f"   ✓ Loaded {len(sample_data)} sample records")
        print("   → Sample data includes various name formats for testing:")
        print("     • Simple names (David Kim)")
        print("     • Hyphenated names (Jane Elizabeth Smith-Wilson)")
        print("     • Multiple middle names (John Michael Doe)")
        print("     • Titles and suffixes (Dr. Sarah Connor-Johnson PhD)")
        print("     • Apostrophes (Anna-Marie O'Sullivan)")
        
    else:
        print(f"   ✓ Data already exists ({existing_count} records)")
    
    # Show current data for verification
    print(f"   → Current data in {input_table}:")
    spark.table(input_table).show(5, truncate=False)

def main(args):
    """
    Initialize Iceberg tables and data for ip_sum transformation
    """
    schema_name = args[0] if len(args) > 0 else "data_pipeline"
    
    print("🚀 Initializing Iceberg Tables for ip_sum transformation")
    print(f"   Schema: iceberg.{schema_name}")
    print("")
    
    # Create Spark session
    spark = create_iceberg_spark_session()
    
    try:
        # Setup tables
        input_table = setup_input_table(spark, schema_name)
        output_table = setup_output_table(spark, schema_name)
        
        # Load sample data
        load_sample_data(spark, input_table)
        
        print("")
        print("✅ Initialization Complete!")
        print("")
        print("🔄 Ready to run ip_sum transformation:")
        print(f"   python ip_sum_iceberg_refactor.py {input_table} {output_table}")
        print("")
        print("🔍 Verify setup:")
        print(f"   Input table:  SELECT * FROM {input_table}")
        print(f"   Output table: SELECT * FROM {output_table}")
        print("")
        print("📊 Schema Summary:")
        print(f"   Input:  id (BIGINT), full_name (STRING)")
        print(f"   Output: id, original_full_name, processed_full_name, name_parts_count, processing_timestamp")
        
    except Exception as e:
        print(f"❌ Initialization failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == '__main__':
    main(sys.argv[1:])

"""
🎯 This script creates the exact tables needed for ip_sum_iceberg_refactor.py:

INPUT TABLE SCHEMA (matches ip_sum function expectations):
- Column 0 (df.columns[0]): id BIGINT
- Column 1 (df.columns[1]): full_name STRING

OUTPUT TABLE SCHEMA (matches ip_sum function output):
- id: BIGINT
- original_full_name: STRING  
- processed_full_name: STRING
- name_parts_count: INTEGER
- processing_timestamp: TIMESTAMP

USAGE:
python init_ip_sum_data.py [schema_name]

EXAMPLE:
python init_ip_sum_data.py data_pipeline

This will create:
- iceberg.data_pipeline.full_name_input (with sample data)
- iceberg.data_pipeline.ip_sum_output (empty, ready for transformation)
"""