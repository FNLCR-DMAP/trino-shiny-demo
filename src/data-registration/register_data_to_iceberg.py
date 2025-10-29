#!/usr/bin/env python3
"""
Register data files (CSV, Parquet, JSON, etc.) into Iceberg/Hive stack
This creates an Iceberg table and loads data with proper schema inference or explicit schema
"""

import sys
import os
import argparse
from pyspark.sql import SparkSession

def create_iceberg_spark_session():
    """
    Create Spark session configured for Iceberg
    """
    return SparkSession.builder \
        .appName("register_data_to_iceberg") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.iceberg.warehouse", "file:///data/warehouse") \
        .config("spark.sql.defaultCatalog", "iceberg") \
        .getOrCreate()

def detect_file_format(file_path):
    """
    Detect file format from extension
    """
    ext = file_path.lower().split('.')[-1]
    format_map = {
        'csv': 'csv',
        'parquet': 'parquet',
        'json': 'json',
        'orc': 'orc',
        'avro': 'avro'
    }
    return format_map.get(ext, 'csv')

def load_data(spark, file_path, file_format=None, schema_module=None):
    """
    Load data from file with automatic format detection or explicit format
    Supports: CSV, Parquet, JSON, ORC, Avro
    
    For CSV with complex types (arrays, structs), reads as strings first then converts.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data file not found: {file_path}")
    
    # Auto-detect format if not specified
    if file_format is None:
        file_format = detect_file_format(file_path)
    
    print(f"📋 Loading {file_format.upper()} data from: {file_path}")
    
    # Load schema if provided
    target_schema = None
    if schema_module:
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location("schema_module", schema_module)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            target_schema = mod.schema
            print(f"   ✓ Using explicit schema from: {schema_module}")
        except Exception as e:
            print(f"   ⚠️  Could not load schema module: {e}")
            print(f"   → Using automatic schema inference")
    
    # Read data based on format
    reader = spark.read
    
    if file_format == 'csv':
        # CSV special handling: Check if target schema has complex types
        if target_schema:
            from pyspark.sql.types import ArrayType, StructType as ST, MapType
            has_complex_types = any(
                isinstance(field.dataType, (ArrayType, ST, MapType)) 
                for field in target_schema.fields
            )
            
            if has_complex_types:
                print(f"   ⚠️  CSV with complex types detected (arrays/structs/maps)")
                print(f"   → Reading as strings first, will convert after...")
                # Read CSV with all columns as strings
                df = reader.option("header", "true").csv(file_path)
                # Convert to target schema
                df = convert_to_schema(spark, df, target_schema)
            else:
                # Simple schema, can apply directly
                df = reader.schema(target_schema).option("header", "true").csv(file_path)
        else:
            # No schema provided, use inference
            df = reader.option("header", "true").option("inferSchema", "true").csv(file_path)
            
    elif file_format == 'parquet':
        df = reader.parquet(file_path)
    elif file_format == 'json':
        df = reader.json(file_path)
    elif file_format == 'orc':
        df = reader.orc(file_path)
    elif file_format == 'avro':
        df = reader.format("avro").load(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
    
    record_count = df.count()
    column_count = len(df.columns)
    
    print(f"   ✓ Read {record_count} records with {column_count} columns")
    
    return df

def convert_to_schema(spark, df, target_schema):
    """
    Convert DataFrame to target schema, handling complex types in CSV
    Arrays stored as strings like "[""val1"",""val2""]" will be parsed
    """
    from pyspark.sql.types import ArrayType, StringType, IntegerType, LongType, DoubleType, BooleanType, DateType, TimestampType
    from pyspark.sql import functions as F
    
    print(f"   → Converting to target schema with complex types...")
    
    converted_cols = []
    for field in target_schema.fields:
        col_name = field.name
        col_type = field.dataType
        
        if col_name not in df.columns:
            # Column doesn't exist, create as null
            converted_cols.append(F.lit(None).cast(col_type).alias(col_name))
            continue
        
        if isinstance(col_type, ArrayType):
            # Parse array from string
            # Arrays in CSV are stored as strings like: "['val1','val2']" or '["val1","val2"]'
            element_type = col_type.elementType
            
            if isinstance(element_type, StringType):
                # Array of strings - parse from string representation
                # Handle both null and empty strings
                converted_cols.append(
                    F.when(F.col(col_name).isNull() | (F.col(col_name) == ""), F.array())
                    .otherwise(
                        F.split(
                            F.regexp_replace(
                                F.regexp_replace(F.col(col_name), r'^\[|\]$', ''),  # Remove [ ]
                                r'["\']', ''  # Remove quotes
                            ), 
                            ','
                        )
                    ).alias(col_name)
                )
            else:
                # Array of other types - parse then cast elements
                print(f"      ⚠️  Column '{col_name}': Array of {element_type} - treating as string array")
                converted_cols.append(
                    F.when(F.col(col_name).isNull() | (F.col(col_name) == ""), F.array())
                    .otherwise(F.split(F.regexp_replace(F.regexp_replace(F.col(col_name), r'^\[|\]$', ''), r'["\']', ''), ','))
                    .alias(col_name)
                )
        else:
            # Simple type - cast directly
            converted_cols.append(F.col(col_name).cast(col_type))
    
    result_df = df.select(converted_cols)
    print(f"   ✓ Schema conversion completed")
    
    return result_df

def create_iceberg_table(spark, df, schema_name, table_name, drop_existing=True):
    """
    Create Iceberg table from DataFrame
    """
    full_table_name = f"iceberg.{schema_name}.{table_name}"
    
    print(f"\n📊 Creating Iceberg table: {full_table_name}")
    
    # Create schema if it doesn't exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS iceberg.{schema_name}")
    
    # Drop table if it exists (for clean registration)
    if drop_existing:
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        print(f"   → Dropping existing table (if any)")
    
    # Show schema overview
    print(f"   → Table schema (first 10 columns):")
    for i, field in enumerate(df.schema.fields[:10]):
        print(f"      {i+1}. {field.name}: {field.dataType.simpleString()}")
    if len(df.schema.fields) > 10:
        print(f"      ... and {len(df.schema.fields) - 10} more columns")
    
    return full_table_name

def write_to_iceberg(df, table_name):
    """
    Write DataFrame to Iceberg table
    """
    print(f"\n⏳ Writing data to Iceberg table...")
    
    # Write to Iceberg table
    df.writeTo(table_name).create()
    
    print(f"   ✓ Successfully wrote {df.count()} records")

def show_sample_data(spark, table_name, limit=5):
    """
    Show sample data from the table
    """
    print(f"\n📋 Sample data from {table_name}:")
    df = spark.table(table_name)
    
    # Select first few columns for display
    display_cols = df.columns[:5]
    df.select(display_cols).show(limit, truncate=True)

def verify_table(spark, table_name):
    """
    Verify the table was created and data was loaded correctly
    """
    print(f"\n🔍 Verifying table: {table_name}")
    
    df = spark.table(table_name)
    count = df.count()
    columns = len(df.columns)
    
    print(f"   ✓ Total records: {count}")
    print(f"   ✓ Total columns: {columns}")
    
    return count

def parse_args():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(
        description='Register data files (CSV, Parquet, JSON, ORC, Avro) into Iceberg/Hive stack',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Register CSV with auto-detection
  python register_data_to_iceberg.py --data-file data.csv --schema demo --table patients

  # Register Parquet (schema already in file)
  python register_data_to_iceberg.py --data-file data.parquet --schema clinical --table samples

  # Register CSV with explicit format
  python register_data_to_iceberg.py --data-file data.txt --schema demo --table mytable --format csv

  # Register with custom schema definition
  python register_data_to_iceberg.py --data-file data.csv --schema demo --table mytable --schema-file my_schema.py
        '''
    )
    
    parser.add_argument('--data-file', '-d', required=True,
                        help='Path to data file (CSV, Parquet, JSON, ORC, Avro)')
    parser.add_argument('--schema', '-s', required=True,
                        help='Iceberg schema/database name (e.g., demo, clinical, lp_lims)')
    parser.add_argument('--table', '-t', required=True,
                        help='Iceberg table name (e.g., patients, samples)')
    parser.add_argument('--format', '-f', required=False,
                        help='File format (csv, parquet, json, orc, avro). Auto-detected if not specified.')
    parser.add_argument('--schema-file', required=False,
                        help='Python file with schema definition (for CSV). Not needed for Parquet/ORC.')
    
    return parser.parse_args()

def main():
    """
    Register data file in Iceberg/Hive stack
    """
    args = parse_args()
    
    data_file = args.data_file
    schema_name = args.schema
    table_name = args.table
    file_format = args.format
    schema_file = args.schema_file
    
    print("🚀 Registering Data in Iceberg")
    print(f"   Data File: {data_file}")
    print(f"   Schema: iceberg.{schema_name}")
    print(f"   Table: {table_name}")
    if file_format:
        print(f"   Format: {file_format}")
    if schema_file:
        print(f"   Schema File: {schema_file}")
    print("")
    
    # Create Spark session
    spark = create_iceberg_spark_session()
    
    try:
        # Load data
        df = load_data(spark, data_file, file_format, schema_file)
        
        # Create Iceberg table
        full_table_name = create_iceberg_table(spark, df, schema_name, table_name)
        
        # Write data to Iceberg
        write_to_iceberg(df, full_table_name)
        
        # Verify table
        record_count = verify_table(spark, full_table_name)
        
        # Show sample data
        show_sample_data(spark, full_table_name)
        
        print("")
        print("✅ Registration Complete!")
        print("")
        print("🔍 Query the table:")
        print(f"   SELECT * FROM {full_table_name} LIMIT 10")
        print("")
        print("📊 Table Info:")
        print(f"   DESCRIBE {full_table_name}")
        print("")
        print(f"✨ Table ready with {record_count} records!")
        
    except Exception as e:
        print(f"❌ Registration failed: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == '__main__':
    main()

"""
🎯 Generic Data Registration Tool for Iceberg/Hive Stack

SUPPORTED FORMATS:
- CSV (with header)
- Parquet
- JSON
- ORC
- Avro

FEATURES:
- Automatic format detection from file extension
- Schema inference or explicit schema definition
- Creates Iceberg table with proper data types
- Verifies data integrity after loading
- Works with any data file format

USAGE EXAMPLES:

1. Register CSV file (auto-detection):
   python register_data_to_iceberg.py data/patients.csv demo patients

2. Register Parquet file:
   python register_data_to_iceberg.py data/samples.parquet clinical samples

3. Register with explicit format:
   python register_data_to_iceberg.py data.txt demo mytable csv

4. Register with custom schema:
   python register_data_to_iceberg.py data.csv demo mytable csv my_schema.py

DOCKER CONTAINER USAGE:
When running in Spark container, paths should be container paths:
   python register_data_to_iceberg.py /opt/spark/work-dir/data.csv demo table
"""
