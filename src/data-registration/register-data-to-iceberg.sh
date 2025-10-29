#!/bin/bash

echo "🚀 Registering Data in Iceberg"
echo "This loads data files (CSV, Parquet, JSON, etc.) into Iceberg/Hive stack"
echo ""

# Configuration
DATA_FILE=${1}
SCHEMA_NAME=${2:-"demo"}
TABLE_NAME=${3}
FILE_FORMAT=${4:-""}
SCHEMA_FILE=${5:-""}

# Validation
if [ -z "$DATA_FILE" ] || [ -z "$TABLE_NAME" ]; then
    echo "❌ Error: Missing required arguments"
    echo ""
    echo "Usage:"
    echo "  ./register-data-to-iceberg.sh <data_file> <schema_name> <table_name> [format] [schema_file]"
    echo ""
    echo "Arguments:"
    echo "  data_file    - Path to data file (CSV, Parquet, JSON, ORC, Avro)"
    echo "  schema_name  - Iceberg schema name (default: 'demo')"
    echo "  table_name   - Iceberg table name"
    echo "  format       - Optional: File format (csv, parquet, json, orc, avro)"
    echo "  schema_file  - Optional: Python file with schema definition"
    echo ""
    echo "Examples:"
    echo "  # Register CSV file"
    echo "  ./register-data-to-iceberg.sh data/patients.csv demo patients"
    echo ""
    echo "  # Register Parquet file"
    echo "  ./register-data-to-iceberg.sh data/samples.parquet clinical samples parquet"
    echo ""
    echo "  # Register with custom schema"
    echo "  ./register-data-to-iceberg.sh data/data.csv demo mytable csv schema.py"
    exit 1
fi

# Check if data file exists
if [ ! -f "$DATA_FILE" ]; then
    echo "❌ Error: Data file not found: $DATA_FILE"
    exit 1
fi

PYTHON_SCRIPT="register_data_to_iceberg.py"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_FILENAME=$(basename "$DATA_FILE")

# Step 1: Ensure infrastructure is running
echo "1. Checking Iceberg infrastructure..."
if ! docker ps | grep -q "spark-iceberg"; then
    echo "   ⚠️  Spark container not running. Starting infrastructure..."
    docker-compose up -d
    echo "   → Waiting for services to be ready..."
    sleep 15
else
    echo "   ✓ Spark infrastructure is running"
fi

# Step 2: Prepare paths and arguments
echo ""
echo "2. Preparing environment..."

# Get absolute paths
DATA_FILE_ABS=$(realpath "$DATA_FILE")
DATA_FILE_DIR=$(dirname "$DATA_FILE_ABS")

# Prepare schema file path if provided
if [ -n "$SCHEMA_FILE" ] && [ -f "$SCHEMA_FILE" ]; then
    SCHEMA_FILE_ABS=$(realpath "$SCHEMA_FILE")
    SCHEMA_FILE_DIR=$(dirname "$SCHEMA_FILE_ABS")
    SCHEMA_FILE_NAME=$(basename "$SCHEMA_FILE_ABS")
    echo "   ✓ Schema file: $SCHEMA_FILE_NAME"
else
    SCHEMA_FILE_ABS=""
    SCHEMA_FILE_DIR=""
    SCHEMA_FILE_NAME=""
fi

echo "   ✓ Data file: $DATA_FILENAME"
echo "   ✓ Data directory: $DATA_FILE_DIR"

# Step 3: Run registration in a new container with volume mounts
echo ""
echo "3. Running Iceberg table registration..."
echo "   → Creating temporary Spark container with data volumes..."
echo "   → Data: ${DATA_FILENAME}, Schema: ${SCHEMA_NAME}, Table: ${TABLE_NAME}"

# Build volume mounts
VOLUME_ARGS="-v $DATA_FILE_DIR:/input_data:ro"
VOLUME_ARGS="$VOLUME_ARGS -v $SCRIPT_DIR:/scripts:ro"
VOLUME_ARGS="$VOLUME_ARGS -v $(pwd)/warehouse:/data/warehouse"
VOLUME_ARGS="$VOLUME_ARGS -v $(pwd)/hive-site.xml:/opt/spark/conf/hive-site.xml:ro"
VOLUME_ARGS="$VOLUME_ARGS -v $(pwd)/jars:/opt/spark/jars-local:ro"

if [ -n "$SCHEMA_FILE_ABS" ]; then
    VOLUME_ARGS="$VOLUME_ARGS -v $SCHEMA_FILE_DIR:/input_schema:ro"
    SCHEMA_ARG="/input_schema/$SCHEMA_FILE_NAME"
else
    SCHEMA_ARG=""
fi

# Build the command arguments using named parameters
CMD_ARGS="--data-file /input_data/${DATA_FILENAME} --schema ${SCHEMA_NAME} --table ${TABLE_NAME}"

if [ -n "$FILE_FORMAT" ]; then
    CMD_ARGS="${CMD_ARGS} --format ${FILE_FORMAT}"
fi

if [ -n "$SCHEMA_ARG" ]; then
    CMD_ARGS="${CMD_ARGS} --schema-file ${SCHEMA_ARG}"
fi

# Copy Iceberg JAR if needed and run (as root for warehouse write permissions)
REG_RESULT=$(docker run --rm \
    --user 0:0 \
    --network $(docker inspect spark-iceberg --format='{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}') \
    $VOLUME_ARGS \
    apache/spark:3.5.0 \
    bash -c "
        if [ -f /opt/spark/jars-local/iceberg-spark-runtime-3.5_2.12-1.4.2.jar ]; then
            cp /opt/spark/jars-local/iceberg-spark-runtime-3.5_2.12-1.4.2.jar /opt/spark/jars/
        fi
        /opt/spark/bin/spark-submit \
            --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
            --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.iceberg.type=hive \
            --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
            --conf spark.sql.catalog.iceberg.warehouse=file:///data/warehouse \
            --conf spark.sql.defaultCatalog=iceberg \
            /scripts/${PYTHON_SCRIPT} ${CMD_ARGS}
    " 2>&1)

if [[ $? -eq 0 ]]; then
    echo "   ✓ Registration completed successfully"
    echo "$REG_RESULT"
else
    echo "   ❌ Registration failed"
    echo "$REG_RESULT"
    exit 1
fi

# Step 4: Verify table in Trino
echo ""
echo "4. Verifying table in Trino..."
VERIFY_QUERY="SELECT COUNT(*) as total_records FROM iceberg.${SCHEMA_NAME}.${TABLE_NAME}"
echo "   → Running: $VERIFY_QUERY"

VERIFY_RESULT=$(docker exec trino-cli trino --server trino:8080 --user admin \
    --execute "$VERIFY_QUERY" 2>&1)

if [[ $? -eq 0 ]]; then
    echo "   ✓ Table verified in Trino:"
    echo "$VERIFY_RESULT"
else
    echo "   ⚠️  Could not verify in Trino (table may still be valid)"
    echo "$VERIFY_RESULT"
fi

echo ""
echo "✅ Registration Complete!"
echo ""
echo "🔍 Query the table with Trino:"
echo "   docker exec trino-cli trino --server trino:8080 --user admin"
echo "   SELECT * FROM iceberg.${SCHEMA_NAME}.${TABLE_NAME} LIMIT 10;"
echo ""
echo "📊 Or use the Shiny app at: http://localhost:8000"
