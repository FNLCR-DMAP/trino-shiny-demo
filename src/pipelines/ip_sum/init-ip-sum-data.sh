#!/bin/bash

echo "🚀 Initializing IP Sum Pipeline Data with Iceberg"
echo "This sets up tables and data for ip_sum_iceberg_refactor.py"
echo ""

# Configuration
SCHEMA_NAME=${1:-"data_pipeline"}
PYTHON_SCRIPT="init_ip_sum_data.py"

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

# Step 2: Copy Python script to Spark container (if needed)
echo ""
echo "2. Preparing Spark environment..."
echo "   → Copying initialization script to Spark container..."
docker cp src/pipelines/ip_sum/${PYTHON_SCRIPT} spark-iceberg:/opt/spark/work-dir/${PYTHON_SCRIPT}
echo "   ✓ Script copied successfully"

# Step 3: Run initialization script in Spark container
echo ""
echo "3. Running Iceberg table initialization..."
echo "   → Executing initialization script in Spark container..."

# Run the Python script in the Spark container with proper Iceberg configuration
INIT_RESULT=$(docker exec spark-iceberg /opt/spark/bin/spark-submit \
    --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg.type=hive \
    --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
    --conf spark.sql.catalog.iceberg.warehouse=file:///data/warehouse \
    --conf spark.sql.defaultCatalog=iceberg \
    /opt/spark/work-dir/${PYTHON_SCRIPT} ${SCHEMA_NAME} 2>&1)

if [[ $? -eq 0 ]]; then
    echo "   ✓ Initialization completed successfully"
    echo "$INIT_RESULT"
else
    echo "   ❌ Initialization failed"
    echo "$INIT_RESULT"
    exit 1
fi

# Step 4: Verify tables were created using Trino (cross-engine verification)
echo ""
echo "4. Verifying tables with Trino (cross-engine verification)..."

# Check input table
echo "   → Checking input table..."
INPUT_COUNT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM iceberg.${SCHEMA_NAME}.full_name_input" 2>/dev/null | tail -1 | tr -d '"')

if [[ ! -z "$INPUT_COUNT" ]]; then
    echo "   ✓ Input table verified: ${INPUT_COUNT} records in iceberg.${SCHEMA_NAME}.full_name_input"
else
    echo "   ⚠️  Could not verify input table"
fi

# Check output table structure
echo "   → Checking output table structure..."
OUTPUT_STRUCTURE=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "DESCRIBE iceberg.${SCHEMA_NAME}.ip_sum_output" 2>/dev/null)

if [[ ! -z "$OUTPUT_STRUCTURE" ]]; then
    echo "   ✓ Output table verified: iceberg.${SCHEMA_NAME}.ip_sum_output"
else
    echo "   ⚠️  Could not verify output table"
fi

# Step 5: Show sample data
echo ""
echo "5. Sample data preview:"
echo "   → Input data sample:"
docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT * FROM iceberg.${SCHEMA_NAME}.full_name_input LIMIT 3" 2>/dev/null

echo ""
echo "✅ IP Sum Pipeline Data Initialization Complete!"
echo ""
echo "🔄 Ready to run transformation:"
echo "   → Run: make run-ip-sum-pipeline"
echo "   → Or manually:"
echo "     docker exec spark-iceberg /opt/spark/bin/spark-submit \\"
echo "       --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \\"
echo "       --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \\"
echo "       --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \\"
echo "       --conf spark.sql.catalog.iceberg.type=hive \\"
echo "       --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \\"
echo "       --conf spark.sql.catalog.iceberg.warehouse=file:///data/warehouse \\"
echo "       --conf spark.sql.defaultCatalog=iceberg \\"
echo "       /opt/spark/work-dir/ip_sum_iceberg_refactor.py \\"
echo "       iceberg.${SCHEMA_NAME}.full_name_input iceberg.${SCHEMA_NAME}.ip_sum_output"
echo ""
echo "🔍 Verify with Trino:"
echo "   • Input:  SELECT * FROM iceberg.${SCHEMA_NAME}.full_name_input"
echo "   • Output: SELECT * FROM iceberg.${SCHEMA_NAME}.ip_sum_output"
echo ""
echo "🌐 Access Points:"
echo "   • Trino Web UI: http://localhost:8081"
echo "   • Shiny Frontend: http://localhost:8000"