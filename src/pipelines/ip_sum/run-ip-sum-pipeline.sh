#!/bin/bash

echo "🔄 Running IP Sum Pipeline Transformation"
echo "This runs the ip_sum_iceberg_refactor.py transformation in Spark container"
echo ""

# Configuration
SCHEMA_NAME=${1:-"data_pipeline"}
INPUT_TABLE="iceberg.${SCHEMA_NAME}.full_name_input"
OUTPUT_TABLE="iceberg.${SCHEMA_NAME}.ip_sum_output"
PYTHON_SCRIPT="ip_sum_iceberg_refactor.py"

# Step 1: Ensure infrastructure is running
echo "1. Checking infrastructure..."
if ! docker ps | grep -q "spark-iceberg"; then
    echo "   ⚠️  Spark container not running. Starting infrastructure..."
    docker-compose up -d
    echo "   → Waiting for services to be ready..."
    sleep 15
else
    echo "   ✓ Infrastructure is running"
fi

# Step 2: Verify input data exists
echo ""
echo "2. Verifying input data..."
INPUT_COUNT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM ${INPUT_TABLE}" 2>/dev/null | tail -1 | tr -d '"')

if [[ ! -z "$INPUT_COUNT" && "$INPUT_COUNT" -gt "0" ]]; then
    echo "   ✓ Input data verified: ${INPUT_COUNT} records in ${INPUT_TABLE}"
else
    echo "   ❌ No input data found. Run 'make init-ip-sum-data' first"
    exit 1
fi

# Step 3: Copy transformation script to Spark container
echo ""
echo "3. Preparing transformation script..."
echo "   → Copying script to Spark container..."
docker cp src/pipelines/ip_sum/${PYTHON_SCRIPT} spark-iceberg:/opt/spark/work-dir/${PYTHON_SCRIPT}
echo "   ✓ Script copied successfully"

# Step 4: Run the transformation
echo ""
echo "4. Running IP Sum transformation..."
echo "   → Input:  ${INPUT_TABLE}"
echo "   → Output: ${OUTPUT_TABLE}"
echo "   → Processing..."

TRANSFORM_RESULT=$(docker exec spark-iceberg /opt/spark/bin/spark-submit \
    --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg.type=hive \
    --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
    --conf spark.sql.catalog.iceberg.warehouse=file:///data/warehouse \
    --conf spark.sql.defaultCatalog=iceberg \
    /opt/spark/work-dir/${PYTHON_SCRIPT} ${INPUT_TABLE} ${OUTPUT_TABLE} 2>&1)

if [[ $? -eq 0 ]]; then
    echo "   ✓ Transformation completed successfully"
    echo ""
    echo "📊 Transformation Output:"
    echo "$TRANSFORM_RESULT"
else
    echo "   ❌ Transformation failed"
    echo "$TRANSFORM_RESULT"
    exit 1
fi

# Step 5: Verify results with cross-engine query (Trino)
echo ""
echo "5. Verifying results with Trino (cross-engine verification)..."

# Check output count
OUTPUT_COUNT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM ${OUTPUT_TABLE}" 2>/dev/null | tail -1 | tr -d '"')

if [[ ! -z "$OUTPUT_COUNT" && "$OUTPUT_COUNT" -gt "0" ]]; then
    echo "   ✓ Results verified: ${OUTPUT_COUNT} records in ${OUTPUT_TABLE}"
else
    echo "   ⚠️  Could not verify results"
fi

# Show sample results
echo ""
echo "6. Sample transformation results:"
echo "   → Processed names sample:"
docker exec trino-cli trino --server trino:8080 --user admin --execute "
SELECT 
    id,
    original_full_name,
    processed_full_name,
    name_parts_count
FROM ${OUTPUT_TABLE} 
ORDER BY id 
LIMIT 5" 2>/dev/null

# Step 6: Demonstrate Iceberg features
echo ""
echo "7. Iceberg Features Demonstration:"

# Time travel - show snapshots
echo "   → Available snapshots (time travel capability):"
docker exec trino-cli trino --server trino:8080 --user admin --execute "
SELECT committed_at, operation, summary 
FROM ${OUTPUT_TABLE}\$snapshots 
ORDER BY committed_at DESC 
LIMIT 3" 2>/dev/null

# Metadata tables
echo "   → Data files information:"
docker exec trino-cli trino --server trino:8080 --user admin --execute "
SELECT file_format, record_count, file_size_in_bytes 
FROM ${OUTPUT_TABLE}\$files 
LIMIT 3" 2>/dev/null

echo ""
echo "✅ IP Sum Pipeline Transformation Complete!"
echo ""
echo "🎯 Summary:"
echo "   • Input records processed: ${INPUT_COUNT}"
echo "   • Output records created: ${OUTPUT_COUNT}"
echo "   • Iceberg features: Time travel, ACID transactions, schema evolution"
echo "   • Cross-engine compatibility: Spark (processing) + Trino (querying)"
echo ""
echo "🔍 Query Results:"
echo "   • Current data: SELECT * FROM ${OUTPUT_TABLE}"
echo "   • Time travel:  SELECT * FROM ${OUTPUT_TABLE} FOR TIMESTAMP AS OF TIMESTAMP '2025-10-07 12:00:00'"
echo "   • Snapshots:    SELECT * FROM ${OUTPUT_TABLE}\$snapshots"
echo ""
echo "🌐 Access Points:"
echo "   • Trino Web UI: http://localhost:8081"
echo "   • Shiny Frontend: http://localhost:8000"