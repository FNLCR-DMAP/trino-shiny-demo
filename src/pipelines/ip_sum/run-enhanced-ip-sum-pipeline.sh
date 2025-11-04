#!/bin/bash

echo "🔄 Running Enhanced IP Sum Pipeline with Configuration-Driven DataPipeline"
echo "This runs the refactored ip_sum_iceberg_refactor.py with data_utils in Spark container"
echo ""

# Configuration
SCHEMA_NAME=${1:-"data_pipeline"}
INPUT_TABLE="iceberg.${SCHEMA_NAME}.full_name_input"
OUTPUT_TABLE="iceberg.${SCHEMA_NAME}.ip_sum_output"
PYTHON_SCRIPT="ip_sum_iceberg_refactor.py"
DATA_UTILS_MODULE="data_utils.py"
CONFIG_FILE="pipeline_config.yaml"

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

# Step 3: Install PyYAML in container if needed
echo ""
echo "3. Ensuring dependencies..."
echo "   → Checking PyYAML availability..."
docker exec spark-iceberg python -c "import yaml; print('PyYAML available')" 2>/dev/null || {
    echo "   → Installing PyYAML..."
    docker exec spark-iceberg pip install pyyaml
}
echo "   ✓ Dependencies ready"

# Step 4: Copy required files to Spark container
echo ""
echo "4. Preparing transformation files..."
echo "   → Copying enhanced pipeline script..."
docker cp src/pipelines/ip_sum/${PYTHON_SCRIPT} spark-iceberg:/opt/spark/work-dir/${PYTHON_SCRIPT}
echo "   → Copying data_utils module..."
docker cp src/dmap_data_sdk/${DATA_UTILS_MODULE} spark-iceberg:/opt/spark/work-dir/${DATA_UTILS_MODULE}
echo "   → Copying configuration file..."
docker cp ${CONFIG_FILE} spark-iceberg:/opt/spark/work-dir/${CONFIG_FILE}
echo "   ✓ All files copied successfully"

# Step 5: Fix import paths for container execution
echo ""
echo "5. Preparing container environment..."
docker exec spark-iceberg sed -i 's|sys.path.append.*|# Import directly from current directory|' /opt/spark/work-dir/${PYTHON_SCRIPT}
echo "   ✓ Import paths configured for container"

# Step 6: Run the enhanced transformation
echo ""
echo "6. Running Enhanced IP Sum transformation..."
echo "   → Input:  ${INPUT_TABLE}"
echo "   → Output: ${OUTPUT_TABLE}"
echo "   → Configuration: ${CONFIG_FILE}"
echo "   → Processing with configuration-driven DataPipeline..."

TRANSFORM_RESULT=$(docker exec spark-iceberg /opt/spark/bin/spark-submit \
    --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
    --py-files /opt/spark/work-dir/${DATA_UTILS_MODULE} \
    /opt/spark/work-dir/${PYTHON_SCRIPT} ${INPUT_TABLE} ${OUTPUT_TABLE} 2>&1)

if [[ $? -eq 0 ]]; then
    echo "   ✓ Enhanced transformation completed successfully"
    echo ""
    echo "📊 Transformation Output:"
    echo "$TRANSFORM_RESULT"
else
    echo "   ❌ Transformation failed"
    echo "$TRANSFORM_RESULT"
    exit 1
fi

# Step 7: Verify results with cross-engine query (Trino)
echo ""
echo "7. Verifying results with Trino (cross-engine verification)..."

# Check output count
OUTPUT_COUNT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM ${OUTPUT_TABLE}" 2>/dev/null | tail -1 | tr -d '"')

if [[ ! -z "$OUTPUT_COUNT" && "$OUTPUT_COUNT" -gt "0" ]]; then
    echo "   ✓ Results verified: ${OUTPUT_COUNT} records in ${OUTPUT_TABLE}"
else
    echo "   ⚠️  Could not verify results"
fi

# Show sample results
echo ""
echo "8. Sample transformation results:"
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

# Step 8: Check lineage tracking (if audit table exists)
echo ""
echo "9. Enhanced Features Verification:"
echo "   → Checking lineage tracking..."
LINEAGE_COUNT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM audit.etl_lineage WHERE transform = 'ip_sum'" 2>/dev/null | tail -1 | tr -d '"')

if [[ ! -z "$LINEAGE_COUNT" && "$LINEAGE_COUNT" -gt "0" ]]; then
    echo "   ✅ Lineage tracking verified: ${LINEAGE_COUNT} lineage records"
    echo "   → Latest lineage record:"
    docker exec trino-cli trino --server trino:8080 --user admin --execute "
    SELECT recorded_at, run_id, target_table, target_snapshot_id 
    FROM audit.etl_lineage 
    WHERE transform = 'ip_sum' 
    ORDER BY recorded_at DESC 
    LIMIT 1" 2>/dev/null
else
    echo "   📝 No lineage records found (audit schema may not exist)"
fi

# Step 9: Demonstrate Iceberg features
echo ""
echo "10. Iceberg Features Demonstration:"

# Time travel - show snapshots
echo "    → Available snapshots (time travel capability):"
docker exec trino-cli trino --server trino:8080 --user admin --execute "
SELECT committed_at, operation, summary 
FROM ${OUTPUT_TABLE}\$snapshots 
ORDER BY committed_at DESC 
LIMIT 3" 2>/dev/null

# Metadata tables
echo "    → Data files information:"
docker exec trino-cli trino --server trino:8080 --user admin --execute "
SELECT file_format, record_count, file_size_in_bytes 
FROM ${OUTPUT_TABLE}\$files 
LIMIT 3" 2>/dev/null

echo ""
echo "✅ Enhanced IP Sum Pipeline Transformation Complete!"
echo ""
echo "🎯 Enhanced Features Summary:"
echo "   • Configuration-driven platform selection (${CONFIG_FILE})"
echo "   • Automatic context detection (Airflow vs standalone)"
echo "   • Built-in lineage tracking and recording"
echo "   • Branch/time travel support"
echo "   • Clean data engineering API"
echo ""
echo "📊 Processing Summary:"
echo "   • Input records processed: ${INPUT_COUNT}"
echo "   • Output records created: ${OUTPUT_COUNT}"
echo "   • Lineage records: ${LINEAGE_COUNT}"
echo ""
echo "🔍 Query Examples:"
echo "   • Current data: SELECT * FROM ${OUTPUT_TABLE}"
echo "   • Time travel:  SELECT * FROM ${OUTPUT_TABLE} FOR TIMESTAMP AS OF TIMESTAMP '2025-11-04 12:00:00'"
echo "   • Lineage:      SELECT * FROM audit.etl_lineage WHERE transform = 'ip_sum'"
echo ""
echo "🌐 Access Points:"
echo "   • Trino Web UI: http://localhost:8081"
echo "   • Shiny Frontend: http://localhost:8000"