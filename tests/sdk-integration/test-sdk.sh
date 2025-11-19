#!/bin/bash
set -e

echo "════════════════════════════════════════════════════════════════"
echo "🧪 Testing DMAP Data SDK in Spark Container"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Check if spark container is running
if ! docker ps | grep -q spark-iceberg; then
    echo "❌ Error: spark-iceberg container is not running"
    echo "   Run: docker-compose up -d spark"
    exit 1
fi

# Check if Trino/Hive stack is healthy
echo "🔍 Checking Trino/Hive stack health..."
if ! timeout 5 docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT 1" >/dev/null 2>&1; then
    echo "⚠️  Trino is not responding or slow - services may need restart"
    echo "   Try: docker-compose restart postgres hive-metastore trino"
    echo "   Then wait 30 seconds and retry this script"
    exit 1
fi
echo "   ✓ Trino/Hive stack is responding"
echo ""

echo "📦 Step 1: Copying SDK to container..."
docker cp ./dmap-data-sdk spark-iceberg:/opt/dmap-data-sdk
echo "   ✓ SDK copied to /opt/dmap-data-sdk"
echo ""

echo "📥 Step 2: Installing SDK in container..."
docker exec spark-iceberg pip3 install /opt/dmap-data-sdk
echo "   ✓ SDK installed"
echo ""

echo "⚙️  Step 3: Copying configuration..."
if [ -f ./pipeline_config.yaml ]; then
    docker cp ./pipeline_config.yaml spark-iceberg:/opt/pipeline_config.yaml
    echo "   ✓ Config copied to /opt/pipeline_config.yaml"
else
    echo "   ⚠️  Warning: pipeline_config.yaml not found, using default SDK config"
fi
echo ""

echo "✅ Step 4: Testing SDK import..."
docker exec spark-iceberg python3 -c "
from dmap_data_sdk import DataPipeline, Ref, PlatformFactory
print('   ✓ DataPipeline imported')
print('   ✓ Ref imported')
print('   ✓ PlatformFactory imported')
print('   ✓ SDK loaded successfully!')
"
echo ""

echo "🔍 Step 5: Checking existing test data..."
docker exec spark-iceberg bash -c "
spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=hive \
  --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.iceberg.warehouse=/data/warehouse \
  -e 'SHOW DATABASES;' 2>/dev/null | grep -q 'data_pipeline'
" && echo "   ✓ data_pipeline database exists" || echo "   ⚠️  data_pipeline database not found (will be created)"
echo ""

echo "🚀 Step 6: Running SDK test pipeline..."
echo "   Source: iceberg.data_pipeline.full_name_input"
echo "   Target: iceberg.data_pipeline.ip_sum_output_sdk_test"
echo ""

docker exec spark-iceberg bash -c "
cd /opt && \
/opt/spark/bin/spark-submit \
  --master local[*] \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=hive \
  --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.iceberg.warehouse=/data/warehouse \
  --conf spark.sql.defaultCatalog=iceberg \
  /opt/dmap-data-sdk/examples/ip_sum_example.py \
  iceberg.data_pipeline.full_name_input \
  iceberg.data_pipeline.ip_sum_output_sdk_test
"

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "✅ SDK Test Completed Successfully!"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "📊 Next steps:"
echo "   1. Query the results with Trino:"
echo "      docker exec -it trino-cli trino --server http://trino:8080"
echo "      SELECT * FROM iceberg.data_pipeline.ip_sum_output_sdk_test LIMIT 10;"
echo ""
echo "   2. Check lineage (if enabled):"
echo "      SELECT * FROM iceberg.audit.etl_lineage"
echo "      WHERE target_table = 'iceberg.data_pipeline.ip_sum_output_sdk_test'"
echo "      ORDER BY recorded_at DESC LIMIT 5;"
echo ""
echo "   3. View in your pipeline repo:"
echo "      Update src/pipelines/ip_sum/ip_sum_iceberg_refactor.py"
echo "      Replace sys.path.append with: from dmap_data_sdk import DataPipeline, Ref"
echo ""
