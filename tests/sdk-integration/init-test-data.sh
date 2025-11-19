#!/bin/bash
set -e

echo "════════════════════════════════════════════════════════════════"
echo "📊 Initializing SDK Integration Test Data"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Configuration
SCHEMA_NAME="data_pipeline"

# Step 1: Check infrastructure
echo "1. Checking Docker infrastructure..."
if ! docker ps | grep -q "spark-iceberg"; then
    echo "   ❌ Spark container not running"
    echo "   Run: docker-compose up -d"
    exit 1
fi

if ! docker ps | grep -q "trino"; then
    echo "   ❌ Trino container not running"
    echo "   Run: docker-compose up -d"
    exit 1
fi
echo "   ✓ Docker infrastructure is running"
echo ""

# Step 2: Wait for Trino to be ready
echo "2. Waiting for Trino to be ready..."
MAX_RETRIES=10
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if timeout 5 docker exec trino trino --execute "SELECT 1" >/dev/null 2>&1; then
        echo "   ✓ Trino is ready"
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
        echo "   ❌ Trino failed to become ready after ${MAX_RETRIES} attempts"
        exit 1
    fi
    echo "   → Waiting for Trino... (attempt $RETRY_COUNT/$MAX_RETRIES)"
    sleep 3
done
echo ""

# Step 3: Create schemas
echo "3. Creating schemas..."
docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.${SCHEMA_NAME}" 2>/dev/null
echo "   ✓ Created schema: iceberg.${SCHEMA_NAME}"

docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.audit" 2>/dev/null
echo "   ✓ Created schema: iceberg.audit"
echo ""

# Step 4: Create and populate input table
echo "4. Creating input test data..."
docker exec trino trino --execute "
CREATE TABLE IF NOT EXISTS iceberg.${SCHEMA_NAME}.full_name_input (
    id BIGINT,
    full_name VARCHAR
) WITH (format = 'PARQUET')
" 2>/dev/null

# Check if data already exists
EXISTING_COUNT=$(docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.${SCHEMA_NAME}.full_name_input" 2>/dev/null | tail -1 | tr -d '"')

if [[ "$EXISTING_COUNT" == "0" ]]; then
    echo "   → Inserting test data..."
    docker exec trino trino --execute "
    INSERT INTO iceberg.${SCHEMA_NAME}.full_name_input VALUES
    (1, 'John Michael Doe'),
    (2, 'Jane Elizabeth Smith'),
    (3, 'Carlos Rodriguez'),
    (4, 'Li Wei Zhang'),
    (5, 'Maria Garcia Lopez'),
    (6, 'Ahmed Hassan Mohamed'),
    (7, 'Anna Schmidt Mueller'),
    (8, 'Yuki Tanaka'),
    (9, 'Raj Kumar Patel'),
    (10, 'Sofia Andersson Berg')
    " 2>/dev/null
    echo "   ✓ Inserted 10 test records"
else
    echo "   ✓ Test data already exists ($EXISTING_COUNT rows)"
fi
echo ""

# Step 5: Verify data
echo "5. Verifying test data..."
COUNT=$(docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.${SCHEMA_NAME}.full_name_input" 2>/dev/null | tail -1 | tr -d '"')
echo "   ✓ Input table has $COUNT records"
echo ""

# Step 6: Show sample
echo "6. Sample data:"
docker exec trino trino --execute "SELECT * FROM iceberg.${SCHEMA_NAME}.full_name_input LIMIT 3" 2>/dev/null
echo ""

echo "════════════════════════════════════════════════════════════════"
echo "✅ Test Data Initialization Complete!"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "📊 Created:"
echo "   • Schema: iceberg.${SCHEMA_NAME}"
echo "   • Schema: iceberg.audit (for lineage)"
echo "   • Table: iceberg.${SCHEMA_NAME}.full_name_input ($COUNT rows)"
echo ""
echo "🚀 Next steps:"
echo "   1. Run SDK tests: make test-sdk"
echo "   2. Or run directly: ./tests/sdk-integration/test-sdk.sh"
echo ""
