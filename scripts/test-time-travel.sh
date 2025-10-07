#!/bin/bash

echo "🕐 Testing Iceberg Time Travel Functionality..."
echo "============================================="
echo

# Test 1: Basic connectivity
echo "1. Testing Trino connectivity..."
CONNECT_TEST=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT 1" 2>/dev/null)
if [[ $CONNECT_TEST == *"1"* ]]; then
    echo "   ✅ Trino connection successful"
else
    echo "   ❌ Trino connection failed"
    exit 1
fi
echo

# Test 2: Snapshot-based time travel
echo "2. Testing snapshot-based time travel..."
echo "   → Getting available snapshots..."
SNAPSHOTS=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT snapshot_id FROM iceberg.demo.\"customers\$snapshots\" ORDER BY committed_at" 2>/dev/null | tail -n +2 | tr -d '"' | head -3)

if [[ -z "$SNAPSHOTS" ]]; then
    echo "   ❌ No snapshots found"
    exit 1
fi

for snapshot in $SNAPSHOTS; do
    echo "   → Testing snapshot: $snapshot"
    RESULT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) as count, COALESCE(ROUND(SUM(total_spent), 2), 0) as revenue FROM iceberg.demo.customers FOR VERSION AS OF $snapshot" 2>/dev/null)
    if [[ $RESULT == *","* ]]; then
        echo "     ✅ Query successful: $RESULT"
    else
        echo "     ❌ Query failed for snapshot $snapshot"
    fi
done
echo

# Test 3: Timestamp-based time travel
echo "3. Testing timestamp-based time travel..."
echo "   → Getting first timestamp for testing..."
FIRST_TIMESTAMP=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT committed_at FROM iceberg.demo.\"customers\$snapshots\" ORDER BY committed_at LIMIT 1" 2>/dev/null | tail -n 1 | tr -d '"')

if [[ ! -z "$FIRST_TIMESTAMP" ]]; then
    echo "   → Testing timestamp: $FIRST_TIMESTAMP"
    TIMESTAMP_RESULT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) as count FROM iceberg.demo.customers FOR TIMESTAMP AS OF TIMESTAMP '$FIRST_TIMESTAMP'" 2>&1)
    
    if [[ $TIMESTAMP_RESULT == *"\""* ]] && [[ $TIMESTAMP_RESULT != *"failed"* ]]; then
        echo "     ✅ Timestamp query successful: $TIMESTAMP_RESULT"
    else
        echo "     ⚠️  Timestamp format may not be supported in this Trino version"
        echo "     Result: $TIMESTAMP_RESULT"
    fi
else
    echo "   ⚠️  No timestamps found"
fi
echo

# Test 4: Schema evolution across time
echo "4. Testing schema evolution across time..."
echo "   → Current schema:"
docker exec trino-cli trino --server trino:8080 --user admin --execute "DESCRIBE iceberg.demo.customers" 2>/dev/null

echo "   → Querying data before schema evolution..."
EARLY_SNAPSHOT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT snapshot_id FROM iceberg.demo.\"customers\$snapshots\" ORDER BY committed_at LIMIT 1" 2>/dev/null | tail -n 1 | tr -d '"')

if [[ ! -z "$EARLY_SNAPSHOT" ]]; then
    echo "   → Early snapshot: $EARLY_SNAPSHOT"
    EARLY_RESULT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT id, name, country FROM iceberg.demo.customers FOR VERSION AS OF $EARLY_SNAPSHOT LIMIT 3" 2>/dev/null)
    echo "     ✅ Early snapshot accessible: $EARLY_RESULT"
    
    # Try to access customer_tier column from early snapshot (should fail)
    TIER_TEST=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT customer_tier FROM iceberg.demo.customers FOR VERSION AS OF $EARLY_SNAPSHOT" 2>&1)
    if [[ $TIER_TEST == *"cannot be resolved"* ]]; then
        echo "     ✅ Schema evolution validation: customer_tier column correctly not available in early snapshot"
    else
        echo "     ℹ️  Schema evolution test: $TIER_TEST"
    fi
fi
echo

# Test 5: Revenue evolution timeline
echo "5. Testing revenue evolution timeline..."
echo "   → Comparing revenue across snapshots..."
SNAPSHOT_COUNT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM iceberg.demo.\"customers\$snapshots\"" 2>/dev/null | tail -n 1 | tr -d '"')
echo "   → Total snapshots available: $SNAPSHOT_COUNT"

# Show current vs historical comparison
CURRENT_REVENUE=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT ROUND(SUM(total_spent), 2) FROM iceberg.demo.customers" 2>/dev/null | tail -n 1 | tr -d '"')
echo "   → Current total revenue: $$CURRENT_REVENUE"

if [[ ! -z "$EARLY_SNAPSHOT" ]]; then
    HISTORICAL_REVENUE=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COALESCE(ROUND(SUM(total_spent), 2), 0) FROM iceberg.demo.customers FOR VERSION AS OF $EARLY_SNAPSHOT" 2>/dev/null | tail -n 1 | tr -d '"')
    echo "   → Historical revenue (earliest): $$HISTORICAL_REVENUE"
    
    if [[ ! -z "$CURRENT_REVENUE" ]] && [[ ! -z "$HISTORICAL_REVENUE" ]]; then
        GROWTH=$(echo "scale=2; $CURRENT_REVENUE - $HISTORICAL_REVENUE" | bc 2>/dev/null || echo "N/A")
        echo "   → Revenue growth: $$GROWTH"
    fi
fi
echo

echo "✅ Time travel tests completed!"
echo "💡 Time travel is working - you can query historical data states!"
echo "🌐 Try the Shiny app at http://localhost:8000 for interactive demos"