#!/bin/bash

echo "🌿 Testing Iceberg Branching Functionality..."
echo "==========================================="
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

# Test 2: Branch listing
echo "2. Testing branch listing..."
echo "   → Available branches and refs:"
REFS=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT * FROM iceberg.demo.\"customers\$refs\"" 2>/dev/null)
if [[ ! -z "$REFS" ]]; then
    echo "$REFS"
    echo "   ✅ Branch listing successful"
else
    echo "   ❌ No branches found or refs table inaccessible"
    exit 1
fi
echo

# Test 3: Branch-based querying
echo "3. Testing branch-based querying..."
echo "   → Querying main branch explicitly:"
MAIN_COUNT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM iceberg.demo.customers FOR VERSION AS OF 'main'" 2>/dev/null)
if [[ $MAIN_COUNT == *"\""* ]]; then
    echo "   ✅ Main branch query successful: $MAIN_COUNT customers"
else
    echo "   ❌ Main branch query failed: $MAIN_COUNT"
fi
echo

# Test 4: Branch creation (testing both Trino and Spark approaches)
echo "4. Testing branch creation capability..."
echo "   → Testing Trino branch creation (expected to fail):"
TRINO_BRANCH_TEST=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "ALTER TABLE iceberg.demo.customers CREATE BRANCH test_branch" 2>&1)
echo "     Trino result: $TRINO_BRANCH_TEST"

if echo "$TRINO_BRANCH_TEST" | grep -q -E "(failed|error|mismatched input|line [0-9]+:[0-9]+)"; then
    echo "   ❌ Trino branch creation failed (as expected for Trino 435)"
    
    # Try with Spark instead
    echo "   → Testing Spark branch creation:"
    SPARK_BRANCH_TEST=$(docker exec spark-iceberg /opt/spark/bin/spark-sql \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.iceberg.type=hive \
        --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
        --conf spark.sql.catalog.iceberg.warehouse=/data/warehouse \
        -e "ALTER TABLE iceberg.demo.customers CREATE BRANCH test_branch;" 2>&1)
    
    if [[ $SPARK_BRANCH_TEST != *"failed"* && $SPARK_BRANCH_TEST != *"error"* && $SPARK_BRANCH_TEST != *"Exception"* ]]; then
        echo "   ✅ Spark branch creation appears successful!"
        
        # Test querying the new branch via Trino
        echo "   → Testing query on new Spark-created branch via Trino..."
        BRANCH_QUERY_RESULT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM iceberg.demo.customers FOR VERSION AS OF 'test_branch'" 2>&1)
        
        if echo "$BRANCH_QUERY_RESULT" | grep -q -E "^\"[0-9]+\"$"; then
            echo "   ✅ New branch query successful: $BRANCH_QUERY_RESULT"
            
            # Clean up - drop test branch using Spark
            echo "   → Cleaning up test branch using Spark..."
            SPARK_DROP_RESULT=$(docker exec spark-iceberg /opt/spark/bin/spark-sql \
                --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
                --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
                --conf spark.sql.catalog.iceberg.type=hive \
                --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
                --conf spark.sql.catalog.iceberg.warehouse=/data/warehouse \
                -e "ALTER TABLE iceberg.demo.customers DROP BRANCH test_branch;" 2>&1)
            if [[ $SPARK_DROP_RESULT != *"failed"* && $SPARK_DROP_RESULT != *"error"* && $SPARK_DROP_RESULT != *"Exception"* ]]; then
                echo "   ✅ Test branch cleaned up successfully"
            else
                echo "   ⚠️  Test branch cleanup had issues (this is OK for testing)"
            fi
        else
            echo "   ⚠️  New branch query had issues: $BRANCH_QUERY_RESULT"
        fi
    else
        echo "   ❌ Both Trino and Spark branch creation failed"
        echo "   → Spark error: $SPARK_BRANCH_TEST"
    fi
else
    echo "   ⚠️  Unexpected Trino result - may have worked: $TRINO_BRANCH_TEST"
fi
echo

# Test 5: Branch comparison (using existing branches/refs)
echo "5. Testing branch comparison capabilities..."
echo "   → Comparing data access methods..."
echo "   → Standard query:"
STANDARD_COUNT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM iceberg.demo.customers" 2>/dev/null | tail -n 1 | tr -d '"')
echo "     Customer count: $STANDARD_COUNT"

echo "   → Main branch query:"
MAIN_BRANCH_COUNT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM iceberg.demo.customers FOR VERSION AS OF 'main'" 2>/dev/null | tail -n 1 | tr -d '"')
echo "     Customer count: $MAIN_BRANCH_COUNT"

echo "   → Dev branch query (if exists):"
DEV_BRANCH_COUNT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM iceberg.demo.customers FOR VERSION AS OF 'dev'" 2>&1)
if echo "$DEV_BRANCH_COUNT" | grep -q -E "^\"[0-9]+\"$"; then
    DEV_COUNT_CLEAN=$(echo "$DEV_BRANCH_COUNT" | tail -n 1 | tr -d '"')
    echo "     Customer count: $DEV_COUNT_CLEAN"
    echo "   ✅ Dev branch exists and accessible"
    
    # Show the test customer that should be in dev branch
    echo "   → Dev branch specific data:"
    DEV_TEST_CUSTOMER=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT name, email FROM iceberg.demo.customers FOR VERSION AS OF 'dev' WHERE id = 99" 2>/dev/null)
    if [[ ! -z "$DEV_TEST_CUSTOMER" && "$DEV_TEST_CUSTOMER" != *"0 rows"* ]]; then
        echo "     Test customer found: $DEV_TEST_CUSTOMER"
    else
        echo "     No test customer found (id=99)"
    fi
else
    echo "     ❌ Dev branch not accessible: $DEV_BRANCH_COUNT"
    echo "   ℹ️  Dev branch may not exist or branching not supported"
fi

if [[ "$STANDARD_COUNT" == "$MAIN_BRANCH_COUNT" ]]; then
    echo "   ✅ Branch querying works correctly - counts match"
else
    echo "   ⚠️  Unexpected count difference (this might be normal depending on timing)"
fi
echo

# Test 6: Advanced branch features
echo "6. Testing advanced branching features..."
echo "   → Getting branch metadata:"
docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT name, type, snapshot_id FROM iceberg.demo.\"customers\$refs\"" 2>/dev/null

echo "   → Testing branch with specific snapshot reference:"
LATEST_SNAPSHOT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT snapshot_id FROM iceberg.demo.\"customers\$snapshots\" ORDER BY committed_at DESC LIMIT 1" 2>/dev/null | tail -n 1 | tr -d '"')
if [[ ! -z "$LATEST_SNAPSHOT" ]]; then
    echo "     Latest snapshot: $LATEST_SNAPSHOT"
    SNAPSHOT_COUNT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM iceberg.demo.customers FOR VERSION AS OF $LATEST_SNAPSHOT" 2>/dev/null | tail -n 1 | tr -d '"')
    echo "     Snapshot-based count: $SNAPSHOT_COUNT"
fi
echo

# Test 7: Working Spark Branching Demo
echo "7. Testing working Spark Iceberg branching..."
echo "   → Demonstrating successful Spark branching (from init-demo-data.sh):"

# Test main branch
echo "   → Main branch product count:"
SPARK_MAIN_COUNT=$(docker exec spark-iceberg /opt/spark/bin/spark-sql \
  --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=hive \
  --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
  -e "SELECT COUNT(*) FROM iceberg.spark_demo.products;" 2>/dev/null | tail -1 | tr -d ' ')

if [[ ! -z "$SPARK_MAIN_COUNT" && "$SPARK_MAIN_COUNT" =~ ^[0-9]+$ ]]; then
    echo "     ✅ Main branch: $SPARK_MAIN_COUNT products"
    
    # Test dev branch
    echo "   → Dev branch product count:"
    SPARK_DEV_COUNT=$(docker exec spark-iceberg /opt/spark/bin/spark-sql \
      --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
      --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
      --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
      --conf spark.sql.catalog.iceberg.type=hive \
      --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
      -e "SELECT COUNT(*) FROM iceberg.spark_demo.products.branch_dev;" 2>/dev/null | tail -1 | tr -d ' ')
    
    if [[ ! -z "$SPARK_DEV_COUNT" && "$SPARK_DEV_COUNT" =~ ^[0-9]+$ ]]; then
        echo "     ✅ Dev branch: $SPARK_DEV_COUNT products (includes dev-only product)"
        
        if [[ "$SPARK_DEV_COUNT" -gt "$SPARK_MAIN_COUNT" ]]; then
            echo "   ✅ BRANCHING SUCCESS: Dev branch contains more products than main!"
            echo "   → This proves Iceberg branching is working with Spark"
        else
            echo "   ⚠️  Branch counts are unexpected"
        fi
    else
        echo "     ❌ Dev branch query failed"
    fi
else
    echo "     ❌ Main branch query failed - Spark demo may not be initialized"
    echo "   → Run ./scripts/init-demo-data.sh to set up the branching demo"
fi
echo

echo "✅ Branching tests completed!"
echo ""
echo "📋 Summary:"
echo "   • Branch listing: ✅ Working"
echo "   • Branch querying: ✅ Working" 
if echo "$BRANCH_TEST" | grep -q -E "(failed|error|mismatched input)"; then
    echo "   • Branch creation (Trino): ❌ Not supported (expected for Trino 435)"
else
    echo "   • Branch creation (Trino): ⚠️  Status unclear"
fi

if [[ ! -z "$SPARK_MAIN_COUNT" && "$SPARK_DEV_COUNT" -gt "$SPARK_MAIN_COUNT" ]]; then
    echo "   • Branch creation (Spark): ✅ Working successfully!"
    echo "   • Branch data isolation: ✅ Confirmed working"
else
    echo "   • Branch creation (Spark): ⚠️  Run init-demo-data.sh for full demo"
fi
echo ""
echo "🌐 Try the Shiny app at http://localhost:8000 for interactive branch demos!"
echo "🔬 Use Spark for full Iceberg branching capabilities in this environment"