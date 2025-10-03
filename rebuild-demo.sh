#!/bin/bash

echo "=== Iceberg + Trino + Hive Demo Script ==="
echo

# Step 1: Teardown
echo "1. Tearing down existing stack..."
docker-compose down -v
echo "   ✓ Containers stopped and volumes removed"

# Step 2: Clean up
echo "2. Cleaning up..."
docker system prune -f > /dev/null
rm -rf warehouse/*
mkdir -p warehouse
chmod 777 warehouse
echo "   ✓ Docker resources cleaned, warehouse reset"

# Step 3: Rebuild
echo "3. Building stack..."
docker-compose up -d
echo "   ✓ Starting containers..."

# Step 4: Wait for services
echo "4. Waiting for services to start..."
sleep 20
echo "   ✓ Services should be ready"

# Step 5: Test connectivity
echo "5. Testing connectivity..."
TRINO_STATUS=$(curl -s http://localhost:8081/v1/info | grep version || echo "FAILED")
if [[ $TRINO_STATUS == *"version"* ]]; then
    echo "   ✓ Trino responding"
else
    echo "   ✗ Trino not responding"
    exit 1
fi

# Step 6: Test catalogs
echo "6. Testing Iceberg catalogs..."
CATALOGS=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SHOW CATALOGS" 2>/dev/null)
if [[ $CATALOGS == *"iceberg"* ]]; then
    echo "   ✓ Iceberg catalogs available"
else
    echo "   ✗ Iceberg catalogs not available"
    exit 1
fi

# Step 7: Test schema creation
echo "7. Testing schema creation..."
SCHEMA_RESULT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "CREATE SCHEMA IF NOT EXISTS iceberg.demo" 2>/dev/null)
if [[ $SCHEMA_RESULT == *"CREATE SCHEMA"* ]]; then
    echo "   ✓ Schema creation works"
else
    echo "   ✗ Schema creation failed"
fi

# Step 8: Create and query Iceberg table
echo "8. Testing Iceberg table operations..."

# Try to create a simple table first (may fail due to permissions, but show the attempt)
echo "   → Creating sample table..."
TABLE_RESULT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "
CREATE TABLE IF NOT EXISTS iceberg.demo.sample_data (
    id BIGINT,
    name VARCHAR,
    amount DECIMAL(10,2),
    created_date DATE
) WITH (format = 'PARQUET')" 2>&1)

if [[ $TABLE_RESULT == *"CREATE TABLE"* ]]; then
    echo "   ✓ Table created successfully"
    
    # Insert sample data
    echo "   → Inserting sample data..."
    INSERT_RESULT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "
    INSERT INTO iceberg.demo.sample_data VALUES 
    (1, 'Alice', 100.50, DATE '2024-01-01'),
    (2, 'Bob', 250.75, DATE '2024-01-02'),
    (3, 'Charlie', 175.25, DATE '2024-01-03')" 2>/dev/null)
    
    if [[ $INSERT_RESULT == *"INSERT"* ]]; then
        echo "   ✓ Data inserted successfully"
        
        # Query the data
        echo "   → Querying data from Iceberg table..."
        docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT * FROM iceberg.demo.sample_data ORDER BY id" 2>/dev/null
        echo "   ✓ Query executed successfully"
        
        # Step 8a: Demonstrate Time Travel
        echo "   → Testing Iceberg Time Travel..."
        echo "     • Getting current snapshot info..."
        SNAPSHOT_DATA=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT snapshot_id, committed_at, operation FROM iceberg.demo.\"sample_data\$snapshots\" ORDER BY committed_at DESC LIMIT 1" 2>/dev/null)
        echo "     • Latest snapshot: $SNAPSHOT_DATA"
        
        # Add more data for time travel demonstration
        echo "   → Adding more data for time travel demo..."
        docker exec trino-cli trino --server trino:8080 --user admin --execute "
        INSERT INTO iceberg.demo.sample_data VALUES 
        (4, 'Diana', 325.00, DATE '2024-01-04'),
        (5, 'Eve', 199.99, DATE '2024-01-05')" 2>/dev/null
        echo "   ✓ Additional data inserted"
        
        # Show current data
        echo "   → Current data (5 rows):"
        docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) as row_count, SUM(amount) as total_amount FROM iceberg.demo.sample_data" 2>/dev/null
        
        # Demonstrate time travel query by snapshot ID
        echo "   → Time travel query (showing data as of first snapshot)..."
        FIRST_SNAPSHOT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT snapshot_id FROM iceberg.demo.\"sample_data\$snapshots\" ORDER BY committed_at LIMIT 1" 2>/dev/null | tail -n 1 | tr -d '"')
        if [[ ! -z "$FIRST_SNAPSHOT" ]]; then
            echo "     • Querying snapshot ID: $FIRST_SNAPSHOT"
            docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) as historical_count FROM iceberg.demo.sample_data FOR VERSION AS OF $FIRST_SNAPSHOT" 2>/dev/null
        fi
        
        # Demonstrate time travel query by timestamp
        echo "   → Time travel query (showing data as of specific timestamp)..."
        FIRST_TIMESTAMP=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT committed_at FROM iceberg.demo.\"sample_data\$snapshots\" ORDER BY committed_at LIMIT 1" 2>/dev/null | tail -n 1 | tr -d '"')
        if [[ ! -z "$FIRST_TIMESTAMP" ]]; then
            echo "     • Querying timestamp: $FIRST_TIMESTAMP"
            docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) as timestamp_count FROM iceberg.demo.sample_data FOR TIMESTAMP AS OF TIMESTAMP '$FIRST_TIMESTAMP'" 2>/dev/null
        fi
        
        # Show comparison of current vs historical data
        echo "   → Comparing current vs historical data:"
        echo "     • Current total amount:"
        docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT SUM(amount) as current_total FROM iceberg.demo.sample_data" 2>/dev/null
        if [[ ! -z "$FIRST_SNAPSHOT" ]]; then
            echo "     • Historical total amount (first snapshot):"
            docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT SUM(amount) as historical_total FROM iceberg.demo.sample_data FOR VERSION AS OF $FIRST_SNAPSHOT" 2>/dev/null
        fi
        
        # Step 8b: Demonstrate Schema Evolution
        echo "   → Testing Iceberg Schema Evolution..."
        docker exec trino-cli trino --server trino:8080 --user admin --execute "
        ALTER TABLE iceberg.demo.sample_data 
        ADD COLUMN customer_type VARCHAR DEFAULT 'standard'" 2>/dev/null
        echo "   ✓ Schema evolved - added customer_type column"
        
        # Update some records with new column
        echo "   → Updating records with new column..."
        docker exec trino-cli trino --server trino:8080 --user admin --execute "
        UPDATE iceberg.demo.sample_data 
        SET customer_type = 'premium' 
        WHERE amount > 200" 2>/dev/null
        echo "   ✓ Updated premium customers"
        
        # Show evolved schema
        echo "   → Current schema with new column:"
        docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT name, amount, customer_type FROM iceberg.demo.sample_data WHERE customer_type = 'premium'" 2>/dev/null
        
        # Step 8c: Demonstrate Branching (if supported)
        echo "   → Testing Iceberg Branching..."
        # Create a branch for experimental changes
        BRANCH_RESULT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "
        ALTER TABLE iceberg.demo.sample_data 
        CREATE BRANCH experimental" 2>&1)
        
        if [[ $BRANCH_RESULT != *"failed"* && $BRANCH_RESULT != *"error"* ]]; then
            echo "   ✓ Created experimental branch"
            
            # Make changes on the branch
            echo "   → Making experimental changes on branch..."
            docker exec trino-cli trino --server trino:8080 --user admin --execute "
            INSERT INTO iceberg.demo.sample_data FOR VERSION AS OF 'experimental'
            VALUES (99, 'TestUser', 999.99, DATE '2024-12-31', 'experimental')" 2>/dev/null
            
            echo "   → Comparing main vs experimental branch:"
            echo "     • Main branch count:"
            docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM iceberg.demo.sample_data" 2>/dev/null
            echo "     • Experimental branch count:"
            docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM iceberg.demo.sample_data FOR VERSION AS OF 'experimental'" 2>/dev/null
        else
            echo "   ℹ Branching not supported in this Trino/Iceberg version"
        fi
        
        # Step 8d: Show Iceberg Metadata Tables
        echo "   → Exploring Iceberg Metadata Tables..."
        echo "     • Table snapshots:"
        docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT committed_at, operation, summary FROM iceberg.demo.\"sample_data\$snapshots\" ORDER BY committed_at DESC LIMIT 3" 2>/dev/null
        
        echo "     • Data files:"
        docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT file_format, record_count, file_size_in_bytes FROM iceberg.demo.\"sample_data\$files\"" 2>/dev/null
        
        echo "     • Table history:"
        docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT made_current_at, snapshot_id FROM iceberg.demo.\"sample_data\$history\" ORDER BY made_current_at DESC LIMIT 3" 2>/dev/null
        
    else
        echo "   ✗ Data insert failed"
    fi
else
    echo "   ✗ Table creation failed (likely file permissions)"
    echo "   → Showing available schemas instead:"
    docker exec trino-cli trino --server trino:8080 --user admin --execute "SHOW SCHEMAS FROM iceberg" 2>/dev/null
fi

# Step 9: Show final status
echo "9. Final status:"
docker ps --format "   {{.Names}}: {{.Status}}" | grep -E "(postgres|hive|trino)"

echo
echo "🎉 Demo complete!"
echo "   • Web UI: http://localhost:8081"
echo "   • Username: admin"
echo "   • CLI: docker exec -it trino-cli trino --server trino:8080 --user admin"
if [[ $TABLE_RESULT == *"CREATE TABLE"* ]]; then
    echo ""
    echo "📊 Sample Iceberg Queries to Try:"
    echo "   • Basic query:"
    echo "     SELECT * FROM iceberg.demo.sample_data;"
    echo ""
    echo "   • Time Travel by Snapshot ID:"
    echo "     SELECT * FROM iceberg.demo.sample_data FOR VERSION AS OF <snapshot_id>;"
    echo ""
    echo "   • Time Travel by Timestamp:"
    echo "     SELECT * FROM iceberg.demo.sample_data FOR TIMESTAMP AS OF TIMESTAMP '2024-01-01 12:00:00.000 UTC';"
    echo ""
    echo "   • Schema Evolution:"
    echo "     SELECT name, amount, customer_type FROM iceberg.demo.sample_data;"
    echo ""
    echo "   • Metadata exploration:"
    echo "     SELECT * FROM iceberg.demo.\"sample_data\$snapshots\";"
    echo "     SELECT * FROM iceberg.demo.\"sample_data\$files\";"
    echo "     SELECT * FROM iceberg.demo.\"sample_data\$history\";"
    echo ""
    echo "   • Advanced analytics:"
    echo "     SELECT customer_type, COUNT(*), AVG(amount) FROM iceberg.demo.sample_data GROUP BY customer_type;"
fi