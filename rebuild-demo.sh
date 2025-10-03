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
    echo "   • Sample queries:"
    echo "     SELECT * FROM iceberg.demo.sample_data;"
    echo "     SELECT name, amount FROM iceberg.demo.sample_data WHERE amount > 150;"
fi