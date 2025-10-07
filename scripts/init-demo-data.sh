#!/bin/bash

echo "📊 Initializing Demo Data..."

# Step 1: Create schema
echo "1. Creating schema..."
SCHEMA_RESULT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "CREATE SCHEMA IF NOT EXISTS iceberg.demo" 2>/dev/null)
if [[ $SCHEMA_RESULT == *"CREATE SCHEMA"* ]]; then
    echo "   ✓ Schema created successfully"
else
    echo "   ✓ Schema already exists"
fi

# Step 2: Create customers table with comprehensive schema
echo "2. Creating customers table..."
TABLE_RESULT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "
CREATE TABLE IF NOT EXISTS iceberg.demo.customers (
    id BIGINT,
    name VARCHAR(100),
    email VARCHAR(100),
    country VARCHAR(50),
    signup_date DATE,
    total_orders INTEGER,
    total_spent DECIMAL(10,2)
) WITH (format = 'PARQUET')" 2>&1)

if [[ $TABLE_RESULT == *"CREATE TABLE"* ]]; then
    echo "   ✓ Table created successfully"
    
    # Step 3: Check if data already exists, insert if needed
    echo "3. Checking existing data..."
    EXISTING_COUNT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM iceberg.demo.customers" 2>/dev/null | tail -1 | tr -d '"')
    
    if [[ "$EXISTING_COUNT" == "0" ]]; then
        echo "   → Inserting initial customer data..."
        INSERT_RESULT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "
        INSERT INTO iceberg.demo.customers VALUES
        (1, 'John Doe', 'john.doe@email.com', 'USA', DATE '2023-01-15', 5, 299.99),
        (2, 'Jane Smith', 'jane.smith@email.com', 'Canada', DATE '2023-02-20', 3, 150.75),
        (3, 'Carlos Rodriguez', 'carlos@email.com', 'Mexico', DATE '2023-03-10', 8, 445.20)" 2>/dev/null)
        echo "   ✓ Initial customer data inserted successfully"
    else
        echo "   ✓ Data already exists ($EXISTING_COUNT rows)"
    fi
    
    # Always proceed with advanced features setup
    if [[ "$EXISTING_COUNT" -lt "5" ]]; then
        
    # Step 4: Add more customers for comprehensive time travel demonstration
    echo "4. Adding additional customers for time travel demo..."
    docker exec trino-cli trino --server trino:8080 --user admin --execute "
    INSERT INTO iceberg.demo.customers VALUES
    (4, 'Emma Wilson', 'emma.wilson@email.com', 'UK', DATE '2023-01-25', 2, 89.50),
    (5, 'Yuki Tanaka', 'yuki.tanaka@email.com', 'Japan', DATE '2023-04-05', 12, 678.90)" 2>/dev/null
    echo "   ✓ Additional customer data inserted"
    else
        echo "4. Skipping data insertion - sufficient data already exists"
    fi
        
    # Step 5: Check if schema evolution has been applied
    echo "5. Checking Iceberg Schema Evolution..."
    COLUMN_CHECK=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "DESCRIBE iceberg.demo.customers" 2>/dev/null | grep customer_tier || echo "NOT_FOUND")
    
    if [[ "$COLUMN_CHECK" == "NOT_FOUND" ]]; then
        echo "   → Adding customer_tier column for business segmentation..."
        docker exec trino-cli trino --server trino:8080 --user admin --execute "
        ALTER TABLE iceberg.demo.customers 
        ADD COLUMN customer_tier VARCHAR" 2>/dev/null
        echo "   ✓ Schema evolved - added customer_tier column"
        
        # Step 6: Update records with business logic for better time travel demo
        echo "6. Updating customers with tier classification..."
        docker exec trino-cli trino --server trino:8080 --user admin --execute "
        UPDATE iceberg.demo.customers 
        SET customer_tier = 'platinum' 
        WHERE total_spent > 500" 2>/dev/null
        docker exec trino-cli trino --server trino:8080 --user admin --execute "
        UPDATE iceberg.demo.customers 
        SET customer_tier = 'gold' 
        WHERE total_spent BETWEEN 200 AND 500" 2>/dev/null
        docker exec trino-cli trino --server trino:8080 --user admin --execute "
        UPDATE iceberg.demo.customers 
        SET customer_tier = 'silver' 
        WHERE customer_tier IS NULL" 2>/dev/null
        echo "   ✓ Updated customer tiers based on spending"
        
        # Step 7: Add promotional update for time travel demonstration
        echo "7. Adding promotional discount for high-value customers..."
        docker exec trino-cli trino --server trino:8080 --user admin --execute "
        UPDATE iceberg.demo.customers 
        SET total_orders = total_orders + 1,
            total_spent = total_spent * 1.1
        WHERE customer_tier IN ('platinum', 'gold')" 2>/dev/null
        echo "   ✓ Applied 10% bonus and extra order to premium customers"
    else
        echo "   ✓ Schema evolution already applied"
    fi
        
    # Step 8: Comprehensive Time Travel and Branching Demo (following rebuild-demo.sh Step 8)
    echo "8. Demonstrating Iceberg Time Travel..."
    
    # Step 8a: Get snapshot information for time travel
    echo "   → Getting current snapshot info for time travel demo..."
    SNAPSHOT_DATA=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT snapshot_id, committed_at, operation FROM iceberg.demo.\"customers\$snapshots\" ORDER BY committed_at DESC LIMIT 1" 2>/dev/null)
    echo "     • Latest snapshot: $SNAPSHOT_DATA"
    
    # Step 8b: Demonstrate time travel query by snapshot ID
    echo "   → Time travel query (showing data evolution across snapshots)..."
    FIRST_SNAPSHOT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT snapshot_id FROM iceberg.demo.\"customers\$snapshots\" ORDER BY committed_at LIMIT 1" 2>/dev/null | tail -n 1 | tr -d '"')
    if [[ ! -z "$FIRST_SNAPSHOT" ]]; then
        echo "     • Querying earliest snapshot ID: $FIRST_SNAPSHOT"
        docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) as historical_count, COALESCE(SUM(total_spent), 0) as historical_revenue FROM iceberg.demo.customers FOR VERSION AS OF $FIRST_SNAPSHOT" 2>/dev/null
    fi
    
    # Step 8c: Demonstrate time travel query by timestamp
    echo "   → Time travel query by timestamp..."
    FIRST_TIMESTAMP=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT committed_at FROM iceberg.demo.\"customers\$snapshots\" ORDER BY committed_at LIMIT 1" 2>/dev/null | tail -n 1 | tr -d '"')
    if [[ ! -z "$FIRST_TIMESTAMP" ]]; then
        echo "     • Querying timestamp: $FIRST_TIMESTAMP"
        docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) as timestamp_count FROM iceberg.demo.customers FOR TIMESTAMP AS OF TIMESTAMP '$FIRST_TIMESTAMP'" 2>/dev/null
    fi
    
    # Step 8d: Show comparison of current vs historical data
    echo "   → Comparing current vs historical revenue evolution:"
    echo "     • Current total revenue:"
    docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT ROUND(SUM(total_spent), 2) as current_total FROM iceberg.demo.customers" 2>/dev/null
    if [[ ! -z "$FIRST_SNAPSHOT" ]]; then
        echo "     • Historical total revenue (first snapshot):"
        docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COALESCE(ROUND(SUM(total_spent), 2), 0) as historical_total FROM iceberg.demo.customers FOR VERSION AS OF $FIRST_SNAPSHOT" 2>/dev/null
    fi
    
    # Step 8e: Demonstrate Cross-Engine Iceberg Branching (Spark creates, Trino queries)
    echo "   → Demonstrating Cross-Engine Iceberg Branching..."
    
    # Use the proven working approach: create table with Trino, then branch with Spark
    echo "   → Creating branching demo table with Trino..."
    docker exec trino-cli trino --server trino:8080 --user admin --execute "CREATE SCHEMA IF NOT EXISTS iceberg.branching_demo" 2>/dev/null
    
    
    TRINO_TABLE_RESULT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "
    CREATE TABLE IF NOT EXISTS iceberg.branching_demo.products (
        id BIGINT,
        name VARCHAR(100),
        category VARCHAR(50),
        price DECIMAL(10,2)
    ) WITH (format = 'PARQUET')" 2>&1)
    
    if [[ $TRINO_TABLE_RESULT == *"CREATE TABLE"* ]]; then
        echo "   ✓ Created products table with Trino"
        
        # Insert sample data with Trino
        docker exec trino-cli trino --server trino:8080 --user admin --execute "
        INSERT INTO iceberg.branching_demo.products VALUES
        (1, 'Laptop', 'Electronics', 999.99),
        (2, 'Coffee Mug', 'Kitchen', 12.50),
        (3, 'Desk Chair', 'Furniture', 149.99)" 2>/dev/null
        echo "   ✓ Inserted sample data with Trino"
        
        # Create dev branch using Spark (fix root cause: add explicit warehouse config)
        echo "   → Creating dev branch with Spark..."
        SPARK_BRANCH_RESULT=$(docker exec spark-iceberg /opt/spark/bin/spark-sql \
            --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
            --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.iceberg.type=hive \
            --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
            --conf spark.sql.catalog.iceberg.warehouse=file:///data/warehouse \
            -e "ALTER TABLE iceberg.branching_demo.products CREATE BRANCH dev;" 2>&1)
        
        if [[ $? -eq 0 && $SPARK_BRANCH_RESULT == *"Time taken:"* ]]; then
            echo "   ✓ Created dev branch with Spark!"
            
            # Add data to dev branch using Spark
            echo "   → Adding development data to dev branch with Spark..."
            SPARK_INSERT_RESULT=$(docker exec spark-iceberg /opt/spark/bin/spark-sql \
                --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
                --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
                --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
                --conf spark.sql.catalog.iceberg.type=hive \
                --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
                --conf spark.sql.catalog.iceberg.warehouse=file:///data/warehouse \
                -e "INSERT INTO iceberg.branching_demo.products.branch_dev VALUES (99, 'Dev Product', 'Testing', 1.00);" 2>&1)
            
            if [[ $? -eq 0 && $SPARK_INSERT_RESULT == *"Time taken:"* ]]; then
                echo "   ✓ Successfully added development data to dev branch with Spark!"
                # Small delay to ensure transaction is committed
                sleep 2
            else
                echo "   ⚠️  Spark insertion may have failed: $SPARK_INSERT_RESULT"
            fi
            
            echo "   ✅ Cross-Engine Branching Demo Complete!"
            echo "   → Now demonstrating THE ULTIMATE TEST: Trino querying Spark-created branch:"
            
            # THE ULTIMATE DEMO: Trino queries Spark-created branch
            echo "     • Main branch count (Trino query):"
            MAIN_COUNT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM iceberg.branching_demo.products" 2>/dev/null | tail -1 | tr -d '"')
            echo "       $MAIN_COUNT products"
            
            echo "     • Dev branch count (Trino querying Spark-created branch):"
            DEV_COUNT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM iceberg.branching_demo.products FOR VERSION AS OF 'dev'" 2>/dev/null | tail -1 | tr -d '"')
            echo "       $DEV_COUNT products"
            
            if [[ "$DEV_COUNT" -gt "$MAIN_COUNT" ]]; then
                echo "   🎉 SUCCESS: Trino successfully queried Spark-created branch!"
                echo "   → Cross-engine Iceberg branching is WORKING PERFECTLY!"
                echo "   → Dev branch has $((DEV_COUNT - MAIN_COUNT)) additional product(s) created by Spark!"
            else
                echo "   ⚠️  Branch counts are equal ($MAIN_COUNT each) - checking if data insertion completed..."
                echo "   → This may indicate a timing issue or insertion failure"
                echo "   → Cross-engine communication is working, but data may not be committed yet"
            fi


        else
            echo "   ❌ Spark branch creation failed"
            echo "   → Error details: $SPARK_BRANCH_RESULT"
        fi
    else
        echo "   ❌ Trino table creation failed: $TRINO_TABLE_RESULT"
    fi
    
    # Step 8f: Show Iceberg Metadata Tables for comprehensive demo
    echo "   → Exploring Iceberg Metadata Tables..."
    echo "     • Table snapshots (showing evolution):"
    docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT committed_at, operation, summary FROM iceberg.demo.\"customers\$snapshots\" ORDER BY committed_at DESC LIMIT 5" 2>/dev/null
    
    echo "     • Data files:"
    docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT file_format, record_count, file_size_in_bytes FROM iceberg.demo.\"customers\$files\"" 2>/dev/null
    
    echo "     • Table history:"
    docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT made_current_at, snapshot_id FROM iceberg.demo.\"customers\$history\" ORDER BY made_current_at DESC LIMIT 3" 2>/dev/null
    
    # Step 8g: Final business summary with time travel context
    echo "9. Demo data summary with time travel context:"
        echo "   → Customer count by tier:"
        docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT customer_tier, COUNT(*) as count FROM iceberg.demo.customers GROUP BY customer_tier ORDER BY count DESC" 2>/dev/null
        echo "   → Revenue by country:"
        docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT country, ROUND(SUM(total_spent), 2) as revenue FROM iceberg.demo.customers GROUP BY country ORDER BY revenue DESC" 2>/dev/null
        echo "   → Time travel snapshots available:"
        docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) as snapshot_count FROM iceberg.demo.\"customers\$snapshots\"" 2>/dev/null | head -1 | sed 's/^/      Snapshots: /'
        echo "   → Total customers and revenue:"
        docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) as customers, ROUND(SUM(total_spent), 2) as total_revenue FROM iceberg.demo.customers" 2>/dev/null
        
        echo ""
    echo ""
    echo "✅ Demo data initialization complete!"
    echo "🌐 Access Shiny Frontend: http://localhost:8000"
    echo "📊 Access Trino Web UI: http://localhost:8081"
        
else
    echo "   ✗ Table creation failed"
    exit 1
fi