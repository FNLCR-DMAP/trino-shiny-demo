# Iceberg + Hive + Trino + Spark Cross-Engine Demo Stack with Shiny Frontend

This project demonstrates a production-ready setup for **cross-engine Apache Iceberg** with Hive Metastore, Trino query engine, and Apache Spark using Docker Compose, plus a **Shiny for Python** web interface for end users. Features advanced Iceberg capabilities including **cross-engine branching** where Spark creates branches and Trino queries them.

## 🏗️ Architecture

- **Trino (435)**: Query engine for interactive analytics and cross-engine querying
- **Apache Spark (3.5.0)**: Processing engine for branch creation and data manipulation
- **Apache Hive (4.0.0)**: Metastore for shared metadata management across engines
- **PostgreSQL (15.4)**: Backend database for Hive Metastore
- **Apache Iceberg (1.4.2)**: Table format with ACID transactions and cross-engine compatibility
- **Shiny for Python**: Web frontend for querying data
- **Local File Storage**: Parquet files stored in `./warehouse` directory with persistence

## 🚀 Quick Start

### 🎯 New Makefile-Based Commands
```bash
# Show all available commands
make help

# Start the full stack with Shiny frontend  
make start

# Verify everything is working
make verify

# Show demo instructions
make demo
```

**Access Points:**
- 🌐 **Shiny Frontend**: http://localhost:8000 (User-friendly web interface)
- 📊 **Trino Web UI**: http://localhost:8081 (Admin interface)
- ⚡ **Spark UI**: http://localhost:8082 (Spark Master Web UI)

### ⚡ Common Commands
```bash
make start        # Start everything
make stop         # Stop all containers
make shiny        # Restart only Shiny app
make status       # Check container status
make logs-shiny   # View Shiny app logs
make clean        # Clean up everything

# Demo & Testing
make init-data    # Initialize demo data with cross-engine branching
make test-all     # Run comprehensive Iceberg feature tests
make test-branching     # Test cross-engine branching specifically
make test-time-travel   # Test time travel functionality
make test-metadata      # Test metadata table access
```

## 📊 What the Demo Does

### Core Iceberg Features
1. **Creates Iceberg tables** with customer data using Trino
2. **Inserts sample records** (5 customers from different countries)
3. **Demonstrates schema evolution** (adding customer_tier column)
4. **Shows time travel queries** across historical snapshots
5. **Displays metadata tables** (snapshots, files, history, refs)

### 🌟 Cross-Engine Branching (Advanced Feature)
6. **Spark creates Iceberg branches** for development/testing
7. **Trino queries Spark-created branches** seamlessly 
8. **Demonstrates cross-engine compatibility** via shared Hive Metastore
9. **Shows branch metadata** and data isolation between branches

### Analytics & Visualization  
10. **Runs analytical queries** showing:
    - Total customers and revenue by country
    - Customer tier segmentation
    - Revenue evolution across time
11. **Generates Parquet files** in the local warehouse directory

## 🌐 Shiny Frontend Features

The included Shiny for Python web application provides an intuitive interface for end users:

### **Query Interface**
- **Pre-built queries**: Show catalogs, schemas, tables, and sample data
- **Custom SQL**: Execute any Trino/SQL query
- **Real-time results**: Immediate feedback on query execution

### **Data Visualization**
- **Automatic charts**: Scatter plots, histograms, and bar charts
- **Interactive plots**: Built with Plotly for rich interactivity
- **Smart detection**: Chooses appropriate visualization based on data types

### **Monitoring**
- **Connection status**: Real-time Trino connection monitoring
- **Query feedback**: Clear success/error messages
- **Performance info**: Row and column counts

### **Example Workflows**
1. **Explore data structure**: Start with "Show Catalogs" → "Show Schemas" → "Show Tables"
2. **Sample data**: Use "Sample Data" to preview table contents
3. **Custom analysis**: Switch to "Custom Query" for specific business questions
4. **Visualize results**: Automatic charts help identify patterns

## 🔍 Manual Exploration

### Connect to Trino CLI
```bash
docker exec -it trino-cli /usr/bin/trino --server trino:8080 --catalog iceberg --schema demo
```

### Example Queries
```sql
-- Show all tables
SHOW TABLES;

-- Query customer data
SELECT * FROM customers;

-- Show table snapshots (Iceberg feature)
SELECT * FROM "customers$snapshots";

-- Show table files (see Parquet files)
SELECT file_path, record_count, file_size_in_bytes FROM "customers$files";
```

## 📁 File Structure

```
./
├── docker-compose.yml          # Main orchestration with Spark + Trino
├── hive-site.xml              # Shared Hive Metastore configuration
├── Makefile                   # Comprehensive commands and testing
├── trino/
│   ├── etc/                   # Trino server configuration
│   │   ├── config.properties
│   │   ├── node.properties
│   │   └── log.properties
│   └── catalog/               # Catalog configurations
│       └── iceberg.properties # Iceberg connector config
├── jars/                      # Persistent Iceberg JAR storage
│   └── iceberg-spark-runtime-3.5_2.12-1.4.2.jar
├── warehouse/                 # Data files (Parquet) with cross-engine access
├── scripts/                   # Demo and comprehensive testing scripts
│   ├── init-demo-data.sh     # Cross-engine demo initialization
│   ├── test-branching.sh     # Branching functionality tests  
│   ├── test-time-travel.sh   # Time travel feature tests
│   └── test-*.sh             # Additional feature test scripts
├── shiny-app/                 # Shiny for Python web interface
│   ├── app.py                # Main Shiny application
│   └── shared/               # Shared query modules
└── archive/                   # Legacy demo scripts
```

## 🌐 Access URLs

- **Shiny Frontend**: http://localhost:8000 (Main user interface)
- **Trino Web UI**: http://localhost:8081 (Query engine admin)
- **Spark Master UI**: http://localhost:8082 (Spark cluster status)
- **PostgreSQL**: localhost:5432 (user: `hive`, password: `hive`)

## 🛠️ Advanced Usage

### Time Travel Queries (Iceberg Feature)
```sql
-- Query data as of a specific timestamp
SELECT * FROM customers FOR TIMESTAMP AS OF TIMESTAMP '2023-12-01 10:00:00';

-- Query data from a specific snapshot
SELECT * FROM customers FOR VERSION AS OF 123456789;

-- View all available snapshots
SELECT * FROM "customers$snapshots" ORDER BY committed_at DESC;
```

### Cross-Engine Branching (Advanced Iceberg Feature)
```sql
-- Trino: Query main branch
SELECT COUNT(*) FROM iceberg.branching_demo.products;

-- Trino: Query Spark-created dev branch  
SELECT COUNT(*) FROM iceberg.branching_demo.products FOR VERSION AS OF 'dev';

-- View available branches
SELECT name, type FROM "products$refs" WHERE type = 'BRANCH';
```

**Note**: Branch creation requires Spark. Use the demo scripts or:
```bash
# Spark SQL: Create a branch (from Spark container)
docker exec spark-iceberg /opt/spark/bin/spark-sql \
  --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
  -e "ALTER TABLE iceberg.demo.products CREATE BRANCH dev;"
```

### Schema Evolution (Iceberg Feature)
```sql
-- Add a new column
ALTER TABLE customers ADD COLUMN phone VARCHAR(20);

-- Update data
UPDATE customers SET phone = '+1-555-0123' WHERE id = 1;
```

### Partitioning
```sql
-- Create a partitioned table
CREATE TABLE sales (
    id BIGINT,
    customer_id BIGINT,
    amount DECIMAL(10,2),
    sale_date DATE
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['month(sale_date)']
);
```

## 🔧 Production Considerations

This setup uses production-ready container images:

- **Trino 435**: Latest stable release with Iceberg connector
- **Apache Spark 3.5.0**: Current stable with Iceberg 1.4.2 runtime
- **Apache Hive 4.0.0**: Current stable metastore release
- **PostgreSQL 15.4**: LTS version for metadata storage

### Cross-Engine Setup Features
- **Persistent JAR Management**: Iceberg runtime JARs survive container rebuilds
- **Shared Metadata**: Single Hive Metastore enables cross-engine table access
- **Automatic JAR Mounting**: Spark containers auto-configure Iceberg runtime
- **Warehouse Alignment**: Consistent `/data/warehouse` path across engines

### For Production Deployment
1. Use external PostgreSQL with proper backup/recovery
2. Configure proper authentication and authorization  
3. Set up monitoring and logging for both Trino and Spark
4. Use distributed storage (S3, HDFS, etc.) instead of local files
5. Scale Trino workers and Spark executors based on workload
6. Implement proper network security between engine components
7. Use external Iceberg JAR management (Maven repositories)

## 🛑 Cleanup

```bash
# Clean stop with Makefile (recommended)
make clean

# Manual cleanup
docker-compose down -v

# Remove warehouse data (optional - preserves demo data for restart)
rm -rf warehouse/

# Note: JAR files in ./jars/ are preserved for persistence
```

## 🧪 Testing & Validation

The project includes comprehensive test suites:

```bash
# Test everything
make test-all

# Individual feature tests  
make test-branching     # Cross-engine branching
make test-time-travel   # Historical queries
make test-metadata      # Iceberg metadata tables
make test-query         # Basic connectivity
```

**Expected Results:**
- ✅ Cross-engine branching: Spark creates branches, Trino queries them
- ✅ Time travel: Query historical snapshots and timestamps  
- ✅ Schema evolution: Add columns and query across versions
- ✅ Metadata access: Explore internal Iceberg metadata
- ✅ JAR persistence: Automatic setup survives rebuilds

## 📚 Learn More

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Trino Documentation](https://trino.io/docs/)
- [Apache Hive Documentation](https://hive.apache.org/)