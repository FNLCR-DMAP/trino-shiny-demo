# Iceberg + Hive + Trino Demo Stack

This project demonstrates a production-ready setup for Apache Iceberg with Hive Metastore and Trino query engine using Docker Compose. All Parquet files are stored locally on the file system.

## 🏗️ Architecture

- **Trino (435)**: Query engine for interactive analytics
- **Apache Hive (4.0.0)**: Metastore for metadata management
- **PostgreSQL (15.4)**: Backend database for Hive Metastore
- **Apache Iceberg**: Table format for data lakes with ACID transactions
- **Local File Storage**: Parquet files stored in `./warehouse` directory

## 🚀 Quick Start

### 1. Start the Stack
```bash
chmod +x start-stack.sh
./start-stack.sh
```

### 2. Run the Demo
```bash
chmod +x run-demo.sh
./run-demo.sh
```

### 3. Explore Generated Files
```bash
chmod +x explore-files.sh
./explore-files.sh
```

## 📊 What the Demo Does

1. **Creates an Iceberg table** with customer data
2. **Inserts sample records** (5 customers from different countries)
3. **Runs analytical queries** to show:
   - Total customers and revenue
   - Customers grouped by country
   - Table metadata and file information
4. **Shows Parquet files** generated in the warehouse directory

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
├── docker-compose.yml          # Main orchestration file
├── hive-site.xml              # Hive Metastore configuration
├── init-db.sql                # PostgreSQL initialization
├── trino/
│   ├── etc/                   # Trino server configuration
│   │   ├── config.properties
│   │   ├── node.properties
│   │   └── log.properties
│   └── catalog/               # Catalog configurations
│       ├── iceberg.properties # Iceberg connector
│       └── hive.properties    # Hive connector
├── warehouse/                 # Data files (Parquet) stored here
└── scripts/                   # Demo and utility scripts
```

## 🌐 Access URLs

- **Trino Web UI**: http://localhost:8081
- **PostgreSQL**: localhost:5432 (user: `hive`, password: `hive`)

## 🛠️ Advanced Usage

### Time Travel Queries (Iceberg Feature)
```sql
-- Query data as of a specific timestamp
SELECT * FROM customers FOR TIMESTAMP AS OF TIMESTAMP '2023-12-01 10:00:00';

-- Query data from a specific snapshot
SELECT * FROM customers FOR VERSION AS OF 123456789;
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

- **Trino 435**: Latest stable release
- **Apache Hive 4.0.0**: Current stable release
- **PostgreSQL 15.4**: LTS version

For production deployment:
1. Use external PostgreSQL with proper backup/recovery
2. Configure proper authentication and authorization
3. Set up monitoring and logging
4. Use distributed storage (S3, HDFS, etc.)
5. Scale Trino workers based on workload

## 🛑 Cleanup

```bash
# Stop all services
docker-compose down

# Remove all data (including PostgreSQL data and warehouse files)
docker-compose down -v
sudo rm -rf warehouse/
```

## 📚 Learn More

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Trino Documentation](https://trino.io/docs/)
- [Apache Hive Documentation](https://hive.apache.org/)