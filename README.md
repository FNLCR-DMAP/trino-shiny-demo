# Modern Data Lakehouse Stack: Iceberg + Hive + Trino + Spark + Shiny

This project demonstrates a **production-ready modern data lakehouse architecture** featuring cross-engine Apache Iceberg with multiple query and processing engines. Built with Docker Compose for easy deployment and featuring a **Shiny for Python** web interface for end users.

## 🏗️ Software Stack & Architecture

### 📊 **Query & Processing Engines**
- **🔍 Trino (435)** - High-performance distributed SQL query engine
  - *Purpose*: Interactive analytics, ad-hoc queries, cross-engine data access
  - *Use Case*: Business intelligence, data exploration, real-time analytics
  
- **⚡ Apache Spark (3.5.0)** - Unified analytics engine for large-scale data processing
  - *Purpose*: ETL processing, machine learning, batch/stream processing, Iceberg branch management
  - *Use Case*: Data transformations, feature engineering, model training, data pipeline orchestration

### 🗄️ **Data Storage & Metadata**
- **🧊 Apache Iceberg (1.4.2)** - Modern table format with ACID transactions
  - *Purpose*: Schema evolution, time travel, partition management, cross-engine compatibility
  - *Use Case*: Data versioning, snapshot isolation, efficient data updates/deletes
  
- **🏛️ Apache Hive Metastore (4.0.0)** - Centralized metadata repository
  - *Purpose*: Shared schema registry, table definitions, partition information across engines
  - *Use Case*: Cross-engine table discovery, metadata consistency, governance
  
- **🐘 PostgreSQL (15.4)** - Relational database backend
  - *Purpose*: Persistent storage for Hive Metastore metadata
  - *Use Case*: ACID metadata operations, concurrent access, backup/recovery

### 🌐 **User Interface & Storage**
- **✨ Shiny for Python** - Interactive web application framework
  - *Purpose*: User-friendly data exploration interface, query execution, visualization
  - *Use Case*: Self-service analytics, business user data access, dashboard creation
  
- **📁 Local File Storage** - Parquet format data warehouse
  - *Purpose*: Persistent data storage with columnar format optimization
  - *Use Case*: Analytics workloads, compression efficiency, schema evolution support

## 🚀 Building the Stack

### 📋 **Prerequisites**
```bash
# Required software
- Docker & Docker Compose
- Make (for simplified commands)
- 8GB+ RAM recommended
- 10GB+ disk space for warehouse data
```

### 🛠️ **Quick Build & Start**
```bash
# Clone or navigate to the project directory
cd /path/to/trino-iceberg-stack

# Build and start the entire stack
make start

# This command:
# 1. Pulls all Docker images (Trino, Spark, Hive, PostgreSQL, Shiny)
# 2. Creates shared networks and volumes
# 3. Initializes PostgreSQL with Hive schema
# 4. Starts Hive Metastore service
# 5. Launches Trino with Iceberg connector
# 6. Starts Spark cluster (master + worker)
# 7. Deploys Shiny web application
# 8. Creates persistent warehouse directory
```

### 🔧 **Step-by-Step Manual Build**
```bash
# 1. Start infrastructure services (PostgreSQL + Hive)
docker-compose up -d postgres hive-metastore

# 2. Wait for Hive Metastore initialization
sleep 30

# 3. Start query engines
docker-compose up -d trino spark-master spark-worker

# 4. Launch web frontend
docker-compose up -d shiny-app

# 5. Verify all services
make status
```

### 🏗️ **Architecture Flow**
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Shiny App     │    │      Trino       │    │     Spark       │
│  (Port 8000)    │────│   (Port 8081)    │────│  (Port 8082)    │
│  Web Interface  │    │  Query Engine    │    │ Processing Engine│
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────────┐
                    │   Hive Metastore    │
                    │    (Port 9083)      │
                    │  Metadata Service   │
                    └─────────────────────┘
                                 │
                    ┌─────────────────────┐
                    │    PostgreSQL       │
                    │    (Port 5432)      │
                    │  Metadata Storage   │
                    └─────────────────────┘
                                 │
                    ┌─────────────────────┐
                    │   Iceberg Tables    │
                    │     ./warehouse     │
                    │   Parquet Files     │
                    └─────────────────────┘
```

## 🚀 Quick Start Guide

### 🎯 **Makefile Commands (Recommended)**
```bash
# Show all available commands with descriptions
make help

# 🏗️ Build & Start the Complete Stack
make start
# - Pulls Docker images for all services
# - Creates networks and volumes
# - Initializes PostgreSQL + Hive Metastore
# - Starts Trino, Spark, and Shiny services
# - Sets up Iceberg warehouse directory

# ✅ Verify Stack Health
make verify
# - Tests database connectivity
# - Validates service endpoints
# - Checks Iceberg table access
# - Confirms cross-engine functionality

# 🎬 Initialize Demo Data
make init-data
# - Creates sample Iceberg tables
# - Inserts customer and product data
# - Demonstrates schema evolution
# - Sets up cross-engine branching examples

# 📚 Show Interactive Demo Guide
make demo
```

### 🌐 **Access Points**
| Service | URL | Purpose |
|---------|-----|---------|
| **🌟 Shiny Frontend** | http://localhost:8000 | **Main user interface** - Query execution, data visualization |
| **📊 Trino Web UI** | http://localhost:8081 | Query monitoring, cluster status, performance metrics |
| **⚡ Spark Master UI** | http://localhost:8082 | Spark cluster management, job monitoring |
| **🐘 PostgreSQL** | localhost:5432 | Direct database access (user: `hive`, password: `hive`) |

### 🔍 **Service Health Check**
```bash
# Quick status overview
make status

# Individual service logs
make logs-trino    # Trino query engine logs
make logs-spark    # Spark processing logs  
make logs-shiny    # Shiny web app logs
make logs-hive     # Hive Metastore logs
```

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

## 🏭 Production Deployment Guide

### 📦 **Container Images & Versions**
| Component | Version | Image | Purpose |
|-----------|---------|-------|---------|
| **Trino** | 435 | `trinodb/trino:435` | Latest stable with Iceberg 1.4.2 support |
| **Spark** | 3.5.0 | `apache/spark:3.5.0` | Current stable with Scala 2.12 |
| **Hive Metastore** | 4.0.0 | `apache/hive:4.0.0` | Latest stable metastore release |
| **PostgreSQL** | 15.4 | `postgres:15.4` | LTS version for metadata persistence |
| **Python/Shiny** | 3.11 | `python:3.11-slim` | Web interface runtime |

### 🔧 **Cross-Engine Architecture Features**
```yaml
# Key architectural decisions for production readiness:

Persistent JAR Management:
  - Iceberg runtime JARs survive container rebuilds
  - Automatic version alignment across engines
  - Shared JAR volume: ./jars:/opt/shared/jars

Shared Metadata Layer:
  - Single Hive Metastore for all engines
  - ACID metadata operations via PostgreSQL
  - Cross-engine table discovery and governance

Warehouse Consistency:
  - Unified data path: ./warehouse:/data/warehouse
  - Parquet format optimization
  - Iceberg manifest management

Network Architecture:
  - Internal Docker network for service communication
  - External port exposure for user interfaces
  - Service discovery via container names
```

### 🚀 **Scaling for Production**

#### **Infrastructure Scaling**
```bash
# Scale Spark workers
docker-compose up -d --scale spark-worker=3

# Add Trino worker nodes (requires cluster configuration)
# Update trino/etc/config.properties:
# coordinator=false
# discovery.uri=http://trino-coordinator:8080

# Scale Shiny app instances (with load balancer)
docker-compose up -d --scale shiny-app=2
```

#### **Performance Optimization**
```yaml
# trino/etc/config.properties
query.max-memory=50GB
query.max-memory-per-node=8GB
query.max-total-memory-per-node=10GB

# Spark configuration (spark-defaults.conf)
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.serializer=org.apache.spark.serializer.KryoSerializer
```

### 🔐 **Production Security Checklist**
- [ ] **Authentication**: Configure LDAP/OAuth for Trino and Spark
- [ ] **Authorization**: Implement role-based access control (RBAC)
- [ ] **Network Security**: Use TLS/SSL for all inter-service communication
- [ ] **Data Encryption**: Enable encryption at rest and in transit
- [ ] **Monitoring**: Deploy Prometheus + Grafana for observability
- [ ] **Backup**: Automated PostgreSQL backups and Iceberg snapshots
- [ ] **Secrets Management**: Use Docker secrets or external vault

### ☁️ **Cloud Deployment Options**

#### **AWS Deployment**
```bash
# Replace local storage with S3
# Update iceberg.properties:
iceberg.catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
s3.endpoint=https://s3.amazonaws.com
s3.path-style-access=false

# Use RDS for PostgreSQL
# Use ECS/EKS for container orchestration
# Use ALB for load balancing
```

#### **Kubernetes Deployment**
```yaml
# Example helm values for production K8s
trino:
  replicas: 3
  resources:
    requests:
      memory: "8Gi"
      cpu: "2"
    limits:
      memory: "16Gi" 
      cpu: "4"

spark:
  master:
    replicas: 1
  worker:
    replicas: 5
    resources:
      requests:
        memory: "4Gi" 
        cpu: "2"
```

### 📊 **Monitoring & Observability**
```bash
# Add monitoring stack to docker-compose.yml
services:
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
  
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin

# Trino metrics endpoint: http://localhost:8081/v1/info
# Spark metrics endpoint: http://localhost:8082/metrics/json
```

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

**Enable file-based authentication for Trino**

This guide explains how to secure a Trino deployment using:

HTTPS (TLS encryption)
Password-based authentication

**Directory structure:**
```bash
/etc/trino/
├── config.properties
├── password-authenticator.properties
├── password.db
└── tls/trino-security
    └── trino.jks
```


**1. Enable Password Authentication**: 
In **etc/config.properties**, set 
```bash
http-server.authentication.type=PASSWORD
http-server.authentication.allow-insecure-over-http=false
http-server.https.enabled=true
http-server.https.port=8443
http-server.https.keystore.path=/etc/trino/tls/trino.jks
http-server.https.keystore.key=<your-keystore-password>
```

TLS and a configured shared secret are required for password authentication.

**2. Create password-authenticator.properties**
In **etc/password-authenticator.properties**, define:
```bash
password-authenticator.name=file
file.password-file=/etc/trino/password.db
```

**3. Create the Password File (password.db)**
Use bcrypt hashes for secure authentication. You can generate entries using htpasswd:
```bash
touch password.db
htpasswd -B -C 10 password.db admin
```

**4. Generate a Self-Signed Certificate**

you may use:

*Self-signed certificate (for testing/internal clusters)*
*Enterprise CA*
*AWS ACM Private CA*
*Kubernetes secret + cert-manager*

Self-signed Certificate (simple option) as certs being used for **localhost**
```bash
keytool -genkeypair \
  -alias trino \
  -keyalg RSA \
  -keysize 2048 \
  -validity 365 \
  -keystore /etc/trino/tls/trino.jks \
  -storepass <your-keystore-password> \
  -keypass <your-keystore-password> \
  -dname "CN=localhost, OU=Trino, O=YourOrg, L=YourCity, ST=YourState, C=US"
```

Replace <your-keystore-password> with the same password used in config.properties.

This creates a .jks file with a self-signed certificate for localhost

**5. Verify the Keystore**
To inspect the contents:
```bash
keytool -list -v -keystore /etc/trino/tls/trino.jks -storepass <your-keystore-password>
```

**6. Restart Trino**
Restart the Trino server to apply changes

✅ Verifying Authentication
Web UI
Visit: https://localhost.com:8443
Enter your username and password
On success, your username appears in the top-right corner



