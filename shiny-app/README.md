# Shiny for Python - Trino Frontend

This is a Shiny for Python application that provides an interactive web interface for demonstrating Iceberg time travel and branching capabilities with Trino.

## Features

- **Story-Based Demo Queries**: Pre-built narrative queries showcasing Iceberg capabilities
- **Time Travel Demos**: Interactive queries showing data evolution over time
- **Branching Capabilities**: Demonstrate branch querying and comparison
- **Schema Evolution**: Show how Iceberg handles schema changes gracefully
- **Shared Query Module**: Centralized query library used by both the web app and test scripts
- **Dynamic Snapshot Selection**: Queries automatically adapt to available snapshots
- **Real-time Visualization**: Automatic visualization of query results
- **Connection Monitoring**: Real-time connection status to Trino

## Quick Start

1. **Full rebuild with demo data** (recommended):
   ```bash
   make rebuild-demo
   ```

2. **Or start the entire stack** (if already built):
   ```bash
   make start
   ```

3. **Access the application**:
   - **Shiny Demo App**: http://localhost:8000
   - **Trino Web UI**: http://localhost:8081

4. **Verify everything works**:
   ```bash
   make test-all
   # Or run the comprehensive test script directly:
   ./scripts/test-all.sh
   ```

## Usage

### Demo Stories
The app provides interactive story-based demos organized by category:

#### **Basics**
- **📊 Current Data**: See the current state of customer data
- **🔍 Branches & Tags**: List all available branches and tags

#### **Time Travel**
- **🕐 The Beginning**: Travel back to the very first snapshot
- **📈 Data Evolution**: Watch customer base and revenue grow over time
- **🔄 Schema Evolution**: See how Iceberg handles new columns gracefully
- **👑 Customer Tiers**: Analyze customer tiers from when the feature was added

#### **Branching**
- **🌿 Main Branch**: Query the main branch explicitly
- **⚖️ Branch vs Snapshot**: Compare production data with historical snapshots

#### **Metadata**
- **🗂️ File Organization**: See how Iceberg organizes data files
- **📚 Snapshot Journey**: Journey through every snapshot chronologically

### Key Demo Points
- **5 customer records** across **7 snapshots**
- **Full schema evolution** (customer_tier column added over time)
- **Time travel** with both snapshot IDs and timestamps
- **Branch querying** capabilities
- **All Iceberg metadata tables** accessible

## Shared Query Module

The app uses a **shared query module** (`shared/demo_queries.py`) that provides:

- **Centralized queries**: Same queries used by both web app and test scripts
- **Dynamic snapshot selection**: Queries automatically adapt to available snapshots
- **Rebuild-proof**: No hardcoded snapshot IDs that break after `make clean`
- **Story-based organization**: Queries designed for narrative demo flow

### Architecture Benefits
- ✅ **Single source of truth** for all demo queries
- ✅ **Consistent behavior** between web app and CLI tests
- ✅ **Environment agnostic** - works after any rebuild
- ✅ **Easy maintenance** - update queries in one place

## Configuration

The app connects to Trino using these default settings:
- **Host**: `trino` (Docker service name)
- **Port**: `8080`
- **User**: `admin`
- **Catalog**: `iceberg`
- **Schema**: `demo`
- **Table**: `customers`

## Testing

Run comprehensive tests to verify everything works:

```bash
# Run all tests including shared module verification
./scripts/test-all.sh

# Or use the Makefile
make test-all

# Individual test categories
make test-query        # Basic connectivity
make test-time-travel  # Time travel functionality  
make test-branching    # Branch querying
make test-metadata     # Metadata tables
```

## Troubleshooting

- **Connection Issues**: Run `make status` to check container health
- **Query Errors**: Check `make logs-trino` for Trino errors
- **Rebuild Issues**: Try `make clean && make rebuild-demo` for a fresh start
- **Port Conflicts**: Ensure ports 8000 (Shiny) and 8081 (Trino) are available
- **Shared Module Issues**: Verify with `make test-all` - Step 5 tests module accessibility

### Common Commands
```bash
make clean           # Remove everything (database included!)  
make rebuild-demo    # Full rebuild with demo data
make status          # Check container status
make logs            # View all container logs
make test-all        # Comprehensive functionality test
```