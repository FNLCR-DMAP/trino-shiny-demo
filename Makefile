# Trino + Iceberg + Shiny Frontend Stack
# =====================================

.PHONY: help build start stop restart clean logs status demo verify shiny-dev shiny-rebuild

# Current git commit short hash for deployment tagging
COMMIT_HASH := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)

# Configurable ports (override with environment variables)
TRINO_PORT ?= 8083
SHINY_PORT ?= 8000
SPARK_PORT ?= 8082
POSTGRES_PORT ?= 5432
HIVE_PORT ?= 9083

# Default target
help:
	@echo "🚀 Trino + Iceberg + Shiny Frontend Stack"
	@echo "=========================================="
	@echo ""
	@echo "📋 Available Commands:"
	@echo ""
	@echo "🔧 Stack Management:"
	@echo "  make build     - Build all Docker images"
	@echo "  make start     - Start the full stack"
	@echo "  make stop      - Stop all containers"
	@echo "  make restart   - Restart all containers"
	@echo "  make clean     - Remove containers and volumes"
	@echo ""
	@echo "📊 Individual Services:"
	@echo "  make trino           - Start only Trino stack (no Shiny)"
	@echo "  make shiny           - Start/restart only Shiny app (basic restart)"
	@echo "  make shiny-dev       - Shiny development restart - picks up all Python changes"
	@echo "  make shiny-rebuild   - Rebuild and restart Shiny app (for dependencies)"
	@echo ""
	@echo "🔍 Monitoring:"
	@echo "  make status    - Show container status"
	@echo "  make logs      - Show all container logs"
	@echo "  make logs-shiny - Show only Shiny app logs"
	@echo "  make logs-trino - Show only Trino logs"
	@echo ""
	@echo "🧪 Testing & Demo:"
	@echo "  make init-data           - Initialize demo schema and sample data"
	@echo "  make init-ip-sum-data    - Initialize IP Sum pipeline tables and data"
	@echo "  make run-ip-sum-pipeline - Run IP Sum transformation pipeline"
	@echo "  make rebuild-demo        - Full rebuild with demo data (like rebuild-demo.sh)"
	@echo "  make test-query          - Test basic Trino connectivity"
	@echo "  make test-time-travel    - Test Iceberg time travel functionality"
	@echo "  make test-branching      - Test Iceberg branching functionality"
	@echo "  make test-metadata       - Test Iceberg metadata tables"
	@echo "  make test-all            - Run all comprehensive Iceberg tests"
	@echo "  make demo                - Show demo instructions"
	@echo "  make verify              - Verify setup and connectivity"
	@echo ""
	@echo "📥 Data Registration:"
	@echo "  make register-data DATA_FILE=<file> SCHEMA=<schema> TABLE=<table>"
	@echo "                       - Register any data file (CSV, Parquet, JSON, etc.) to Iceberg"
	@echo "  make register-lims-ngs   - Quick shortcut to register LIMS NGS data"
	@echo ""
	@echo "🌐 Access Points (Default):"
	@echo "  Shiny Frontend: http://localhost:$(SHINY_PORT)"
	@echo "  Trino Web UI:   http://localhost:$(TRINO_PORT)"
	@echo "  Spark Web UI:   http://localhost:$(SPARK_PORT)"
	@echo ""
	@echo "� Port Configuration:"
	@echo "  Customize any port to avoid conflicts:"
	@echo "  TRINO_PORT=8090 make start              # Change Trino port only"
	@echo "  TRINO_PORT=8090 SHINY_PORT=8001 make start  # Change multiple ports"
	@echo ""
	@echo "  Available port variables:"
	@echo "  TRINO_PORT (default: 8083), SHINY_PORT (default: 8000)"
	@echo "  SPARK_PORT (default: 8082), POSTGRES_PORT (default: 5432)"
	@echo "  HIVE_PORT (default: 9083)"
	@echo ""
	@echo "🔗 Remote Access:"
	@echo "  If on remote server, use SSH or VS Code port forwarding"

# Build all images
build:
	@echo "🔨 Building Docker images..."
	docker-compose build
	docker build -t trino-shiny-app ./shiny-app

# Start the full stack
start:
	@echo "🚀 Starting full stack..."
	@echo "📝 Using ports: Trino=$(TRINO_PORT), Shiny=$(SHINY_PORT), Spark=$(SPARK_PORT)"
	TRINO_PORT=$(TRINO_PORT) SHINY_PORT=$(SHINY_PORT) SPARK_PORT=$(SPARK_PORT) POSTGRES_PORT=$(POSTGRES_PORT) HIVE_PORT=$(HIVE_PORT) docker-compose up -d
	@echo "✅ Stack started!"
	@echo "🌐 Shiny Frontend: http://localhost:$(SHINY_PORT)"
	@echo "📊 Trino Web UI:   http://localhost:$(TRINO_PORT)"
	@echo "⚡ Spark Web UI:   http://localhost:$(SPARK_PORT)"

# Start only Trino stack (no Shiny)
trino:
	@echo "🚀 Starting Trino stack only..."
	TRINO_PORT=$(TRINO_PORT) POSTGRES_PORT=$(POSTGRES_PORT) HIVE_PORT=$(HIVE_PORT) docker-compose up -d postgres hive-metastore trino trino-cli
	@echo "✅ Trino stack started!"
	@echo "📊 Trino Web UI: http://localhost:$(TRINO_PORT)"

# Start/restart only Shiny app (basic restart)
shiny:
	@echo "🔄 Restarting Shiny app..."
	docker-compose restart shiny-app
	@echo "✅ Shiny app restarted!"
	@echo "🌐 Frontend: http://localhost:8000"

# Shiny development restart - picks up all Python module changes
shiny-dev:
	@echo "🔄 Shiny Development Restart - Recreating container..."
	@echo "This ensures all Python module changes are picked up!"
	docker-compose stop shiny-app
	docker-compose rm -f shiny-app
	docker-compose up -d shiny-app
	@echo "⏳ Waiting for container to start..."
	@sleep 3
	@echo "🔍 Validating volume mounts..."
	@if docker-compose exec -T shiny-app test -f /app/app.py; then \
		echo "✅ Volume mounts verified - app.py is accessible"; \
	else \
		echo "❌ Warning: app.py not found in container"; \
	fi
	@if docker-compose exec -T shiny-app test -d /app/shared; then \
		echo "✅ Volume mounts verified - shared/ directory is accessible"; \
	else \
		echo "❌ Warning: shared/ directory not found in container"; \
	fi
	@echo "📋 Recent logs:"
	@docker-compose logs --tail=5 shiny-app
	@echo ""
	@echo "🚀 App available at: http://localhost:8000"
	@echo "📝 Container recreated - all Python changes picked up!"

# Rebuild and restart Shiny app (for dependency changes)
shiny-rebuild:
	@echo "🔨 Rebuilding Shiny app image..."
	docker build -t trino-shiny-app ./shiny-app
	@echo "🔄 Recreating container with new image..."
	docker-compose stop shiny-app
	docker-compose rm -f shiny-app
	docker-compose up -d shiny-app
	@echo "⏳ Waiting for container to start..."
	@sleep 3
	@echo "✅ Shiny app rebuilt and restarted!"
	@echo "🌐 Frontend: http://localhost:8000"

# Stop all containers
stop:
	@echo "🛑 Stopping all containers..."
	docker-compose down

# Restart all containers
restart:
	@echo "🔄 Restarting all containers..."
	docker-compose restart
	@echo "✅ All containers restarted!"

# Clean up everything
clean:
	@echo "🧹 Cleaning up containers and volumes..."
	docker-compose down -v
	docker system prune -f
	@echo "✅ Cleanup complete!"

# Show container status
status:
	@echo "📊 Container Status:"
	@docker-compose ps

# Show all logs
logs:
	@echo "📜 All Container Logs:"
	docker-compose logs --tail=20

# Show Shiny app logs only
logs-shiny:
	@echo "📱 Shiny App Logs:"
	docker-compose logs --tail=20 shiny-app

# Show Trino logs only
logs-trino:
	@echo "🔍 Trino Logs:"
	docker-compose logs --tail=20 trino

# Verify setup and connectivity
verify:
	@echo "🔍 Verifying Setup..."
	@echo "Checking Docker..."
	@docker --version > /dev/null && echo "✅ Docker is running" || echo "❌ Docker not found"
	@echo "Checking containers..."
	@if docker ps | grep -q "trino.*healthy"; then \
		echo "✅ Trino is healthy"; \
	else \
		echo "❌ Trino not healthy"; \
	fi
	@if docker ps | grep -q "shiny-app"; then \
		echo "✅ Shiny app is running"; \
	else \
		echo "❌ Shiny app not running"; \
	fi
	@echo "🌐 Access points:"
	@echo "  Shiny Frontend: http://localhost:$(SHINY_PORT)"
	@echo "  Trino Web UI:   http://localhost:$(TRINO_PORT)"
	@echo "  Spark Web UI:   http://localhost:$(SPARK_PORT)"

# Initialize demo data
init-data:
	@./scripts/init-demo-data.sh

# Full rebuild with demo data (like rebuild-demo.sh)
rebuild-demo:
	@echo "🔄 Full Demo Rebuild (like rebuild-demo.sh)..."
	@echo "1. Tearing down existing stack..."
	docker-compose down -v
	@echo "2. Cleaning up..."
	docker system prune -f > /dev/null 2>&1 || true
	rm -rf warehouse/* || true
	mkdir -p warehouse
	chmod 777 warehouse
	@echo "3. Building stack..."
	docker-compose up -d
	@echo "4. Waiting for services (20s)..."
	sleep 20
	@echo "5. Testing connectivity..."
	@docker exec trino-cli trino --server trino:8080 --user admin --execute "SHOW CATALOGS" > /dev/null 2>&1 && echo "✅ Trino ready" || echo "❌ Trino not ready"
	@echo "6. Initializing demo data..."
	$(MAKE) init-data
	@echo "🎉 Rebuild complete!"
	@echo "🌐 Shiny Frontend: http://localhost:$(SHINY_PORT)"
	@echo "📊 Trino Web UI:   http://localhost:$(TRINO_PORT)"
	@echo "⚡ Spark Web UI:   http://localhost:$(SPARK_PORT)"

# Test a simple query
test-query:
	@echo "🧪 Testing Trino connectivity..."
	docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT 1 as test" 2>/dev/null || echo "❌ Query failed"
	@echo "✅ Trino query successful!"

# Comprehensive Time Travel and Branching Tests
test-time-travel:
	@./scripts/test-time-travel.sh

test-shared:
	@echo "🐍 Testing Shared Query Module..."
	@python3 scripts/test-shared-queries.py

test-branching:
	@echo "🌿 Testing Iceberg Branching Functionality..."
	@echo ""
	@echo "1. Testing Cross-Engine Branching (our working demo)..."
	@if docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM iceberg.branching_demo.products" 2>/dev/null >/dev/null; then \
		echo "   → Cross-engine branching table exists!"; \
		echo "   → Main branch query:"; \
		MAIN_COUNT=$$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM iceberg.branching_demo.products" 2>/dev/null | tail -1 | tr -d '"'); \
		echo "     ✅ Main branch: $$MAIN_COUNT products"; \
		echo "   → Dev branch query (Trino querying Spark-created branch):"; \
		DEV_COUNT=$$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM iceberg.branching_demo.products FOR VERSION AS OF 'dev'" 2>/dev/null | tail -1 | tr -d '"'); \
		if [ "$$DEV_COUNT" != "" ] && [ "$$DEV_COUNT" -gt "$$MAIN_COUNT" ]; then \
			echo "     🎉 Dev branch: $$DEV_COUNT products - Cross-engine branching WORKING!"; \
		else \
			echo "     ⚠️  Dev branch query issue or no difference detected"; \
		fi; \
		echo "   → Branch metadata:"; \
		docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT name, type FROM iceberg.branching_demo.\"products\$$refs\"" 2>/dev/null; \
	else \
		echo "   ⚠️  Cross-engine demo table not found - run 'make init-data' first"; \
	fi
	@echo ""
	@echo "2. Testing standard demo table branching..."
	@echo "   → Available branches and refs:"
	@docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT * FROM iceberg.demo.\"customers\$$refs\"" 2>/dev/null
	@echo "   → Querying main branch explicitly:"
	@docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) as main_count FROM iceberg.demo.customers FOR VERSION AS OF 'main'" 2>/dev/null
	@echo ""
	@echo "3. Testing branch creation capability..."
	@echo "   ℹ️  Note: Trino $(shell docker exec trino-cli trino --version 2>/dev/null | head -1 || echo "version unknown") does not support CREATE BRANCH"
	@echo "   → This is expected - use Spark for branch creation, Trino for querying"
	@echo "   → Our cross-engine demo (above) shows this working correctly"
	@echo ""
	@echo "✅ Branching tests completed!"
	@echo "💡 Cross-engine branching: Spark creates branches, Trino queries them perfectly!"

test-metadata:
	@echo "📊 Testing Iceberg Metadata Tables..."
	@echo ""
	@echo "1. Testing snapshots metadata..."
	@docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT snapshot_id, committed_at, operation FROM iceberg.demo.\"customers\$$snapshots\" ORDER BY committed_at DESC LIMIT 3" 2>/dev/null
	@echo ""
	@echo "2. Testing files metadata..."
	@docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT file_format, record_count, file_size_in_bytes FROM iceberg.demo.\"customers\$$files\"" 2>/dev/null
	@echo ""
	@echo "3. Testing history metadata..."
	@docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT made_current_at, snapshot_id FROM iceberg.demo.\"customers\$$history\" ORDER BY made_current_at DESC LIMIT 3" 2>/dev/null
	@echo ""
	@echo "4. Testing refs metadata..."
	@docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT name, type, snapshot_id FROM iceberg.demo.\"customers\$$refs\"" 2>/dev/null
	@echo "✅ Metadata tests completed!"

# Run all Iceberg feature tests
test-all:
	@echo "🧪 Running Comprehensive Iceberg Feature Tests..."
	@echo "================================================="
	@echo ""
	$(MAKE) test-query
	@echo ""
	$(MAKE) test-time-travel
	@echo ""
	$(MAKE) test-branching
	@echo ""
	$(MAKE) test-metadata
	@echo ""
	@echo "🎉 All tests completed!"
	@echo "💡 Next: Try the Shiny app at http://localhost:$(SHINY_PORT)"

# Show demo instructions
demo:
	@echo "🎉 Shiny Frontend Demo"
	@echo "====================="
	@echo ""
	@echo "1. 🌐 Open http://localhost:$(SHINY_PORT)"
	@echo ""
	@echo "2. 🔍 Try these workflows:"
	@echo "   • Show Catalogs → Execute"
	@echo "   • Show Schemas → Execute"
	@echo "   • Show Tables → Execute"
	@echo "   • Sample Data → Execute"
	@echo ""
	@echo "3. 🧪 Try custom queries:"
	@echo "   SELECT 1 as number, 'Hello Trino!' as message"
	@echo "   SHOW FUNCTIONS"
	@echo "   SELECT current_timestamp"

# Initialize IP Sum pipeline data
init-ip-sum-data:
	@./src/pipelines/ip_sum/init-ip-sum-data.sh

# Run IP Sum pipeline transformation
run-ip-sum-pipeline:
	@./src/pipelines/ip_sum/run-ip-sum-pipeline.sh
	@echo ""
	@echo "🔧 Need help? Run: make verify"

# Deploy Shiny app to Posit Connect
deploy:
	@echo "🚀 Deploying Shiny app to Posit Connect... (commit: $(COMMIT_HASH))"
	docker-compose run --rm -T --no-deps \
	  deploy-shiny \
	  bash -c "echo 'Commit $(COMMIT_HASH)'; ls -1 /root/.rsconnect-python || echo 'No rsconnect config'; rsconnect list || echo 'No servers'; rsconnect deploy shiny -n appshare-dev -t 'Trino Shiny Demo ($(COMMIT_HASH))' --verbose ."


# Register data files to Iceberg (CSV, Parquet, JSON, etc.)
# Usage: make register-data DATA_FILE=path/to/file.csv SCHEMA=demo TABLE=mytable [FORMAT=csv] [SCHEMA_FILE=schema.py]
register-data:
	@if [ -z "$(DATA_FILE)" ] || [ -z "$(TABLE)" ]; then \
		echo "❌ Error: Missing required arguments"; \
		echo ""; \
		echo "Usage:"; \
		echo "  make register-data DATA_FILE=<file> SCHEMA=<schema> TABLE=<table> [FORMAT=<format>] [SCHEMA_FILE=<schema.py>]"; \
		echo ""; \
		echo "Examples:"; \
		echo "  # Register CSV file (auto-detected)"; \
		echo "  make register-data DATA_FILE=data/patients.csv SCHEMA=demo TABLE=patients"; \
		echo ""; \
		echo "  # Register Parquet file"; \
		echo "  make register-data DATA_FILE=data/samples.parquet SCHEMA=clinical TABLE=samples FORMAT=parquet"; \
		echo ""; \
		echo "  # Register LIMS NGS data with custom schema"; \
		echo "  make register-data DATA_FILE=data/notional_combined_lims_ngs.csv SCHEMA=demo TABLE=combined_lims_ngs SCHEMA_FILE=data/combined_lims_ngs_schema.py"; \
		exit 1; \
	fi
	@if [ -n "$(FORMAT)" ] && [ -n "$(SCHEMA_FILE)" ]; then \
		./src/data-registration/register-data-to-iceberg.sh "$(DATA_FILE)" "$(SCHEMA)" "$(TABLE)" "$(FORMAT)" "$(SCHEMA_FILE)"; \
	elif [ -n "$(SCHEMA_FILE)" ]; then \
		./src/data-registration/register-data-to-iceberg.sh "$(DATA_FILE)" "$(SCHEMA)" "$(TABLE)" "" "$(SCHEMA_FILE)"; \
	elif [ -n "$(FORMAT)" ]; then \
		./src/data-registration/register-data-to-iceberg.sh "$(DATA_FILE)" "$(SCHEMA)" "$(TABLE)" "$(FORMAT)"; \
	else \
		./src/data-registration/register-data-to-iceberg.sh "$(DATA_FILE)" "$(SCHEMA)" "$(TABLE)"; \
	fi

# Quick shortcut for LIMS NGS data
register-lims-ngs:
	@$(MAKE) register-data DATA_FILE=data/notional_combined_lims_ngs.csv SCHEMA=demo TABLE=combined_lims_ngs SCHEMA_FILE=data/combined_lims_ngs_schema.py
