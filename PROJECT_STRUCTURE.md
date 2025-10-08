# Project Structure

## Directory Organization

```
├── src/                          # Source code and pipelines
│   ├── pipelines/               # Data transformation pipelines
│   │   └── ip_sum/             # IP Sum name processing pipeline
│   │       ├── README.md       # Pipeline documentation
│   │       ├── init_ip_sum_data.py         # Table initialization
│   │       ├── ip_sum_iceberg_refactor.py  # Main transformation
│   │       ├── init-ip-sum-data.sh         # Container setup script
│   │       └── run-ip-sum-pipeline.sh      # Pipeline execution script
│   └── demos/                   # Demo and example scripts
│       └── iceberg-transformation-demo.py
├── scripts/                     # Infrastructure and test scripts
│   ├── init-demo-data.sh       # Main demo data initialization
│   ├── dev-restart.sh          # Development utilities
│   ├── test-*.sh               # Test scripts
│   └── test-*.py               # Test utilities
├── shiny-app/                   # Frontend application
├── trino/                       # Trino configuration
├── warehouse/                   # Iceberg data warehouse
└── Makefile                     # Build and run commands
```

## Pipeline Organization

- **`src/pipelines/`** - Production data pipelines
  - Each pipeline gets its own subdirectory
  - Contains all pipeline-specific code and documentation
  - Follows consistent naming patterns

- **`scripts/`** - Infrastructure scripts
  - System setup and configuration
  - Testing utilities
  - Development tools

- **`src/demos/`** - Demo and example code
  - Educational examples
  - Proof-of-concept implementations

## Usage

All pipelines are accessible via Makefile targets:

```bash
# IP Sum Pipeline
make init-ip-sum-data      # Initialize IP Sum pipeline
make run-ip-sum-pipeline   # Run IP Sum transformation

# Infrastructure
make init-data             # Initialize demo data
make start                 # Start all services
make test-all             # Run all tests
```