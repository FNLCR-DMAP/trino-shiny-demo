"""
DMAP Data SDK
=============

A unified data engineering SDK for Apache Iceberg, Delta Lake, and Snowflake
with built-in lineage tracking, time travel, and branch management support.

Main Components
---------------
- **DataPipeline**: High-level API for data engineers (recommended)
- **DataPlatform**: Low-level platform abstractions (IcebergSparkPlatform, DeltaSparkPlatform, SnowflakePlatform)
- **Ref**: Table version reference (branch, tag, snapshot, timestamp)
- **RunContext**: Execution context (Airflow run_id, DAG, task, code metadata)
- **LineageSink**: Lineage persistence (IcebergLineageSink, NoopLineageSink)

Quick Start
-----------
```python
from dmap_data_sdk import DataPipeline, Ref

# Configuration-driven pipeline (recommended)
pipeline = DataPipeline.from_config(transform_name="my_transform")

# Read data with automatic lineage tracking
df = pipeline.read_table("iceberg.raw.orders")

# Your business logic
result = df.groupBy("customer_id").sum("amount")

# Write with automatic lineage and metadata stamping
pipeline.write_table(result, "iceberg.analytics.customer_totals")
```

Advanced Usage
--------------
```python
from dmap_data_sdk import DataPlatform, PlatformFactory, Ref, RunContext, millis
from pyspark.sql import SparkSession

# Low-level platform API
spark = SparkSession.builder.getOrCreate()
platform = PlatformFactory.iceberg_spark(spark)

# Time travel and branch support
orders = platform.read_table("orders", Ref(branch="main"))
orders_yesterday = platform.read_table("orders", Ref(as_of_ts_millis=millis() - 86400000))

# Custom context
ctx = RunContext(
    run_id="manual-123",
    code_branch="feature-x",
    default_input_ref=Ref(branch="main"),
    target_ref=Ref(branch="feature-x")
)
```

For full documentation, see: https://github.com/FNLCR-DMAP/dmap-data-sdk
"""

__version__ = "1.0.0"

# High-level API (recommended for data engineers)
from .data_utils import (
    DataPipeline,
    Ref,
    RunContext,
)

# Low-level platform abstractions
from .data_utils import (
    DataPlatform,
    PlatformFactory,
    IcebergSparkPlatform,
    DeltaSparkPlatform,
    SnowflakePlatform,
    BranchingNotSupported,
)

# Lineage components
from .data_utils import (
    LineageSink,
    IcebergLineageSink,
    NoopLineageSink,
    LineageRecord,
    InputLineage,
)

# Results and metadata
from .data_utils import (
    WriteResult,
)

# Utilities
from .data_utils import (
    millis,
    ctx_from_airflow,
)

__all__ = [
    # Version
    "__version__",
    
    # High-level API
    "DataPipeline",
    "Ref",
    "RunContext",
    
    # Low-level platforms
    "DataPlatform",
    "PlatformFactory",
    "IcebergSparkPlatform",
    "DeltaSparkPlatform",
    "SnowflakePlatform",
    "BranchingNotSupported",
    
    # Lineage
    "LineageSink",
    "IcebergLineageSink",
    "NoopLineageSink",
    "LineageRecord",
    "InputLineage",
    
    # Results
    "WriteResult",
    
    # Utilities
    "millis",
    "ctx_from_airflow",
]
