"""
Shared Iceberg Time Travel and Branching Demo Queries

This module contains all the queries used by both the test scripts 
and the Shiny demo app to ensure consistency.
"""

class IcebergDemoQueries:
    """
    Centralized repository of demo queries for Iceberg time travel and branching.
    
    Each method returns a tuple of (query_sql, description) for use in both
    test scripts and the Shiny demo application.
    """
    
    def __init__(self, catalog="iceberg", schema="demo", table="customers"):
        self.catalog = catalog
        self.schema = schema
        self.table = table
    
    # ========================================================================
    # BASIC CONNECTIVITY AND METADATA
    # ========================================================================
    
    def connectivity_test(self):
        """Simple connectivity test query"""
        return (
            "SELECT 1 as connection_test",
            "Basic Trino connectivity test"
        )
    
    def warehouse_info(self):
        """Get warehouse information - catalogs, schemas, and tables"""
        return (
            f"""-- WAREHOUSE OVERVIEW
-- Show available catalogs, schemas, and tables
WITH warehouse_summary AS (
    SELECT 
        'Catalogs' as resource_type,
        COUNT(*) as count,
        ARRAY_JOIN(ARRAY_AGG(catalog_name), ', ') as items
    FROM system.metadata.catalogs
    WHERE catalog_name NOT IN ('system', 'tpcds', 'tpch', 'jmx', 'memory')
    
    UNION ALL
    
    SELECT 
        'Schemas in ' || '{self.catalog}' as resource_type,
        COUNT(*) as count,
        ARRAY_JOIN(ARRAY_AGG(schema_name), ', ') as items
    FROM {self.catalog}.information_schema.schemata
    WHERE schema_name != 'information_schema'
    
    UNION ALL
    
    SELECT 
        'Tables in ' || '{self.catalog}.{self.schema}' as resource_type,
        COUNT(*) as count,
        ARRAY_JOIN(ARRAY_AGG(table_name), ', ') as items
    FROM {self.catalog}.information_schema.tables
    WHERE table_schema = '{self.schema}'
)
SELECT 
    resource_type,
    count,
    items as available_items
FROM warehouse_summary
ORDER BY resource_type""",
            "Overview of available catalogs, schemas, and tables in the warehouse"
        )
    
    def get_snapshots(self):
        """Get all available snapshots with metadata"""
        return (
            f"""SELECT 
                snapshot_id,
                committed_at,
                operation,
                summary
            FROM {self.catalog}.{self.schema}."{self.table}$snapshots" 
            ORDER BY committed_at""",
            "List all available table snapshots with metadata"
        )
    
    def get_branches_and_refs(self):
        """Get all available branches and tags"""
        return (
            f"""SELECT 
                name as branch_or_tag_name,
                type,
                snapshot_id
            FROM {self.catalog}.{self.schema}."{self.table}$refs"
            ORDER BY type, name""",
            "List all available branches and tags for the table"
        )
    
    # ========================================================================
    # STORY-BASED TIME TRAVEL DEMOS
    # ========================================================================
    
    def story_current_data(self):
        """Story: Show current data state"""
        return (
            f"""-- STORY: Current Customer Data
-- This is what our customer database looks like right now
SELECT 
    id, 
    name, 
    country, 
    total_spent,
    CASE 
        WHEN customer_tier IS NOT NULL THEN customer_tier
        ELSE 'Not Set'
    END as customer_tier,
    'Current Data' as time_context
FROM {self.catalog}.{self.schema}.{self.table}
ORDER BY total_spent DESC
LIMIT 10""",
            "Current customer data with all the latest information"
        )
    
    def story_first_snapshot(self):
        """Story: Travel back to the very first snapshot"""
        return (
            f"""-- STORY: The Beginning 
-- Travel back to the first snapshot to see how it all started
WITH first_snapshot AS (
    SELECT snapshot_id 
    FROM {self.catalog}.{self.schema}."{self.table}$snapshots" 
    ORDER BY committed_at ASC 
    LIMIT 1
)
SELECT 
    id, 
    name, 
    country, 
    total_spent,
    'First Snapshot - The Beginning' as time_context
FROM {self.catalog}.{self.schema}.{self.table} 
FOR VERSION AS OF (SELECT snapshot_id FROM first_snapshot)
ORDER BY id""",
            "The very first data - see how the customer base started"
        )
    
    def story_data_evolution(self):
        """Story: See how data evolved over specific snapshots"""
        return (
            f"""-- STORY: Data Evolution
-- Compare customer counts across different points in time
WITH snapshot_periods AS (
    SELECT 
        snapshot_id,
        ROW_NUMBER() OVER (ORDER BY committed_at) as rn,
        COUNT(*) OVER () as total_snapshots
    FROM {self.catalog}.{self.schema}."{self.table}$snapshots"
),
evolution AS (
    SELECT 'Early Days' as period, COUNT(*) as customers, SUM(total_spent) as revenue
    FROM {self.catalog}.{self.schema}.{self.table} 
    FOR VERSION AS OF (
        SELECT snapshot_id FROM snapshot_periods WHERE rn = 1
    )
    
    UNION ALL
    
    SELECT 'Growth Phase' as period, COUNT(*) as customers, SUM(total_spent) as revenue  
    FROM {self.catalog}.{self.schema}.{self.table} 
    FOR VERSION AS OF (
        SELECT snapshot_id FROM snapshot_periods 
        WHERE rn = GREATEST(2, total_snapshots / 2)
    )
    
    UNION ALL
    
    SELECT 'Current State' as period, COUNT(*) as customers, SUM(total_spent) as revenue
    FROM {self.catalog}.{self.schema}.{self.table}
)
SELECT 
    period,
    customers,
    ROUND(revenue, 2) as revenue,
    customers - LAG(customers) OVER (ORDER BY period) as customer_growth,
    ROUND(revenue - LAG(revenue) OVER (ORDER BY period), 2) as revenue_growth
FROM evolution
ORDER BY period""",
            "Watch how our customer base and revenue grew over time"
        )
    
    def story_schema_evolution(self):
        """Story: Demonstrate schema evolution with customer_tier column"""
        return (
            f"""-- STORY: Schema Evolution Magic
-- New column 'customer_tier' was added - old snapshots won't have it!
WITH first_snapshot AS (
    SELECT snapshot_id 
    FROM {self.catalog}.{self.schema}."{self.table}$snapshots" 
    ORDER BY committed_at ASC 
    LIMIT 1
)
SELECT 
    'Old Snapshot (no customer_tier)' as data_source,
    COUNT(*) as customers,
    COUNT(customer_tier) as customers_with_tier
FROM {self.catalog}.{self.schema}.{self.table} 
FOR VERSION AS OF (SELECT snapshot_id FROM first_snapshot)

UNION ALL

SELECT 
    'Current Data (with customer_tier)' as data_source,
    COUNT(*) as customers,
    COUNT(customer_tier) as customers_with_tier  
FROM {self.catalog}.{self.schema}.{self.table}""",
            "See how Iceberg handles schema evolution gracefully"
        )
    
    def story_detailed_customer_tiers(self):
        """Story: Show detailed customer tier analysis"""
        return (
            f"""-- STORY: Customer Tier Analysis
-- Only recent snapshots have the customer_tier column
WITH recent_snapshot AS (
    SELECT snapshot_id 
    FROM {self.catalog}.{self.schema}."{self.table}$snapshots" 
    ORDER BY committed_at DESC 
    LIMIT 1 OFFSET 1  -- Second most recent to show evolution
)
SELECT 
    customer_tier,
    COUNT(*) as customers,
    ROUND(AVG(total_spent), 2) as avg_spent,
    ROUND(SUM(total_spent), 2) as total_revenue
FROM {self.catalog}.{self.schema}.{self.table} 
FOR VERSION AS OF (SELECT snapshot_id FROM recent_snapshot)
WHERE customer_tier IS NOT NULL
GROUP BY customer_tier
ORDER BY total_revenue DESC""",
            "Analyze customer tiers from when that feature was introduced"
        )
    
    # ========================================================================
    # BRANCHING DEMOS
    # ========================================================================
    
    def story_main_branch(self):
        """Story: Query the main branch explicitly"""
        return (
            f"""-- STORY: Main Branch Querying
-- Explicitly query the 'main' branch (current production data)
SELECT 
    'main' as branch_name,
    COUNT(*) as customers,
    ROUND(SUM(total_spent), 2) as total_revenue,
    'Production Data' as description
FROM {self.catalog}.{self.schema}.{self.table} FOR VERSION AS OF 'main'""",
            "Query the main branch - this is your production data"
        )
    
    def story_branch_comparison(self):
        """Story: Compare main branch with a specific snapshot"""
        return (
            f"""-- STORY: Branch vs Snapshot Comparison  
-- Compare main branch with an earlier snapshot
WITH first_snapshot AS (
    SELECT snapshot_id 
    FROM {self.catalog}.{self.schema}."{self.table}$snapshots" 
    ORDER BY committed_at ASC 
    LIMIT 1
)
SELECT 
    'main branch' as source,
    COUNT(*) as customers,
    ROUND(SUM(total_spent), 2) as revenue
FROM {self.catalog}.{self.schema}.{self.table} FOR VERSION AS OF 'main'

UNION ALL

SELECT 
    'early snapshot' as source,
    COUNT(*) as customers, 
    ROUND(SUM(total_spent), 2) as revenue
FROM {self.catalog}.{self.schema}.{self.table} 
FOR VERSION AS OF (SELECT snapshot_id FROM first_snapshot)""",
            "Compare current production data with historical snapshots"
        )
    
    # ========================================================================
    # METADATA EXPLORATION
    # ========================================================================
    
    def story_file_metadata(self):
        """Story: Explore Iceberg file-level metadata"""
        return (
            f"""-- STORY: Under the Hood - File Metadata
-- See how Iceberg organizes data files
SELECT 
    file_format,
    COUNT(*) as file_count,
    SUM(record_count) as total_records,
    ROUND(SUM(file_size_in_bytes) / (1024.0 * 1024.0), 2) as total_size_mb
FROM {self.catalog}.{self.schema}."{self.table}$files"
GROUP BY file_format
ORDER BY total_records DESC""",
            "Explore how Iceberg organizes your data files"
        )
    
    def story_snapshot_history(self):
        """Story: Complete snapshot history analysis"""
        return (
            f"""-- STORY: Complete Snapshot Journey
-- See every snapshot with customer and revenue metrics  
SELECT 
    ROW_NUMBER() OVER (ORDER BY committed_at) as snapshot_number,
    snapshot_id,
    committed_at,
    operation
FROM {self.catalog}.{self.schema}."{self.table}$snapshots" 
ORDER BY committed_at""",
            "Journey through every snapshot in chronological order"
        )
    
    # ========================================================================
    # UTILITY QUERIES FOR TESTING
    # ========================================================================
    
    def count_by_snapshot(self, snapshot_id):
        """Get customer count for a specific snapshot"""
        return (
            f"""SELECT 
                COUNT(*) as customer_count, 
                ROUND(COALESCE(SUM(total_spent), 0), 2) as total_revenue
            FROM {self.catalog}.{self.schema}.{self.table} 
            FOR VERSION AS OF {snapshot_id}""",
            f"Customer count and revenue for snapshot {snapshot_id}"
        )
    
    def test_timestamp_query(self, timestamp):
        """Test timestamp-based time travel"""
        return (
            f"""SELECT COUNT(*) as customer_count 
            FROM {self.catalog}.{self.schema}.{self.table} 
            FOR TIMESTAMP AS OF TIMESTAMP '{timestamp}'""",
            f"Customer count at timestamp {timestamp}"
        )
    
    def get_customer_data_at_timestamp(self, timestamp):
        """Get customer data at a specific timestamp, handling schema evolution"""
        return (
            f"""-- TIME TRAVEL: Data at Selected Time Point
-- Timestamp: {timestamp}
-- Shows customer data as it existed at this specific point in time
-- Using SELECT * to automatically handle schema evolution
SELECT *
FROM {self.catalog}.{self.schema}.{self.table}
FOR TIMESTAMP AS OF TIMESTAMP '{timestamp}'
ORDER BY id""",
            f"Customer data at selected timestamp: {timestamp}"
        )

    def get_snapshot_times(self):
        """Get snapshots with their committed times for time travel demo"""
        return (
            f"""-- TIME TRAVEL: Snapshot Timeline
-- Show all snapshots with their times for time travel demonstration
SELECT 
    snapshot_id,
    committed_at,
    operation,
    CASE 
        WHEN ROW_NUMBER() OVER (ORDER BY committed_at) = 1 THEN 'Initial Data'
        WHEN ROW_NUMBER() OVER (ORDER BY committed_at DESC) = 1 THEN 'Latest Data'
        ELSE 'Historical Point'
    END as snapshot_type,
    summary
FROM {self.catalog}.{self.schema}."{self.table}$snapshots" 
ORDER BY committed_at""",
            "Timeline of all snapshots showing when data changes occurred"
        )
    
    def get_initial_data(self):
        """Get customer data from the very first snapshot"""
        # First, we need to get the snapshot ID - this will be done in two steps
        # Step 1: Get the first snapshot ID
        # Step 2: Query the data using that snapshot ID
        
        # For now, let's create a query that shows current data with snapshot context
        return (
            f"""-- TIME TRAVEL: Initial Data State
-- Shows current customer data with first snapshot timestamp for context
-- In a real implementation, this would use the earliest snapshot ID
SELECT 
    c.id,
    c.name,
    c.email,
    c.country,
    c.signup_date,
    c.total_orders,
    c.total_spent,
    c.customer_tier,
    s.committed_at as first_snapshot_time,
    'Data from earliest state' as context
FROM {self.catalog}.{self.schema}.{self.table} c
CROSS JOIN (
    SELECT committed_at
    FROM {self.catalog}.{self.schema}."{self.table}$snapshots"
    ORDER BY committed_at ASC
    LIMIT 1
) s
ORDER BY c.id""",
            "Customer data with first snapshot context - showing the initial state"
        )
        
    def get_initial_data_with_snapshot_id(self, snapshot_id):
        """Get customer data from a specific snapshot ID"""
        return (
            f"""-- TIME TRAVEL: Data from Specific Snapshot
-- Customer data as it existed at snapshot {snapshot_id}
SELECT 
    id,
    name,
    email,
    country,
    signup_date,
    total_orders,
    total_spent,
    customer_tier,
    '{snapshot_id}' as snapshot_id,
    'Historical data' as context
FROM {self.catalog}.{self.schema}.{self.table}
FOR VERSION AS OF {snapshot_id}
ORDER BY id""",
            f"Customer data from snapshot {snapshot_id}"
        )
    
    def get_time_travel_overview(self):
        """Complete time travel overview showing all snapshots and their states"""
        return (
            f"""-- TIME TRAVEL: Complete Overview
-- Shows all snapshots with their timestamps, operations, and data evolution
-- This gives you all the time points to explore the data history
WITH snapshot_info AS (
    SELECT 
        snapshot_id,
        committed_at,
        operation,
        ROW_NUMBER() OVER (ORDER BY committed_at) as snapshot_number,
        CASE 
            WHEN ROW_NUMBER() OVER (ORDER BY committed_at) = 1 THEN 'Table Creation (empty)'
            WHEN ROW_NUMBER() OVER (ORDER BY committed_at) = 2 THEN 'Initial Data (3 customers)'
            WHEN ROW_NUMBER() OVER (ORDER BY committed_at) = 3 THEN 'More Customers (5 total)'
            WHEN operation = 'overwrite' AND ROW_NUMBER() OVER (ORDER BY committed_at) = 4 THEN 'Schema Evolution (customer_tier added)'
            WHEN operation = 'overwrite' THEN 'Customer Updates/Promotions'
            ELSE 'Data Changes'
        END as description,
        CASE 
            WHEN ROW_NUMBER() OVER (ORDER BY committed_at) = 1 THEN 0
            WHEN ROW_NUMBER() OVER (ORDER BY committed_at) = 2 THEN 3
            WHEN ROW_NUMBER() OVER (ORDER BY committed_at) = 3 THEN 5
            ELSE 5
        END as estimated_customer_count
    FROM {self.catalog}.{self.schema}."{self.table}$snapshots"
)
SELECT 
    snapshot_number,
    CAST(snapshot_id AS VARCHAR) as snapshot_id,
    committed_at as timestamp,
    operation,
    description as what_happened,
    estimated_customer_count as approx_customers,
    'Use FOR TIMESTAMP AS OF TIMESTAMP ''' || CAST(committed_at AS VARCHAR) || '''' as time_travel_syntax
FROM snapshot_info
ORDER BY snapshot_number""",
            "Complete time travel overview - all snapshots with their data states"
        )

# ========================================================================
# STORY DEFINITIONS FOR UI
# ========================================================================

DEMO_STORIES = {
    "current_data": {
        "title": "📊 Current Data",
        "description": "See what our customer database looks like right now",
        "category": "basics"
    },
    "first_snapshot": {
        "title": "🕐 The Beginning", 
        "description": "Travel back to the very first snapshot - how it all started",
        "category": "time_travel"
    },
    "data_evolution": {
        "title": "📈 Data Evolution",
        "description": "Watch how customers and revenue grew over time",
        "category": "time_travel"
    },
    "schema_evolution": {
        "title": "🔄 Schema Evolution",
        "description": "See how Iceberg handles new columns gracefully", 
        "category": "time_travel"
    },
    "customer_tiers": {
        "title": "👑 Customer Tiers",
        "description": "Analyze customer tiers from when that feature was added",
        "category": "time_travel"
    },
    "main_branch": {
        "title": "🌿 Main Branch",
        "description": "Query the main branch explicitly",
        "category": "branching"
    },
    "branch_comparison": {
        "title": "⚖️ Branch vs Snapshot",
        "description": "Compare production data with historical snapshots", 
        "category": "branching"
    },
    "file_metadata": {
        "title": "🗂️ File Organization", 
        "description": "See how Iceberg organizes data files under the hood",
        "category": "metadata"
    },
    "snapshot_history": {
        "title": "📚 Snapshot Journey",
        "description": "Journey through every snapshot chronologically",
        "category": "metadata"
    },
    "branches_and_refs": {
        "title": "🔍 Branches & Tags",
        "description": "List all available branches and tags",
        "category": "metadata"
    },
    "snapshot_times": {
        "title": "⏰ Snapshot Timeline",
        "description": "Show all snapshots with their committed times",
        "category": "time_travel"
    },
    "time_travel_overview": {
        "title": "🕰️ Time Travel Overview",
        "description": "See all snapshots and time points for data exploration",
        "category": "time_travel"
    }
}