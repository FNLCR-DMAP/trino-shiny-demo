#!/usr/bin/env python3
"""
Test runner for Shiny app methods using the shared query module.
Focuses only on methods actually used in the Shiny application.
"""

import sys
import os
import subprocess
import json

# Add the shared module to the path (using the one in shiny-app)
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
shared_dir = os.path.join(parent_dir, 'shiny-app', 'shared')
sys.path.insert(0, shared_dir)

from demo_queries import IcebergDemoQueries


def run_trino_query(query_sql, description=""):
    """Run a query via Trino CLI and return the result"""
    try:
        cmd = [
            'docker', 'exec', 'trino-cli', 'trino',
            '--server', 'trino:8080', 
            '--user', 'admin',
            '--execute', query_sql
        ]
        
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            timeout=30
        )
        
        if result.returncode == 0:
            return True, result.stdout.strip()
        else:
            return False, result.stderr.strip()
            
    except subprocess.TimeoutExpired:
        return False, "Query timeout"
    except Exception as e:
        return False, str(e)


def test_connectivity():
    """Test basic Trino connectivity"""
    print("1. Testing Trino connectivity...")
    queries = IcebergDemoQueries()
    query_sql, description = queries.connectivity_test()
    
    success, output = run_trino_query(query_sql)
    if success and "1" in output:
        print("   ✅ Trino connection successful")
        return True
    else:
        print(f"   ❌ Trino connection failed: {output}")
        return False


def test_app_methods():
    """Test methods actually used in the Shiny app"""
    print("2. Testing Shiny app methods...")
    queries = IcebergDemoQueries()
    
    # Test methods used in app.py
    app_methods = [
        ("warehouse_info", queries.warehouse_info),
        ("time_travel_overview", queries.get_time_travel_overview),
        ("customer_data_at_timestamp", lambda: queries.get_customer_data_at_timestamp("2025-10-07 17:24:16.681"))
    ]
    
    for method_name, method_func in app_methods:
        print(f"   → Testing {method_name}...")
        query_sql, description = method_func()
        success, output = run_trino_query(query_sql)
        
        if success:
            print(f"   ✅ {method_name} query successful")
            # Show a sample of the output for verification
            lines = output.split('\n')
            if len(lines) > 1:
                print(f"     📊 Sample result: {lines[1][:50]}...")
        else:
            print(f"   ❌ {method_name} query failed: {output}")
    
    return True


def test_branching_methods():
    """Test new branching functionality - matches init-demo-data.sh"""
    print("3. Testing branching methods...")
    queries = IcebergDemoQueries()
    
    # Test the new branching methods
    branching_methods = [
        ("main_branch_count", queries.get_main_branch_count),
        ("dev_branch_count", queries.get_dev_branch_count), 
        ("main_vs_dev_comparison", queries.compare_main_vs_dev_branches)
    ]
    
    for method_name, method_func in branching_methods:
        print(f"   → Testing {method_name}...")
        query_sql, description = method_func()
        success, output = run_trino_query(query_sql)
        
        if success:
            print(f"   ✅ {method_name} query successful")
            # Show the actual count/result for verification
            lines = output.split('\n')
            if len(lines) > 1:
                result_line = lines[1].strip().strip('"')
                if method_name.endswith('_count'):
                    print(f"     📊 Product count: {result_line}")
                else:
                    print(f"     📊 Sample result: {result_line[:50]}...")
        else:
            print(f"   ❌ {method_name} query failed: {output}")
    
    return True


def main():
    """Main test runner - Shiny app methods + new branching features"""
    print("🧪 Testing Shiny App & Branching Methods (Shared Module)")
    print("=" * 55)
    print()
    
    # Test basic connectivity
    if not test_connectivity():
        sys.exit(1)
    print()
    
    # Test methods used in the Shiny app
    test_app_methods()
    print()
    
    # Test new branching functionality
    test_branching_methods()
    print()
    
    print("🎉 All tests completed!")


if __name__ == "__main__":
    main()