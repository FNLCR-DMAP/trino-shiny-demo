#!/usr/bin/env python3
"""
Test runner for Iceberg demo queries using the shared query module.
This ensures consistency between test scripts and the Shiny app.
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


def test_time_travel():
    """Test time travel functionality using shared queries"""
    print("2. Testing time travel functionality...")
    queries = IcebergDemoQueries()
    
    # Test 1: Get snapshots
    print("   → Getting available snapshots...")
    query_sql, description = queries.get_snapshots()
    success, output = run_trino_query(query_sql)
    
    if not success:
        print(f"   ❌ Failed to get snapshots: {output}")
        return False
    
    # Extract snapshot IDs (simple parsing)
    lines = output.split('\n')[1:]  # Skip header
    snapshot_count = len([line for line in lines if line.strip() and not line.startswith('-')])
    
    if snapshot_count > 0:
        print(f"   ✅ Found {snapshot_count} snapshots")
    else:
        print("   ❌ No snapshots found")
        return False
    
    # Test 2: Test story queries
    test_stories = [
        ("current_data", queries.story_current_data),
        ("first_snapshot", queries.story_first_snapshot),
        ("data_evolution", queries.story_data_evolution),
        ("schema_evolution", queries.story_schema_evolution)
    ]
    
    # Test 3: Test new time travel demo methods
    time_travel_demo_methods = [
        ("snapshot_times", queries.get_snapshot_times),
        ("initial_data", queries.get_initial_data),
        ("updated_data_comparison", queries.get_updated_data_comparison)
    ]
    
    for story_name, story_method in test_stories:
        print(f"   → Testing story: {story_name}")
        query_sql, description = story_method()
        success, output = run_trino_query(query_sql)
        
        if success:
            print(f"     ✅ {story_name} query successful")
        else:
            print(f"     ❌ {story_name} query failed: {output}")
    
    # Test the new time travel demo methods specifically
    print("   → Testing new time travel demo methods...")
    for demo_name, demo_method in time_travel_demo_methods:
        print(f"     → Testing {demo_name}...")
        query_sql, description = demo_method()
        success, output = run_trino_query(query_sql)
        
        if success:
            print(f"       ✅ {demo_name} query successful")
            # Show a sample of the output for verification
            lines = output.split('\n')
            if len(lines) > 1:
                print(f"       📊 Sample result: {lines[1][:50]}...")
        else:
            print(f"       ❌ {demo_name} query failed: {output}")
    
    return True


def test_branching():
    """Test branching functionality using shared queries"""
    print("3. Testing branching functionality...")
    queries = IcebergDemoQueries()
    
    # Test 1: List branches and refs
    print("   → Testing branch listing...")
    query_sql, description = queries.get_branches_and_refs()
    success, output = run_trino_query(query_sql)
    
    if success:
        print("   ✅ Branch listing successful")
        print(f"     Available refs: {len(output.split(chr(10))) - 1}")
    else:
        print(f"   ❌ Branch listing failed: {output}")
        return False
    
    # Test 2: Main branch query
    print("   → Testing main branch query...")
    query_sql, description = queries.story_main_branch()
    success, output = run_trino_query(query_sql)
    
    if success:
        print("   ✅ Main branch query successful")
    else:
        print(f"   ❌ Main branch query failed: {output}")
    
    # Test 3: Branch comparison
    print("   → Testing branch comparison...")
    query_sql, description = queries.story_branch_comparison()
    success, output = run_trino_query(query_sql)
    
    if success:
        print("   ✅ Branch comparison successful")
    else:
        print(f"   ❌ Branch comparison failed: {output}")
    
    return True


def test_metadata():
    """Test metadata exploration using shared queries"""
    print("4. Testing metadata functionality...")
    queries = IcebergDemoQueries()
    
    test_queries = [
        ("file_metadata", queries.story_file_metadata),
        ("snapshot_history", queries.story_snapshot_history)
    ]
    
    for test_name, query_method in test_queries:
        print(f"   → Testing {test_name}...")
        query_sql, description = query_method()
        success, output = run_trino_query(query_sql)
        
        if success:
            print(f"   ✅ {test_name} successful")
        else:
            print(f"   ❌ {test_name} failed: {output}")
    
    return True


def main():
    """Main test runner"""
    test_type = sys.argv[1] if len(sys.argv) > 1 else "all"
    
    print("🧪 Testing Iceberg Demo Queries (Using Shared Module)")
    print("=" * 55)
    print()
    
    if test_type in ["all", "connectivity"]:
        if not test_connectivity():
            sys.exit(1)
        print()
    
    if test_type in ["all", "time-travel"]:
        test_time_travel()
        print()
    
    if test_type in ["all", "branching"]:
        test_branching()
        print()
    
    if test_type in ["all", "metadata"]:
        test_metadata()
        print()
    
    print("🎉 All tests completed!")


if __name__ == "__main__":
    main()