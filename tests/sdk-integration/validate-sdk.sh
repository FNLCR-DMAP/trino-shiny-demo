#!/bin/bash
# Validation script for DMAP Data SDK
# This script validates that the SDK is working correctly end-to-end

set -e  # Exit on error

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}         DMAP Data SDK - Validation Report${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo ""

# Step 1: Check Docker containers
echo -e "${YELLOW}[1/10]${NC} Checking Docker stack health..."
CONTAINERS=$(docker ps --format "{{.Names}}" | grep -E "postgres|hive|trino|spark" | wc -l)
if [ "$CONTAINERS" -ge 4 ]; then
    echo -e "   ${GREEN}✓${NC} Docker stack running ($CONTAINERS containers)"
else
    echo -e "   ${RED}✗${NC} Docker stack incomplete (only $CONTAINERS containers)"
    exit 1
fi

# Step 2: Check SDK installation
echo -e "${YELLOW}[2/10]${NC} Verifying SDK installation..."
SDK_VERSION=$(docker exec spark-iceberg pip3 list 2>/dev/null | grep dmap-data-sdk | awk '{print $2}')
if [ -n "$SDK_VERSION" ]; then
    echo -e "   ${GREEN}✓${NC} SDK installed (version: $SDK_VERSION)"
else
    echo -e "   ${RED}✗${NC} SDK not installed"
    exit 1
fi

# Step 3: Test SDK imports
echo -e "${YELLOW}[3/10]${NC} Testing SDK imports..."
IMPORT_TEST=$(docker exec spark-iceberg python3 -c "from dmap_data_sdk import DataPipeline, Ref, PlatformFactory; print('OK')" 2>&1)
if echo "$IMPORT_TEST" | grep -q "OK"; then
    echo -e "   ${GREEN}✓${NC} All SDK imports successful"
else
    echo -e "   ${RED}✗${NC} Import failed: $IMPORT_TEST"
    exit 1
fi

# Step 4: Check input data
echo -e "${YELLOW}[4/10]${NC} Verifying input data..."
INPUT_COUNT=$(docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.data_pipeline.full_name_input" 2>&1 | grep -v WARNING | grep -oP '"\d+"' | tr -d '"')
if [ "$INPUT_COUNT" -ge 10 ]; then
    echo -e "   ${GREEN}✓${NC} Input data exists ($INPUT_COUNT records)"
else
    echo -e "   ${RED}✗${NC} Input data missing or incomplete"
    exit 1
fi

# Step 5: Check output table exists
echo -e "${YELLOW}[5/10]${NC} Checking output table..."
OUTPUT_COUNT=$(docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.data_pipeline.ip_sum_output_sdk_test" 2>&1 | grep -v WARNING | grep -oP '"\d+"' | tr -d '"' || echo "0")
if [ "$OUTPUT_COUNT" -ge 10 ]; then
    echo -e "   ${GREEN}✓${NC} Output table exists with $OUTPUT_COUNT records"
else
    echo -e "   ${YELLOW}⚠${NC} Output table empty or missing (will be created by pipeline)"
fi

# Step 6: Verify table schema
echo -e "${YELLOW}[6/10]${NC} Verifying output table schema..."
SCHEMA_CHECK=$(docker exec trino trino --execute "DESCRIBE iceberg.data_pipeline.ip_sum_output_sdk_test" 2>&1 | grep -c "original_full_name" || echo "0")
if [ "$SCHEMA_CHECK" -ge 1 ]; then
    echo -e "   ${GREEN}✓${NC} Table schema correct"
else
    echo -e "   ${YELLOW}⚠${NC} Table schema not verified (table may not exist yet)"
fi

# Step 7: Check snapshot metadata
echo -e "${YELLOW}[7/10]${NC} Checking Iceberg snapshot metadata..."
SNAPSHOT_COUNT=$(docker exec trino trino --execute 'SELECT COUNT(*) FROM iceberg.data_pipeline."ip_sum_output_sdk_test\$snapshots"' 2>&1 | grep -v WARNING | grep -oP '"\d+"' | tr -d '"' || echo "0")
if [ "$SNAPSHOT_COUNT" -ge 1 ]; then
    echo -e "   ${GREEN}✓${NC} Snapshots exist ($SNAPSHOT_COUNT snapshots)"
else
    echo -e "   ${YELLOW}⚠${NC} No snapshots yet (will be created on first write)"
fi

# Step 8: Check audit schema
echo -e "${YELLOW}[8/10]${NC} Verifying lineage infrastructure..."
AUDIT_SCHEMA=$(docker exec trino trino --execute "SHOW SCHEMAS IN iceberg" 2>&1 | grep -c "audit" || echo "0")
if [ "$AUDIT_SCHEMA" -ge 1 ]; then
    echo -e "   ${GREEN}✓${NC} Audit schema exists"
else
    echo -e "   ${YELLOW}⚠${NC} Audit schema missing (will be created by SDK)"
fi

# Step 9: Check lineage table
echo -e "${YELLOW}[9/10]${NC} Checking lineage table..."
LINEAGE_COUNT=$(docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.audit.etl_lineage WHERE target_table = 'iceberg.data_pipeline.ip_sum_output_sdk_test'" 2>&1 | grep -v WARNING | grep -oP '"\d+"' | tr -d '"' || echo "0")
if [ "$LINEAGE_COUNT" -ge 1 ]; then
    echo -e "   ${GREEN}✓${NC} Lineage records exist ($LINEAGE_COUNT records)"
    
    # Show latest lineage
    echo -e "   ${BLUE}Latest lineage:${NC}"
    docker exec trino trino --execute "SELECT transform, target_snapshot_id, recorded_at FROM iceberg.audit.etl_lineage WHERE target_table = 'iceberg.data_pipeline.ip_sum_output_sdk_test' ORDER BY recorded_at DESC LIMIT 1" 2>&1 | grep -v WARNING | sed 's/^/      /'
else
    echo -e "   ${YELLOW}⚠${NC} No lineage records yet"
fi

# Step 10: Run full pipeline test
echo -e "${YELLOW}[10/10]${NC} Running end-to-end pipeline test..."
TEST_OUTPUT=$(./scripts/test-sdk-in-docker.sh 2>&1)
if echo "$TEST_OUTPUT" | grep -q "✅ SDK Test Completed Successfully"; then
    echo -e "   ${GREEN}✓${NC} Pipeline test passed"
    
    # Extract and display key metrics
    RECORDS_WRITTEN=$(echo "$TEST_OUTPUT" | grep -oP "Wrote \K\d+" | tail -1)
    SNAPSHOT_ID=$(echo "$TEST_OUTPUT" | grep -oP "Snapshot ID: \K\d+" | tail -1)
    
    if [ -n "$RECORDS_WRITTEN" ]; then
        echo -e "   ${BLUE}Records written:${NC} $RECORDS_WRITTEN"
    fi
    if [ -n "$SNAPSHOT_ID" ]; then
        echo -e "   ${BLUE}Snapshot ID:${NC} $SNAPSHOT_ID"
    fi
else
    echo -e "   ${RED}✗${NC} Pipeline test failed"
    echo "$TEST_OUTPUT" | tail -20
    exit 1
fi

echo ""
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ All validation checks passed!${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo ""
echo "Summary:"
echo "  • SDK Version: $SDK_VERSION"
echo "  • Input Records: $INPUT_COUNT"
echo "  • Output Records: $OUTPUT_COUNT"
echo "  • Lineage Records: $LINEAGE_COUNT"
echo "  • Snapshots: $SNAPSHOT_COUNT"
echo ""
echo "Next steps:"
echo "  1. View data: docker exec trino trino --execute 'SELECT * FROM iceberg.data_pipeline.ip_sum_output_sdk_test LIMIT 5'"
echo "  2. Check lineage: docker exec trino trino --execute 'SELECT * FROM iceberg.audit.etl_lineage'"
echo "  3. View snapshots: docker exec trino trino --execute 'SELECT * FROM iceberg.data_pipeline.\"ip_sum_output_sdk_test\$snapshots\"'"
echo ""
