#!/bin/bash
# Interactive script to analyze PostgreSQL spikes
# Extracts data from both pg00 and pg01, then automatically runs analysis

set -euo pipefail

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "================================================"
echo "  PostgreSQL Spike Analysis - Interactive Mode"
echo "================================================"
echo ""

echo "This script will extract data from BOTH pg00 (leader) and pg01 (replica)"
echo "and generate a comparison report."
echo ""

echo "================================================"
echo "  Spike Period (problematic period)"
echo "================================================"
echo ""
echo "Format: YYYY-MM-DDTHH:MM (e.g., 2025-10-27T16:00)"
echo ""
read -p "Enter spike START time: " SPIKE_START
read -p "Enter spike END time: " SPIKE_END

echo ""
echo "================================================"
echo "  Normal Period (baseline reference)"
echo "================================================"
echo ""
echo "Format: YYYY-MM-DDTHH:MM (e.g., 2025-10-27T03:00)"
echo ""
read -p "Enter normal START time: " NORMAL_START
read -p "Enter normal END time: " NORMAL_END

echo ""
echo "================================================"
echo "  Summary"
echo "================================================"
echo "  Spike period: $SPIKE_START to $SPIKE_END"
echo "  Normal period: $NORMAL_START to $NORMAL_END"
echo ""
read -p "Proceed with extraction? (y/n): " CONFIRM

if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
    echo "❌ Cancelled by user"
    exit 0
fi

echo ""
echo "================================================"
echo "  Extracting Data from Both Servers"
echo "================================================"
echo ""

# Generate output filenames
PG00_SPIKE="spike_pg00_$(echo $SPIKE_START | tr ':' '-')_$(echo $SPIKE_END | tr ':' '-').json"
PG00_NORMAL="normal_pg00_$(echo $NORMAL_START | tr ':' '-')_$(echo $NORMAL_END | tr ':' '-').json"
PG01_SPIKE="spike_pg01_$(echo $SPIKE_START | tr ':' '-')_$(echo $SPIKE_END | tr ':' '-').json"
PG01_NORMAL="normal_pg01_$(echo $NORMAL_START | tr ':' '-')_$(echo $NORMAL_END | tr ':' '-').json"

# Extract pg00 spike
echo "📊 Step 1/4: Extracting pg00 (leader) spike period..."
echo ""
"$SCRIPT_DIR/extract_pg_activity_interval.sh" "$SPIKE_START" "$SPIKE_END" "pg00" "$PG00_SPIKE"

echo ""
echo "📊 Step 2/4: Extracting pg00 (leader) normal period..."
echo ""
"$SCRIPT_DIR/extract_pg_activity_interval.sh" "$NORMAL_START" "$NORMAL_END" "pg00" "$PG00_NORMAL"

echo ""
echo "📊 Step 3/4: Extracting pg01 (replica) spike period..."
echo ""
"$SCRIPT_DIR/extract_pg_activity_interval.sh" "$SPIKE_START" "$SPIKE_END" "pg01" "$PG01_SPIKE"

echo ""
echo "📊 Step 4/4: Extracting pg01 (replica) normal period..."
echo ""
"$SCRIPT_DIR/extract_pg_activity_interval.sh" "$NORMAL_START" "$NORMAL_END" "pg01" "$PG01_NORMAL"

echo ""
echo "================================================"
echo "  ✅ Data Extraction Complete!"
echo "================================================"
echo ""

# Run Python analysis
echo "================================================"
echo "  🔍 Analyzing Data..."
echo "================================================"
echo ""

REPORT_FILE="spike_analysis_$(date +%Y%m%d_%H%M%S).md"

python3 "$SCRIPT_DIR/compare_leader_replica.py" \
  --pg00-normal "$PG00_NORMAL" \
  --pg00-spike "$PG00_SPIKE" \
  --pg01-normal "$PG01_NORMAL" \
  --pg01-spike "$PG01_SPIKE" \
  --output "$REPORT_FILE"

echo ""
echo "================================================"
echo "  ✅ Analysis Complete!"
echo "================================================"
echo ""
echo "📄 Report saved to: $REPORT_FILE"
echo ""
echo "📊 Data files generated:"
echo "  - $PG00_SPIKE"
echo "  - $PG00_NORMAL"
echo "  - $PG01_SPIKE"
echo "  - $PG01_NORMAL"
echo ""
echo "To view the report:"
echo "  cat $REPORT_FILE"
echo ""
