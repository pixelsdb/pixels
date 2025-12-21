#!/bin/bash

# ====================================================
# Script Function: Batch generation of CPU/I/O/Scheduling related Flame Graphs (Perf + FlameGraph)
# Analysis Events: 1. cpu-clock, 2. page-faults, 3. branch-misses, 4. sched_switch/sched_stat_wait
# Distinction: Each event uses a different color palette (hot, mem, perf, chain)
# Usage: $0 <SQL_FILE_PATH> <DUCKDB_BINARY_PATH> <OUTPUT_DIR>
# Example: $0 test_q01.sql ../../build/release/duckdb ./results
# ====================================================

# --- Configure FlameGraph Path ---
FLAMEGRAPH_DIR="$HOME/third-party/FlameGraph"
STACKCOLLAPSE="${FLAMEGRAPH_DIR}/stackcollapse-perf.pl"
FLAMEGRAPH_PL="${FLAMEGRAPH_DIR}/flamegraph.pl"

# ----------------------------------------------------
# 1. Check Environment and Arguments
# ----------------------------------------------------

# Check FlameGraph dependencies
if [ ! -x "$STACKCOLLAPSE" ] || [ ! -x "$FLAMEGRAPH_PL" ]; then
    echo "Error: Cannot find or execute FlameGraph tools (stackcollapse-perf.pl or flamegraph.pl)."
    echo "Please ensure the FlameGraph repository is cloned to $HOME/FlameGraph."
    exit 1
fi

# Check argument count
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <SQL_FILE_PATH> <DUCKDB_BINARY_PATH> <OUTPUT_DIR>"
    echo "Example: $0 test_q01.sql ../build/release/duckdb ./results"
    exit 1
fi

# Receive arguments
SQL_FILE=$1
DUCKDB_BINARY=$2
OUTPUT_DIR=$3

# Check if DuckDB executable exists
if [ ! -x "$DUCKDB_BINARY" ]; then
    echo "Error: Cannot find or execute ${DUCKDB_BINARY}"
    exit 1
fi

# Create output directory (if it doesn't exist)
mkdir -p "$OUTPUT_DIR"
if [ ! -d "$OUTPUT_DIR" ]; then
    echo "Error: Cannot create output directory ${OUTPUT_DIR}"
    exit 1
fi

# Extract filename as prefix (e.g., test_q01)
FILENAME_PREFIX=$(basename "$SQL_FILE" .sql)

echo "--- Starting Query Analysis: ${FILENAME_PREFIX} ---"
echo "Using executable: ${DUCKDB_BINARY}"
echo "Output directory: ${OUTPUT_DIR}"

# ----------------------------------------------------
# 2. Core Function Definition
# ----------------------------------------------------

# Function: Generate Flame Graph for specified event
# $1: Event Name (perf event name, e.g., cpu-clock, page-faults)
# $2: Friendly Event Name (e.g., CPU Time)
# $3: Output File Suffix (e.g., cpu_time, page_faults)
# $4: Color Palette Name (e.g., hot, mem, perf, chain)
function generate_flamegraph {
    local EVENT_NAME="$1"
    local FRIENDLY_NAME="$2"
    local SUFFIX="$3"
    local COLOR_PALETTE="$4"

    echo ""
    echo "----------------------------------------------------"
    echo "Start recording event: ${FRIENDLY_NAME} (${EVENT_NAME}) - Color: ${COLOR_PALETTE}"
    echo "----------------------------------------------------"

    local DATA_FILE="${OUTPUT_DIR}/${FILENAME_PREFIX}_${SUFFIX}.data"
    local PERF_TXT="${OUTPUT_DIR}/${FILENAME_PREFIX}_${SUFFIX}.perf.txt"
    local FOLDED_FILE="${OUTPUT_DIR}/${FILENAME_PREFIX}_${SUFFIX}.folded"
    local SVG_FILE="${OUTPUT_DIR}/${FILENAME_PREFIX}_${SUFFIX}.svg"

    # --- Record Data ---
    # -e specifies event, --call-graph=dwarf captures call stack, -g enables call graph
    sudo -E perf record --call-graph=dwarf -e "$EVENT_NAME" -g -o "$DATA_FILE" -F 10\
      -- "$DUCKDB_BINARY" < "$SQL_FILE"

    if [ $? -ne 0 ]; then
        echo "[ERROR] perf record failed. Check permissions or perf installation."
        return 1
    fi

    # --- Convert Data and Generate Flame Graph ---
    echo "Converting ${FRIENDLY_NAME} data and generating Flame Graph..."
    sudo perf script -i "$DATA_FILE" > "$PERF_TXT"
    "$STACKCOLLAPSE" "$PERF_TXT" > "$FOLDED_FILE"

    # Add --color argument to specify color palette
    "$FLAMEGRAPH_PL" --title="${FILENAME_PREFIX} ${FRIENDLY_NAME} Hotspots" --countname="$FRIENDLY_NAME" \
      --color="$COLOR_PALETTE" \
      "$FOLDED_FILE" > "$SVG_FILE"
    echo "✅ ${FRIENDLY_NAME} Flame Graph generated: ${SVG_FILE}"

    # --- Cleanup Intermediate Files ---
    echo "Cleaning up ${FRIENDLY_NAME} intermediate files..."
    rm -f "$PERF_TXT" "$FOLDED_FILE"
    sudo rm -f "$DATA_FILE"
}

# ----------------------------------------------------
# 3. Run Analysis (4 Flame Graphs total)
# ----------------------------------------------------

# 1. CPU Time Analysis (Standard CPU bottleneck analysis)
generate_flamegraph "cpu-clock" "CPU Time" "cpu_time" "hot"

# 2. I/O Bottleneck Analysis (Related to memory access)
generate_flamegraph "page-faults" "Page Faults" "page_faults" "mem"

# 3. Computational Efficiency Analysis (Related to pipeline)
generate_flamegraph "branch-misses" "Branch Misses" "branch_misses" "hot"

# 4. Scheduling/Wait Bottleneck Analysis (Related to lock contention, context switching)
# Note: This event requires two perf event names
generate_flamegraph "sched:sched_switch,sched:sched_stat_wait" "Thread Scheduling" "sched" "chain"


# ----------------------------------------------------
# 4. Task Summary
# ----------------------------------------------------
echo ""
echo "--- Task Complete ---"
echo "Final result files (SVG/HTML) are in the ${OUTPUT_DIR} directory:"
find "$OUTPUT_DIR" -name "${FILENAME_PREFIX}_*.svg"