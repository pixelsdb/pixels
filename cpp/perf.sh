#!/bin/bash

# Check number of arguments
if [ $# -ne 2 ]; then
    echo "Usage: $0 <query_file> <output_file_prefix>"
    echo "Example: $0 test_q01.sql query_1"
    exit 1
fi

# Assign parameters
QUERY_FILE="$1"
OUTPUT_PREFIX="$2"

# Record start time (nanoseconds)
START_TIME=$(date +%s%N)
PREVIOUS_TIME=$START_TIME

# Function to display time duration
show_time() {
    local start=$1
    local end=$2
    local stage=$3

    # Calculate duration in milliseconds (integer arithmetic in bash)
    local duration=$(( (end - start) / 1000000 ))
    echo "Stage '$stage' duration: ${duration}ms"
}

echo "Starting performance analysis..."

# Run perf recording
echo "1. Running perf record..."
sudo -E perf record -F 1 --call-graph=dwarf -g ./build/release/duckdb < "$QUERY_FILE"
CURRENT_TIME=$(date +%s%N)
show_time $PREVIOUS_TIME $CURRENT_TIME "Running perf record"
PREVIOUS_TIME=$CURRENT_TIME

# Generate perf script output
echo "2. Generating perf script output..."
sudo perf script -i perf.data > "${OUTPUT_PREFIX}.perf"
CURRENT_TIME=$(date +%s%N)
show_time $PREVIOUS_TIME $CURRENT_TIME "Generating perf script output"
PREVIOUS_TIME=$CURRENT_TIME

# Collapse call stacks
echo "3. Collapsing call stacks..."
stackcollapse-perf.pl "${OUTPUT_PREFIX}.perf" > "${OUTPUT_PREFIX}.folded"
CURRENT_TIME=$(date +%s%N)
show_time $PREVIOUS_TIME $CURRENT_TIME "Collapsing call stacks"
PREVIOUS_TIME=$CURRENT_TIME

# Generate flame graph
echo "4. Generating flame graph..."
flamegraph.pl "${OUTPUT_PREFIX}.folded" > "${OUTPUT_PREFIX}-cpu.svg"
CURRENT_TIME=$(date +%s%N)
show_time $PREVIOUS_TIME $CURRENT_TIME "Generating flame graph"
PREVIOUS_TIME=$CURRENT_TIME

# Rename perf data file
echo "5. Renaming perf data file..."
mv perf.data "${OUTPUT_PREFIX}-perf.data"
CURRENT_TIME=$(date +%s%N)
show_time $PREVIOUS_TIME $CURRENT_TIME "Renaming files"
PREVIOUS_TIME=$CURRENT_TIME

# Calculate total duration
TOTAL_DURATION=$(( (CURRENT_TIME - START_TIME) / 1000000 ))
echo "Total duration: ${TOTAL_DURATION}ms"

echo "Operation completed. Generated files:"
echo "${OUTPUT_PREFIX}.perf"
echo "${OUTPUT_PREFIX}.folded"
echo "${OUTPUT_PREFIX}-cpu.svg"
echo "${OUTPUT_PREFIX}-perf.data"