#!/bin/bash
#
# Copyright 2025 ProjectASAP contributors
# SPDX-License-Identifier: Apache-2.0
#


# Script to run experiments with different tumbling window sizes
# Each experiment runs sequentially and waits for completion before starting the next

set -e  # Exit on error

# Define tumbling window sizes to test (in seconds)
WINDOW_SIZES=(
    100
    500
    1000
    2000
    3000
)

# Path to the experiment config file
EXPERIMENT_CONFIG="${FLINK_HOME}/FlinkSketch/flinksketch-bench/config/experiment_config.yaml"

# Path to the start_experiment script
SCRIPT_DIR="${FLINK_HOME}/FlinkSketch/flinksketch-bench/benchmarks/experiments"

echo "=========================================="
echo "Starting Tumbling Window Sweep Experiments"
echo "=========================================="
echo "Window sizes to test (seconds): ${WINDOW_SIZES[@]}"
echo ""

# Get the aggregation config file paths from the experiment config
BASELINE_CONFIG=$(grep -A 2 "baseline:" "$EXPERIMENT_CONFIG" | grep "config_file_path" | awk '{print $2}' | tr -d "'")
DATASKETCHES_CONFIG=$(grep -A 2 "datasketches:" "$EXPERIMENT_CONFIG" | grep "config_file_path" | awk '{print $2}' | tr -d "'")
CUSTOM_CONFIG=$(grep -A 2 "custom:" "$EXPERIMENT_CONFIG" | grep "config_file_path" | awk '{print $2}' | tr -d "'")

echo "Aggregation config files:"
echo "  Baseline: $BASELINE_CONFIG"
echo "  DataSketches: $DATASKETCHES_CONFIG"
echo "  Custom: $CUSTOM_CONFIG"
echo ""

# Loop through each window size
for window_size in "${WINDOW_SIZES[@]}"; do
    echo "=========================================="
    echo "Running experiment with window size: $window_size seconds"
    echo "=========================================="

    # Calculate monitoring timer (window size + 100)
    monitoring_timer=$((window_size + 100))

    # Update the tumbling window size in baseline config
    if [ -n "$BASELINE_CONFIG" ] && [ -f "$BASELINE_CONFIG" ]; then
        sed -i "s/^\s*tumblingWindowSize:.*/    tumblingWindowSize: $window_size/" "$BASELINE_CONFIG"
        echo "Updated baseline config:"
        grep "tumblingWindowSize" "$BASELINE_CONFIG"
    fi

    # Update the tumbling window size in datasketches config
    if [ -n "$DATASKETCHES_CONFIG" ] && [ -f "$DATASKETCHES_CONFIG" ]; then
        sed -i "s/^\s*tumblingWindowSize:.*/    tumblingWindowSize: $window_size/" "$DATASKETCHES_CONFIG"
        echo "Updated datasketches config:"
        grep "tumblingWindowSize" "$DATASKETCHES_CONFIG"
    fi

    # Update the tumbling window size in custom config
    if [ -n "$CUSTOM_CONFIG" ] && [ -f "$CUSTOM_CONFIG" ]; then
        sed -i "s/^\s*tumblingWindowSize:.*/    tumblingWindowSize: $window_size/" "$CUSTOM_CONFIG"
        echo "Updated custom config:"
        grep "tumblingWindowSize" "$CUSTOM_CONFIG"
    fi

    # Update the monitoring timer in experiment config
    sed -i "s/^\s*timer:.*/  timer: $monitoring_timer/" "$EXPERIMENT_CONFIG"
    echo "Updated monitoring timer to: $monitoring_timer seconds"
    grep "timer:" "$EXPERIMENT_CONFIG"

    echo ""

    # Run the experiment
    echo "Starting experiment..."
    cd "$SCRIPT_DIR"
    uv run python start_experiment.py --config "$EXPERIMENT_CONFIG"

    # Check exit status
    if [ $? -eq 0 ]; then
        echo "✓ Experiment with window size $window_size seconds completed successfully"
    else
        echo "✗ Experiment with window size $window_size seconds failed"
        exit 1
    fi

    echo ""
    echo "Waiting 5 seconds before next experiment..."
    sleep 5
    echo ""
done

echo "=========================================="
echo "All window sweep experiments completed!"
echo "=========================================="
echo ""
echo "You can now run post-processing with:"
echo "  cd $SCRIPT_DIR"
echo "  uv run python -m utils.post.post_experiment"
