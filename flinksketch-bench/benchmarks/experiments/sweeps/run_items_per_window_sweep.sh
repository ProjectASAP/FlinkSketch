#!/bin/bash
#
# Copyright 2025 ProjectASAP contributors
# SPDX-License-Identifier: Apache-2.0
#


# Script to run experiments with different items per window
# Each experiment runs sequentially and waits for completion before starting the next

set -e  # Exit on error

# Define items per window to test
ITEMS_PER_WINDOW=(
    100000
    200000
    400000
    800000
    1600000
    # 1000000
    # 2000000
    # 4000000
    # 8000000
    # 16000000
    # 32000000
)

# Path to the experiment config file
EXPERIMENT_CONFIG="${FLINK_HOME}/FlinkSketch/flinksketch-bench/config/experiment_config.yaml"

# Path to the start_experiment script
SCRIPT_DIR="${FLINK_HOME}/FlinkSketch/flinksketch-bench/benchmarks/experiments"

echo "=========================================="
echo "Starting Items Per Window Sweep Experiments"
echo "=========================================="
echo "Items per window to test: ${ITEMS_PER_WINDOW[@]}"
echo ""

# Loop through each items per window value
for items in "${ITEMS_PER_WINDOW[@]}"; do
    echo "=========================================="
    echo "Running experiment with items per window: $items"
    echo "=========================================="

    # Update the datagen_items_per_window in experiment config
    sed -i "s/^\s*datagen_items_per_window:.*/  datagen_items_per_window: $items  # Number of items to fit in each tumbling window/" "$EXPERIMENT_CONFIG"
    echo "Updated experiment config:"
    grep "datagen_items_per_window" "$EXPERIMENT_CONFIG"

    echo ""

    # Run the experiment
    echo "Starting experiment..."
    cd "$SCRIPT_DIR"
    uv run python start_experiment.py --config "$EXPERIMENT_CONFIG"

    # Check exit status
    if [ $? -eq 0 ]; then
        echo "✓ Experiment with $items items per window completed successfully"
    else
        echo "✗ Experiment with $items items per window failed"
        exit 1
    fi

    echo ""
    echo "Waiting 5 seconds before next experiment..."
    sleep 5
    echo ""
done

echo "=========================================="
echo "All items per window sweep experiments completed!"
echo "=========================================="
echo ""
echo "You can now run post-processing with:"
echo "  cd $SCRIPT_DIR"
echo "  uv run python -m utils.post.post_experiment"
