#!/bin/bash
#
# Copyright 2025 ProjectASAP contributors
# SPDX-License-Identifier: Apache-2.0
#


# Script to run experiments with different cardinalities
# Each experiment runs sequentially and waits for completion before starting the next

set -e  # Exit on error

# Define cardinalities to test
CARDINALITIES=(
    100000
    200000
    400000
    800000
    1600000
    3200000
)

# Path to the experiment config file
CONFIG_FILE="${FLINK_HOME}/FlinkSketch/flinksketch-bench/config/experiment_config.yaml"

# Path to the start_experiment script
SCRIPT_DIR="${FLINK_HOME}/FlinkSketch/flinksketch-bench/benchmarks/experiments"

echo "=========================================="
echo "Starting Cardinality Sweep Experiments"
echo "=========================================="
echo "Cardinalities to test: ${CARDINALITIES[@]}"
echo ""

# Loop through each cardinality
for cardinality in "${CARDINALITIES[@]}"; do
    echo "=========================================="
    echo "Running experiment with cardinality: $cardinality"
    echo "=========================================="

    # Update the cardinality in the config file using sed
    # This replaces the line containing 'datagen_key_cardinality:' with the new value
    sed -i "s/^\s*datagen_key_cardinality:.*/  datagen_key_cardinality: $cardinality/" "$CONFIG_FILE"

    # Verify the change
    echo "Updated config file:"
    grep "datagen_key_cardinality" "$CONFIG_FILE"
    echo ""

    # Run the experiment
    echo "Starting experiment..."
    cd "$SCRIPT_DIR"
    uv run python start_experiment.py --config "$CONFIG_FILE"

    # Check exit status
    if [ $? -eq 0 ]; then
        echo "✓ Experiment with cardinality $cardinality completed successfully"
    else
        echo "✗ Experiment with cardinality $cardinality failed"
        exit 1
    fi

    echo ""
    echo "Waiting 5 seconds before next experiment..."
    sleep 5
    echo ""
done

echo "=========================================="
echo "All cardinality sweep experiments completed!"
echo "=========================================="
echo ""
echo "You can now run post-processing with:"
echo "  cd $SCRIPT_DIR"
echo "  uv run python -m utils.post.post_experiment"
