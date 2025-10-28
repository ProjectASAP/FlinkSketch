#!/usr/bin/env python3
#
# Copyright 2025 ProjectASAP contributors
# SPDX-License-Identifier: Apache-2.0
#

"""
Simple Experiment Runner for Flink Streaming Jobs
Reads configuration from YAML, starts one container, and monitors one job.
"""

import argparse
import os
import subprocess
import time
import yaml
from utils.start.container_manager import FlinkClusterService
from utils.start.job_monitor import FlinkJobMonitor
from typing import Dict, Any


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file"""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config or {}


def get_config_value(config: Dict[str, Any], *keys):
    """Get nested configuration value"""
    current = config
    for key in keys:
        current = current[key]
    return current


def generate_resource_values(range_config, step):
    """Generate resource values based on range and step configuration"""
    if isinstance(range_config, list):
        if len(range_config) == 2:
            # [min, max] - generate range using step
            min_val, max_val = range_config
            values = []
            current = min_val
            while current <= max_val:
                values.append(current)
                current *= step
            return values
        elif len(range_config) == 1:
            # Single value in a list, unwrap it
            return range_config
        else:
            # List with 3+ values - use as explicit list of values
            return range_config
    else:
        # Single scalar value
        return [range_config]


def run_single_experiment(
    config, resource_value, experiment_number, algorithm_type, config_file_path
):
    """Run a single experiment with specified resource value and algorithm type"""
    print(
        f"\n=== Running Experiment {experiment_number} ({algorithm_type}) (Resource: {resource_value}) ==="
    )

    # Extract configuration values
    flink_root = os.getenv("FLINK_HOME")
    if not flink_root:
        raise ValueError("FLINK_HOME environment variable is not set")
    jar_path = os.path.join(flink_root, get_config_value(config, "flink", "jar_path"))
    output_format = get_config_value(config, "flink", "output_format")
    verbose = get_config_value(config, "flink", "verbose")
    pipeline = get_config_value(config, "flink", "pipeline")
    output_mode = get_config_value(config, "flink", "output_mode")
    parallelism = get_config_value(config, "flink", "parallelism")
    datagen_key_cardinality = get_config_value(
        config, "flink", "datagen_key_cardinality"
    )
    datagen_items_per_window = get_config_value(
        config, "flink", "datagen_items_per_window"
    )
    enable_merging = get_config_value(config, "flink", "enable_merging")
    distribution = config.get("flink", {}).get("distribution", "uniform")

    # Get resource configuration
    resource_name = get_config_value(config, "experiments", "resource", "name")
    query_name = get_config_value(config, "experiments", "query", "name")
    query_keys = get_config_value(config, "experiments", "query", "keys")
    query_ranks = get_config_value(config, "experiments", "query", "ranks")

    # Read tumbling window size from aggregation config file
    tumbling_window_size = None
    if config_file_path and os.path.exists(config_file_path):
        with open(config_file_path, "r") as f:
            agg_config = yaml.safe_load(f)
            if "aggregations" in agg_config and len(agg_config["aggregations"]) > 0:
                tumbling_window_size = agg_config["aggregations"][0].get(
                    "tumblingWindowSize"
                )

    # Docker resource settings - use defaults and override the target resource
    jobmanager_cpu = get_config_value(
        config, "docker", "default_resources", "jobmanager", "cpu"
    )
    jobmanager_memory = get_config_value(
        config, "docker", "default_resources", "jobmanager", "memory"
    )
    taskmanager_cpu = get_config_value(
        config, "docker", "default_resources", "taskmanager", "cpu"
    )
    taskmanager_jvm_heap_memory = get_config_value(
        config, "docker", "default_resources", "taskmanager", "jvm_heap_memory"
    )

    # Apply the resource value to the specified resource
    if resource_name == "cpu":
        taskmanager_cpu = float(resource_value)
        # Set parallelism to match CPU cores when varying CPU
        parallelism = int(resource_value)
    elif resource_name == "memory":
        # When varying memory, the resource_value is the JVM heap size
        taskmanager_jvm_heap_memory = f"{resource_value}g"
    else:
        raise ValueError(
            f"Unsupported resource name: '{resource_name}'. "
            f"Supported resource names are: 'cpu', 'memory'"
        )

    # Create experiment-specific output directory
    base_output_dir = os.path.join(
        flink_root, get_config_value(config, "paths", "experiments_base")
    )

    # Build folder name with tumbling window size and items per window if available
    folder_parts = [
        f"experiment_{experiment_number}",
        query_name,
        algorithm_type,
        f"{resource_name}_{resource_value}",
        f"cardinality_{datagen_key_cardinality}",
    ]
    if tumbling_window_size is not None:
        folder_parts.append(f"window_{tumbling_window_size}")
    if datagen_items_per_window is not None:
        folder_parts.append(f"itemsWindow_{datagen_items_per_window}")

    experiment_output_dir = os.path.join(base_output_dir, "_".join(folder_parts))

    # Monitoring settings
    monitoring_host = get_config_value(config, "monitoring", "host")
    monitoring_port = get_config_value(config, "monitoring", "port")
    monitoring_interval = get_config_value(config, "monitoring", "interval")
    monitoring_timer = get_config_value(config, "monitoring", "timer")
    monitoring_output_dir = os.path.join(experiment_output_dir, "monitoring")
    monitoring_flink_bin = os.path.join(
        flink_root, get_config_value(config, "monitoring", "flink_bin")
    )
    log_file_name = get_config_value(config, "monitoring", "log_file_name")
    architecture = get_config_value(config, "monitoring", "architecture")

    # Flink file paths
    output_file_path = os.path.join(experiment_output_dir, "flink")

    print(f"Experiment output directory: {experiment_output_dir}")
    print(f"Duration: {monitoring_timer} seconds")

    # Create experiment output directory
    os.makedirs(experiment_output_dir, exist_ok=True)

    # Dump experiment configuration for reproducibility
    config_dump_path = os.path.join(experiment_output_dir, "experiment_config.yaml")
    with open(config_dump_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    print(f"Experiment configuration saved to: {config_dump_path}")

    print("\n--- Starting Flink Container ---")

    # Initialize and start Flink container
    container_service = FlinkClusterService()

    # Start container (non-blocking)
    container_service.start(
        jobmanager_cpu=float(jobmanager_cpu),
        jobmanager_memory=str(jobmanager_memory),
        taskmanager_cpu=float(taskmanager_cpu),
        taskmanager_memory=str(taskmanager_jvm_heap_memory),
        experiment_output_dir=experiment_output_dir,
        jar_path=jar_path,
        output_file_path=output_file_path,
        config_file_path=config_file_path,
        output_format=output_format,
        verbose=verbose,
        pipeline=pipeline,
        output_mode=output_mode,
        datagen_key_cardinality=int(datagen_key_cardinality),
        datagen_items_per_window=int(datagen_items_per_window),
        query_keys=query_keys,
        query_ranks=query_ranks,
        query_name=query_name,
        flink_root=flink_root,
        taskmanager_slots=int(parallelism),
        parallelism=int(parallelism),
        enable_merging=bool(enable_merging),
        distribution=distribution,
    )

    print("Web UI available at: http://localhost:8081")

    print("Waiting for cluster to stabilize...")
    while True:
        result = subprocess.run(
            [
                "docker",
                "exec",
                "flink-jobmanager",
                "curl",
                "-s",
                "http://localhost:8081/overview",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            print("Cluster is ready!")
            break
        time.sleep(2)

    print("\n--- Starting Job Monitoring ---")

    monitor = FlinkJobMonitor(
        host=monitoring_host,
        port=int(monitoring_port),
        interval=int(monitoring_interval),
        output_dir=monitoring_output_dir,
        timer=int(monitoring_timer),
        flink_bin=monitoring_flink_bin,
        log_file_name=log_file_name,
        container_service=container_service,
        architecture=architecture,
        parallelism=int(parallelism),
    )

    monitor.monitor_continuously()  # blocking

    print("Cleaning up containers...")
    container_service.stop()

    print(f"\n--- Experiment {experiment_number} Completed ---")
    print("Job monitoring finished and jobs stopped.")
    print(f"Experiment {experiment_number} results saved in: {experiment_output_dir}")


def main():
    parser = argparse.ArgumentParser(description="Flink Experiment Runner")
    parser.add_argument("--config", required=True, help="Configuration file path")

    args = parser.parse_args()

    print("=== Flink Sequential Experiment Runner ===")
    print(f"Config file: {args.config}")

    # Load configuration
    config = load_config(args.config)

    # Get experiment resource configuration
    resource_name = get_config_value(config, "experiments", "resource", "name")
    resource_range = get_config_value(config, "experiments", "resource", "range")
    resource_step = get_config_value(config, "experiments", "resource", "step")

    # Generate resource values to test
    resource_values = generate_resource_values(resource_range, resource_step)

    print(f"\nResource to test: {resource_name}")
    print(f"Resource values: {resource_values}")

    # Create base output directory
    flink_root = os.getenv("FLINK_HOME")
    base_output_dir = os.path.join(
        flink_root, get_config_value(config, "paths", "experiments_base")
    )
    os.makedirs(base_output_dir, exist_ok=True)

    # Get available algorithm configurations
    query_config = get_config_value(config, "experiments", "query")
    baseline_config_path = query_config.get("baseline", {}).get("config_file_path")
    datasketches_config_path = query_config.get("datasketches", {}).get(
        "config_file_path"
    )
    custom_config_path = query_config.get("custom", {}).get("config_file_path")

    algorithms = []
    if baseline_config_path:
        algorithms.append(("baseline", os.path.join(flink_root, baseline_config_path)))
    if datasketches_config_path:
        algorithms.append(
            ("datasketches", os.path.join(flink_root, datasketches_config_path))
        )
    if custom_config_path:
        algorithms.append(("custom", os.path.join(flink_root, custom_config_path)))

    if not algorithms:
        print("ERROR: No algorithm configurations found!")
        return

    print(f"Algorithms to test: {[alg[0] for alg in algorithms]}")
    print(f"Total experiments: {len(resource_values) * len(algorithms)}")

    # Run experiments sequentially
    experiment_counter = 1
    for resource_value in resource_values:
        for algorithm_type, config_file_path in algorithms:
            run_single_experiment(
                config,
                resource_value,
                experiment_counter,
                algorithm_type,
                config_file_path,
            )
            experiment_counter += 1

            # Add delay between experiments to ensure clean separation
            print("\nWaiting 10 seconds before next experiment...")
            time.sleep(10)

    print("\n=== All Experiments Completed ===")
    print(f"Results saved in: {base_output_dir}")
    print("Done!")


if __name__ == "__main__":
    main()
