#!/usr/bin/env python3
#
# Copyright 2025 ProjectASAP contributors
# SPDX-License-Identifier: Apache-2.0
#


from typing import Dict, Any, List, Tuple
from .output_parsers import create_parser
import re


class ExperimentAnalyzer:
    """Class responsible for analyzing and aggregating experiment results."""

    def __init__(self, sketch_type: str):
        self.sketch_type = sketch_type

    def aggregate_experiment_results(
        self, results: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Aggregate experiment results with throughput, CPU, and accuracy metrics."""
        summary = {}

        # First, group baseline experiments by their CPU configuration for matching
        baseline_experiments = self._group_baseline_experiments(results)

        # Process all experiments and calculate accuracy for matching pairs
        for experiment_name, experiment_data in results.items():
            summary[experiment_name] = self._process_single_experiment(
                experiment_name, experiment_data, baseline_experiments
            )

        return summary

    def _group_baseline_experiments(
        self, results: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Tuple[str, Dict[str, Any]]]:
        """
        Group baseline experiments by their resource configuration for matching.

        Experiments can have names like:
        - experiment_1_freq_baseline_cpu_1.0
        - experiment_1_freq_baseline_cpu_1.0_cardinality_1000000000
        - experiment_1_quantile_baseline_cpu_1_cardinality_1_window_2000_itemsWindow_1000000

        The matching key prioritizes itemsWindow if present, otherwise falls back to
        CPU, cardinality, and window for backward compatibility.
        """
        baseline_experiments = {}

        for experiment_name, experiment_data in results.items():
            if "baseline" in experiment_name:
                parts = experiment_name.split("_")

                # Find all relevant configuration parameters
                cpu_key = None
                cardinality_key = None
                window_key = None
                items_window_key = None

                for i, part in enumerate(parts):
                    if part == "cpu" and i + 1 < len(parts):
                        cpu_key = f"cpu_{parts[i + 1]}"
                    elif part == "cardinality" and i + 1 < len(parts):
                        cardinality_key = f"cardinality_{parts[i + 1]}"
                    elif part == "window" and i + 1 < len(parts):
                        window_key = f"window_{parts[i + 1]}"
                    elif part == "itemsWindow" and i + 1 < len(parts):
                        items_window_key = f"itemsWindow_{parts[i + 1]}"

                # Create matching key:
                # - If itemsWindow is present, use ONLY itemsWindow for matching
                # - Otherwise, use cpu, cardinality, window for backward compatibility
                if items_window_key:
                    key = items_window_key
                elif cpu_key:
                    key_parts = [cpu_key]
                    if cardinality_key:
                        key_parts.append(cardinality_key)
                    if window_key:
                        key_parts.append(window_key)
                    key = "_".join(key_parts)
                else:
                    continue

                baseline_experiments[key] = (experiment_name, experiment_data)

        return baseline_experiments

    def _process_single_experiment(
        self,
        experiment_name: str,
        experiment_data: Dict[str, Any],
        baseline_experiments: Dict[str, Tuple[str, Dict[str, Any]]],
    ) -> Dict[str, Any]:
        """Process a single experiment and calculate all metrics."""
        throughput_in_data = experiment_data.get("throughput_in_data", {})
        throughput_out_data = experiment_data.get("throughput_out_data", {})
        cpu_usage_data = experiment_data.get("cpu_usage_data", [])
        memory_data = experiment_data.get("memory_data", [])
        precompute_hashmaps = experiment_data.get("precompute_hashmaps", [])
        experiment_metadata = experiment_data.get("experiment_metadata", {})
        overall_avg_backpressure = experiment_data.get("overall_avg_backpressure", 0.0)

        # Calculate throughput metrics
        throughput_in_metrics = self._calculate_throughput_metrics(throughput_in_data)
        throughput_out_metrics = self._calculate_throughput_metrics(throughput_out_data)

        # Calculate CPU usage metrics
        cpu_metrics = self._calculate_cpu_metrics(cpu_usage_data)

        # Calculate memory usage metrics
        memory_metrics = self._calculate_memory_metrics(memory_data)

        # Calculate accuracy if this is a datasketches experiment with a matching baseline
        accuracy_metrics = self._calculate_accuracy_metrics(
            experiment_name, precompute_hashmaps, baseline_experiments
        )

        return {
            "avg_throughput_in": throughput_in_metrics["avg"],
            "max_throughput_in": throughput_in_metrics["max"],
            "throughput_in_samples": throughput_in_metrics["samples"],
            "avg_throughput_out": throughput_out_metrics["avg"],
            "max_throughput_out": throughput_out_metrics["max"],
            "throughput_out_samples": throughput_out_metrics["samples"],
            "avg_cpu_usage": cpu_metrics["avg"],
            "max_cpu_usage": cpu_metrics["max"],
            "avg_memory_usage": memory_metrics["avg"],
            "max_memory_usage": memory_metrics["max"],
            "memory_samples": memory_metrics["samples"],
            "avg_source_backpressure": overall_avg_backpressure,
            "precompute_hashmaps": precompute_hashmaps,
            "accuracy_metrics": accuracy_metrics,
            "experiment_metadata": experiment_metadata,
        }

    def _calculate_throughput_metrics(
        self, throughput_data: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, float]:
        """Calculate throughput metrics (aggregate across all vertices)."""
        all_throughputs = []
        total_samples = 0

        for measurements in throughput_data.values():
            for measurement in measurements:
                all_throughputs.append(measurement["throughput"])
                total_samples += 1

        if all_throughputs:
            return {
                "avg": sum(all_throughputs) / len(all_throughputs),
                "max": max(all_throughputs),
                "samples": total_samples,
            }
        else:
            return {"avg": 0, "max": 0, "samples": 0}

    def _calculate_cpu_metrics(
        self, cpu_usage_data: List[Dict[str, Any]]
    ) -> Dict[str, float]:
        """Calculate CPU usage metrics."""
        if cpu_usage_data:
            all_cpu_usage = [measurement["cpu_usage"] for measurement in cpu_usage_data]
            return {
                "avg": sum(all_cpu_usage) / len(all_cpu_usage),
                "max": max(all_cpu_usage),
            }
        else:
            return {"avg": 0, "max": 0}

    def _calculate_memory_metrics(
        self, memory_data: List[Dict[str, Any]]
    ) -> Dict[str, float]:
        """Calculate memory usage metrics."""
        if memory_data:
            all_memory_usage = [
                measurement["memory_bytes"] for measurement in memory_data
            ]
            return {
                "avg": sum(all_memory_usage) / len(all_memory_usage),
                "max": max(all_memory_usage),
                "samples": len(all_memory_usage),
            }
        else:
            return {"avg": 0, "max": 0, "samples": 0}

    def _calculate_accuracy_metrics(
        self,
        experiment_name: str,
        precompute_hashmaps: List[Any],
        baseline_experiments: Dict[str, Tuple[str, Dict[str, Any]]],
    ) -> Dict[str, Any]:
        """Calculate accuracy metrics if this is a sketch experiment (not baseline) with a matching baseline."""
        accuracy_metrics = {}

        # Skip baseline experiments - they are the ground truth
        if "baseline" in experiment_name:
            return accuracy_metrics

        # For all non-baseline experiments (datasketches, custom, etc.), try to find matching baseline
        # Extract all configuration parameters for matching
        parts = experiment_name.split("_")

        cpu_key = None
        cardinality_key = None
        window_key = None
        items_window_key = None

        for i, part in enumerate(parts):
            if part == "cpu" and i + 1 < len(parts):
                cpu_key = f"cpu_{parts[i + 1]}"
            elif part == "cardinality" and i + 1 < len(parts):
                cardinality_key = f"cardinality_{parts[i + 1]}"
            elif part == "window" and i + 1 < len(parts):
                window_key = f"window_{parts[i + 1]}"
            elif part == "itemsWindow" and i + 1 < len(parts):
                items_window_key = f"itemsWindow_{parts[i + 1]}"

        # Create matching key (same logic as _group_baseline_experiments):
        # - If itemsWindow is present, use ONLY itemsWindow for matching
        # - Otherwise, use cpu, cardinality, window for backward compatibility
        key = None
        if items_window_key:
            key = items_window_key
        elif cpu_key:
            key_parts = [cpu_key]
            if cardinality_key:
                key_parts.append(cardinality_key)
            if window_key:
                key_parts.append(window_key)
            key = "_".join(key_parts)

        if key and key in baseline_experiments:
            _, baseline_data = baseline_experiments[key]
            baseline_hashmaps = baseline_data.get("precompute_hashmaps", [])
            if precompute_hashmaps and baseline_hashmaps:
                # Create parser to calculate accuracy
                parser = create_parser(
                    self.sketch_type, ""
                )  # directory not needed for accuracy calc
                accuracy_metrics = parser.calculate_accuracy(
                    precompute_hashmaps, baseline_hashmaps
                )

        return accuracy_metrics

    @staticmethod
    def _experiment_sort_key(name: str):
        """
        Extract numeric index from strings like 'experiment_5_*'.
        Falls back to +inf so non-matching names go to the end; tie-break on full name.
        """
        m = re.search(r"experiment[_-]?(\d+)", name)
        idx = int(m.group(1)) if m else float("inf")
        return (idx, name)

    def create_experiment_pairs(
        self, results: Dict[str, Dict[str, Any]]
    ) -> List[Tuple[Tuple[str, Dict[str, Any]], Tuple[str, Dict[str, Any]]]]:
        """Group experiments by query type and cardinality, then create pairs within each group."""
        # Group experiments by (query_type, cardinality)
        query_cardinality_groups = {}
        for exp_name, exp_data in results.items():
            parts = exp_name.split("_")
            if len(parts) < 3:
                continue
            query_type = parts[2]  # freq, quantile, topk, etc.

            # Extract cardinality from folder name
            cardinality = None
            try:
                if "cardinality" in parts:
                    cardinality_idx = parts.index("cardinality")
                    if cardinality_idx + 1 < len(parts):
                        cardinality = int(parts[cardinality_idx + 1])
            except (ValueError, IndexError):
                pass

            # Group by (query_type, cardinality)
            group_key = (query_type, cardinality)
            if group_key not in query_cardinality_groups:
                query_cardinality_groups[group_key] = []
            query_cardinality_groups[group_key].append((exp_name, exp_data))

        # Sort within each group and create pairs
        pairs: List[Tuple[Tuple[str, Dict[str, Any]], Tuple[str, Dict[str, Any]]]] = []
        for group_key in sorted(query_cardinality_groups.keys()):
            experiments = sorted(
                query_cardinality_groups[group_key],
                key=lambda kv: self._experiment_sort_key(kv[0]),
            )
            # Create pairs within this query type and cardinality: (0,1), (2,3), etc.
            for i in range(0, len(experiments) - 1, 2):
                if i + 1 < len(experiments):
                    pairs.append((experiments[i], experiments[i + 1]))

        return pairs


class ThroughputCalculator:
    """Helper class for calculating throughput metrics."""

    @staticmethod
    def get_experiment_start_time(exp_data: Dict[str, Any]) -> float:
        """Calculate start time for an experiment."""
        exp_timestamps = []
        for data_type in ["throughput_in_data", "throughput_out_data"]:
            for measurements in exp_data.get(data_type, {}).values():
                for measurement in measurements:
                    exp_timestamps.append(measurement["timestamp"])
        return min(exp_timestamps) if exp_timestamps else 0

    @staticmethod
    def prepare_throughput_data(
        measurements: List[Dict[str, Any]], start_time: float
    ) -> Tuple[List[float], List[float]]:
        """Prepare throughput data for plotting."""
        measurements.sort(key=lambda x: x["timestamp"])
        timestamps = [m["timestamp"] for m in measurements]
        throughputs = [m["throughput"] for m in measurements]
        relative_times = [(t - start_time) for t in timestamps]
        return relative_times, throughputs
