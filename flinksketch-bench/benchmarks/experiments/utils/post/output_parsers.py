#!/usr/bin/env python3
#
# Copyright 2025 ProjectASAP contributors
# SPDX-License-Identifier: Apache-2.0
#


import json
import glob
import logging
import os
import sys
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, List
from tqdm import tqdm


class BaseOutputParser(ABC):
    """Base class for parsing experiment outputs."""

    def __init__(self, experiments_dir: str):
        self.experiments_dir = Path(experiments_dir)

    def parse_experiment_output(self) -> Dict[str, Dict[str, Any]]:
        """Parse experiment output for all experiments in the directory."""
        results = {}
        experiment_folders = [
            d
            for d in self.experiments_dir.iterdir()
            if d.is_dir() and d.name.startswith("experiment")
        ]

        for folder in experiment_folders:
            folder_name = folder.name
            results[folder_name] = {"precompute_data": [], "memory_data": []}
            metadata_extracted = False

            pattern = str(folder / "**" / ".part-*")
            output_files = glob.glob(pattern, recursive=True)

            for file_path in output_files:
                filename = os.path.basename(file_path)
                lines = Path(file_path).read_text().strip().split("\n")

                logging.info(f"Processing {filename} ({len(lines)} lines)")

                for line in tqdm(
                    lines,
                    desc=f"Parsing {filename}",
                    unit="lines",
                    file=sys.stdout,
                    leave=True,
                ):
                    if not line.strip():
                        continue

                    record = json.loads(line)

                    # Parse query data if present
                    precompute_data = record.get("query")
                    if precompute_data:
                        parsed_data = self.parse_precompute_record(precompute_data)
                        if parsed_data:
                            results[folder_name]["precompute_data"].append(parsed_data)

                    # Parse memory data if present
                    memory_bytes = record.get("memory_bytes")
                    if memory_bytes is not None:
                        results[folder_name]["memory_data"].append(
                            {
                                "timestamp": record.get("start_timestamp", 0),
                                "memory_bytes": memory_bytes,
                            }
                        )

                    if not metadata_extracted:
                        config_data = record.get("config", {})
                        results[folder_name]["experiment_metadata"] = {
                            "aggregation_type": config_data.get("aggregationType"),
                            "aggregation_package": config_data.get(
                                "aggregationPackage"
                            ),
                            "parameters": config_data.get("parameters", {}),
                            "window_size": config_data.get("tumblingWindowSize"),
                            "pipeline": record.get("pipeline", "unknown"),
                            "output_mode": record.get("output_mode", "unknown"),
                        }
                        metadata_extracted = True

                logging.info(f"Finished {filename}")
        return results

    @abstractmethod
    def parse_precompute_record(self, precompute_data: Dict[str, Any]) -> Any:
        """Parse a single precompute record. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def calculate_accuracy(
        self, sketch_data: List[Any], baseline_data: List[Any]
    ) -> Dict[str, float]:
        """
        Calculate accuracy metrics comparing sketch and baseline data.

        Must return a dict with at minimum these keys:
        - avg_relative_error: float
        - total_windows: int
        - avg_relative_error_percent: float
        - max_window_error: float
        - min_window_error: float
        """
        pass


class FreqSketchOutputParser(BaseOutputParser):
    """Parser for frequency sketch experiments."""

    def parse_precompute_record(self, precompute_data: Dict[str, Any]) -> Any:
        """Parse frequency sketch precompute record."""
        return precompute_data

    def calculate_accuracy(
        self,
        sketch_hashmaps: List[Dict[str, int]],
        baseline_hashmaps: List[Dict[str, int]],
    ) -> Dict[str, float]:
        """Calculate average relative error by comparing frequency sketch and baseline hashmaps."""
        window_errors = []

        for sketch_map, baseline_map in zip(sketch_hashmaps, baseline_hashmaps):
            all_keys = set(sketch_map.keys()) | set(baseline_map.keys())
            errors = [
                abs(sketch_map.get(k, 0) - baseline_map.get(k, 0))
                / baseline_map.get(k, 0)
                for k in all_keys
                if baseline_map.get(k, 0) != 0
            ]

            if errors:
                window_errors.append(sum(errors) / len(errors))

        if not window_errors:
            return {
                "avg_relative_error": float("inf"),
                "total_windows": 0,
                "avg_relative_error_percent": float("inf"),
                "max_window_error": float("inf"),
                "min_window_error": float("inf"),
                "error": "No valid comparisons possible",
            }

        avg_error = sum(window_errors) / len(window_errors)
        return {
            "avg_relative_error": avg_error,
            "total_windows": len(window_errors),
            "avg_relative_error_percent": avg_error * 100,
            "avg_accuracy_percent": (1 - avg_error)
            * 100,  # 1 - relative_error as percentage
            "max_window_error": max(window_errors),
            "min_window_error": min(window_errors),
        }


class QuantileSketchOutputParser(BaseOutputParser):
    """Parser for quantile sketch experiments."""

    def parse_precompute_record(self, precompute_data: Dict[str, Any]) -> Any:
        """Parse quantile sketch precompute record."""
        return precompute_data

    def calculate_accuracy(
        self, sketch_data: List[Dict[str, float]], baseline_data: List[Dict[str, float]]
    ) -> Dict[str, float]:
        """
        Calculate average relative error by comparing quantile sketch and baseline data.

        Dynamically extracts rank fields from the data (e.g., rank_0.1, rank_0.9)
        which are returned by the query() method in QuantilesAccumulator and DDSketchAccumulator.
        """
        if not sketch_data or not baseline_data:
            return {
                "avg_relative_error": float("inf"),
                "total_windows": 0,
                "avg_relative_error_percent": float("inf"),
                "max_window_error": float("inf"),
                "min_window_error": float("inf"),
                "error": "No data to compare",
            }

        # Dynamically extract rank fields from baseline data
        # Fields have format "rank_0.1", "rank_0.5", "rank_0.9", etc.
        rank_fields = set()
        for baseline_rec in baseline_data:
            if baseline_rec:
                rank_fields.update(
                    [
                        field
                        for field in baseline_rec.keys()
                        if field.startswith("rank_")
                    ]
                )
                break

        if not rank_fields:
            logging.warning("No rank fields found in baseline data")
            return {
                "avg_relative_error": float("inf"),
                "total_windows": 0,
                "avg_relative_error_percent": float("inf"),
                "max_window_error": float("inf"),
                "min_window_error": float("inf"),
                "error": "No rank fields found in data",
            }

        # Sort rank fields by numeric value for consistent ordering
        rank_fields = sorted(rank_fields, key=lambda x: float(x.split("_")[1]))
        logging.debug(f"Comparing rank fields: {rank_fields}")

        window_errors = []

        for sketch_rec, baseline_rec in zip(sketch_data, baseline_data):
            errors = []
            for field in rank_fields:
                sketch_val = sketch_rec.get(field)
                baseline_val = baseline_rec.get(field)

                if sketch_val is None or baseline_val is None:
                    continue

                if baseline_val == 0:
                    if sketch_val != 0:
                        errors.append(1.0)  # 100% error
                else:
                    errors.append(abs(sketch_val - baseline_val) / abs(baseline_val))

            if errors:
                window_errors.append(sum(errors) / len(errors))

        if not window_errors:
            return {
                "avg_relative_error": float("inf"),
                "total_windows": 0,
                "avg_relative_error_percent": float("inf"),
                "max_window_error": float("inf"),
                "min_window_error": float("inf"),
                "error": "No valid comparisons possible",
            }

        avg_error = sum(window_errors) / len(window_errors)
        return {
            "avg_relative_error": avg_error,
            "total_windows": len(window_errors),
            "avg_relative_error_percent": avg_error * 100,
            "avg_accuracy_percent": (1 - avg_error)
            * 100,  # 1 - relative_error as percentage
            "max_window_error": max(window_errors),
            "min_window_error": min(window_errors),
        }


class CardinalitySketchOutputParser(BaseOutputParser):
    """Parser for cardinality sketch experiments."""

    def parse_precompute_record(self, precompute_data: Dict[str, Any]) -> Any:
        """Parse cardinality sketch precompute record."""
        return precompute_data

    def calculate_accuracy(
        self, sketch_data: List[Dict[str, float]], baseline_data: List[Dict[str, float]]
    ) -> Dict[str, float]:
        """Calculate average relative error by comparing cardinality sketch and baseline data."""
        window_errors = []

        for sketch_rec, baseline_rec in zip(sketch_data, baseline_data):
            sketch_count = sketch_rec.get("distinct_count")
            baseline_count = baseline_rec.get("distinct_count")

            if sketch_count is None or baseline_count is None:
                continue

            if baseline_count == 0:
                if sketch_count != 0:
                    window_errors.append(1.0)
            else:
                window_errors.append(
                    abs(sketch_count - baseline_count) / baseline_count
                )

        if not window_errors:
            return {
                "avg_relative_error": float("inf"),
                "total_windows": 0,
                "avg_relative_error_percent": float("inf"),
                "max_window_error": float("inf"),
                "min_window_error": float("inf"),
                "error": "No valid comparisons possible",
            }

        avg_error = sum(window_errors) / len(window_errors)
        return {
            "avg_relative_error": avg_error,
            "total_windows": len(window_errors),
            "avg_relative_error_percent": avg_error * 100,
            "avg_accuracy_percent": (1 - avg_error)
            * 100,  # 1 - relative_error as percentage
            "max_window_error": max(window_errors),
            "min_window_error": min(window_errors),
        }


class EntropySketchOutputParser(BaseOutputParser):
    """Parser for entropy sketch experiments."""

    def parse_precompute_record(self, precompute_data: Dict[str, Any]) -> Any:
        """Parse entropy sketch precompute record."""
        return precompute_data

    def calculate_accuracy(
        self, sketch_data: List[Dict[str, float]], baseline_data: List[Dict[str, float]]
    ) -> Dict[str, float]:
        """Calculate average relative error by comparing entropy sketch and baseline data."""
        window_errors = []

        for sketch_rec, baseline_rec in zip(sketch_data, baseline_data):
            sketch_count = sketch_rec.get("total_entropy")
            baseline_count = baseline_rec.get("total_entropy")

            if sketch_count is None or baseline_count is None:
                continue

            if baseline_count == 0:
                if sketch_count != 0:
                    window_errors.append(1.0)
            else:
                window_errors.append(
                    abs(sketch_count - baseline_count) / baseline_count
                )

        if not window_errors:
            return {
                "avg_relative_error": float("inf"),
                "total_windows": 0,
                "avg_relative_error_percent": float("inf"),
                "max_window_error": float("inf"),
                "min_window_error": float("inf"),
                "error": "No valid comparisons possible",
            }

        avg_error = sum(window_errors) / len(window_errors)
        return {
            "avg_relative_error": avg_error,
            "total_windows": len(window_errors),
            "avg_relative_error_percent": avg_error * 100,
            "avg_accuracy_percent": (1 - avg_error)
            * 100,  # 1 - relative_error as percentage
            "max_window_error": max(window_errors),
            "min_window_error": min(window_errors),
        }


class PerKeyQuantileSketchOutputParser(BaseOutputParser):
    """Parser for per-key quantile sketch experiments."""

    def parse_precompute_record(self, precompute_data: Dict[str, Any]) -> Any:
        """Parse per-key quantile sketch precompute record."""
        return precompute_data

    def calculate_accuracy(
        self, sketch_data: List[Dict[str, float]], baseline_data: List[Dict[str, float]]
    ) -> Dict[str, float]:
        """
        Calculate average relative error by comparing per-key quantile sketch and baseline data.

        Expected format: Each record is {key: [quantile_values], ...}
        For example: {"key1": [42.5, 89.2, 99.1], "key2": [37.8, 82.3, 95.7]}
        Array elements correspond to different ranks in the order queried.
        """
        if not sketch_data or not baseline_data:
            return {
                "avg_relative_error": float("inf"),
                "total_windows": 0,
                "avg_relative_error_percent": float("inf"),
                "max_window_error": float("inf"),
                "min_window_error": float("inf"),
                "error": "No data to compare",
            }

        window_errors = []

        for sketch_rec, baseline_rec in zip(sketch_data, baseline_data):
            # Get all keys from both sketches and baselines
            all_keys = set(sketch_rec.keys()) | set(baseline_rec.keys())
            # Remove any metadata keys if present (keys starting with underscore)
            all_keys = {k for k in all_keys if not k.startswith("_")}

            errors = []
            for key in all_keys:
                sketch_vals = sketch_rec.get(key)
                baseline_vals = baseline_rec.get(key)

                if sketch_vals is None or baseline_vals is None:
                    continue

                # Ensure both are lists/arrays
                if not isinstance(sketch_vals, list):
                    sketch_vals = [sketch_vals]
                if not isinstance(baseline_vals, list):
                    baseline_vals = [baseline_vals]

                # Compare arrays element by element (rank by rank)
                for sketch_val, baseline_val in zip(sketch_vals, baseline_vals):
                    # Handle NaN values
                    if sketch_val == "NaN" or (
                        isinstance(sketch_val, float) and (sketch_val != sketch_val)
                    ):
                        continue
                    if baseline_val == "NaN" or (
                        isinstance(baseline_val, float)
                        and (baseline_val != baseline_val)
                    ):
                        continue

                    # Convert to float if needed
                    if isinstance(sketch_val, str):
                        sketch_val = float(sketch_val)
                    if isinstance(baseline_val, str):
                        baseline_val = float(baseline_val)

                    if baseline_val == 0:
                        if sketch_val != 0:
                            errors.append(1.0)  # 100% error
                    else:
                        errors.append(
                            abs(sketch_val - baseline_val) / abs(baseline_val)
                        )

            if errors:
                window_errors.append(sum(errors) / len(errors))

        if not window_errors:
            return {
                "avg_relative_error": float("inf"),
                "total_windows": 0,
                "avg_relative_error_percent": float("inf"),
                "max_window_error": float("inf"),
                "min_window_error": float("inf"),
                "error": "No valid comparisons possible",
            }

        avg_error = sum(window_errors) / len(window_errors)
        return {
            "avg_relative_error": avg_error,
            "total_windows": len(window_errors),
            "avg_relative_error_percent": avg_error * 100,
            "max_window_error": max(window_errors),
            "min_window_error": min(window_errors),
        }


class TopKSketchOutputParser(BaseOutputParser):
    """Parser for top-K sketch experiments."""

    def parse_precompute_record(self, precompute_data: Dict[str, Any]) -> Any:
        """Parse top-K sketch precompute record."""
        return precompute_data

    def calculate_accuracy(
        self, sketch_data: List[Dict[str, Any]], baseline_data: List[Dict[str, Any]]
    ) -> Dict[str, float]:
        """Calculate accuracy by comparing top-K sketch and baseline results."""
        window_count_errors = []
        window_recall_errors = []
        window_precisions = []

        for sketch_rec, baseline_rec in zip(sketch_data, baseline_data):
            sketch_topk = sketch_rec.get("topk", {})
            baseline_topk = baseline_rec.get("topk", {})

            if not sketch_topk or not baseline_topk:
                continue

            # top_k is now a dict of {key: value}
            sketch_keys = sketch_topk
            baseline_keys = baseline_topk

            # Calculate recall and precision for keys
            # Ψ = sketch's top-k items, Φ = ground-truth top-k items
            psi = set(sketch_keys.keys())  # Sketch's top-k
            phi = set(baseline_keys.keys())  # Ground-truth top-k

            if not phi:
                continue

            overlap = len(psi & phi)

            # Recall = |Ψ ∩ Φ| / |Φ|
            # Recall Error = 1 - Recall
            recall = overlap / len(phi)
            recall_error = 1 - recall
            window_recall_errors.append(recall_error)

            # Precision = |Ψ ∩ Φ| / |Ψ|
            if len(psi) > 0:
                precision = overlap / len(psi)
                window_precisions.append(precision)

            # Calculate relative error for overlapping keys
            errors = []
            for key in psi & phi:
                baseline_count = baseline_keys[key]
                sketch_count = sketch_keys[key]
                if baseline_count > 0:
                    errors.append(abs(sketch_count - baseline_count) / baseline_count)

            avg_count_error = sum(errors) / len(errors) if errors else 0
            window_count_errors.append(avg_count_error)

        if not window_count_errors or not window_recall_errors:
            return {
                "avg_relative_error": float("inf"),
                "avg_recall_error": float("inf"),
                "avg_precision": 0.0,
                "total_windows": 0,
                "avg_relative_error_percent": float("inf"),
                "avg_recall_error_percent": float("inf"),
                "avg_precision_percent": 0.0,
                "max_window_error": float("inf"),
                "min_window_error": float("inf"),
                "max_recall_error": float("inf"),
                "min_recall_error": float("inf"),
                "max_precision": 0.0,
                "min_precision": 0.0,
                "error": "No valid comparisons possible",
            }

        avg_count_error = sum(window_count_errors) / len(window_count_errors)
        avg_recall_error = sum(window_recall_errors) / len(window_recall_errors)
        avg_precision = (
            sum(window_precisions) / len(window_precisions)
            if window_precisions
            else 0.0
        )

        return {
            "avg_relative_error": avg_count_error,
            "avg_recall_error": avg_recall_error,
            "avg_precision": avg_precision,
            "total_windows": len(window_count_errors),
            "avg_relative_error_percent": avg_count_error * 100,
            "avg_recall_error_percent": avg_recall_error * 100,
            "avg_precision_percent": avg_precision * 100,
            "max_window_error": max(window_count_errors),
            "min_window_error": min(window_count_errors),
            "max_recall_error": max(window_recall_errors),
            "min_recall_error": min(window_recall_errors),
            "max_precision": max(window_precisions) if window_precisions else 0.0,
            "min_precision": min(window_precisions) if window_precisions else 0.0,
        }


def create_parser(sketch_type: str, experiments_dir: str) -> BaseOutputParser:
    """Factory function to create the appropriate parser based on sketch type.

    Supported query name values (matching experiment config):
    - freq: Frequency count on keys
    - quantile: Quantile estimation (global)
    - perKeyQuantile: Per-key quantile estimation
    - cardinality: Cardinality/distinct count estimation
    - topk: Top-K most frequent items
    - entropy: Entropy estimation (for Univmon)
    """
    if sketch_type == "freq":
        return FreqSketchOutputParser(experiments_dir)
    elif sketch_type == "quantile":
        return QuantileSketchOutputParser(experiments_dir)
    elif sketch_type == "perKeyQuantile":
        return PerKeyQuantileSketchOutputParser(experiments_dir)
    elif sketch_type == "cardinality":
        return CardinalitySketchOutputParser(experiments_dir)
    elif sketch_type == "entropy":
        return EntropySketchOutputParser(experiments_dir)
    elif sketch_type == "topk":
        return TopKSketchOutputParser(experiments_dir)
    else:
        raise ValueError(
            f"Unknown sketch type: '{sketch_type}'. Supported types: freq, quantile, perKeyQuantile, cardinality, topk, entropy"
        )
