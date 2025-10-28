#!/usr/bin/env python3
#
# Copyright 2025 ProjectASAP contributors
# SPDX-License-Identifier: Apache-2.0
#


import json
import logging
from pathlib import Path
from typing import Dict, Any
from .experiment_analysis import ExperimentAnalyzer
from .visualization import ExperimentVisualizer, SummaryWriter
from .output_parsers import create_parser


class ExperimentOrchestrator:
    """
    Orchestrates the complete experiment analysis workflow.

    This class coordinates the entire post-processing pipeline:
    1. Parse experiment outputs (metrics, precompute data, monitoring logs)
    2. Aggregate results (calculate throughput, CPU, memory, accuracy metrics)
    3. Generate comparisons (create pairwise or 3-way algorithm comparisons)
    4. Create visualizations (performance plots, memory analysis)
    5. Write summaries (text reports with all statistics)
    """

    def __init__(
        self,
        experiments_dir: str,
        output_dir: str,
        sketch_type: str,
        compare_num: int = 2,
        memory_source: str = None,
    ):
        """
        Initialize the experiment orchestrator.

        Args:
            experiments_dir: Directory containing experiment output folders
            output_dir: Directory where analysis results will be written
            sketch_type: Type of sketch (freq, quantile, topk, cardinality)
            compare_num: Number of algorithms to compare (2=pairwise, 3=baseline vs datasketches vs custom)
            memory_source: Source for memory data ('none', 'monitor', or 'sink') - REQUIRED
        """
        if memory_source is None:
            raise ValueError(
                "memory_source is required and must be one of: 'none', 'monitor', 'sink'"
            )

        self.experiments_dir = experiments_dir
        self.output_dir = Path(output_dir)
        self.sketch_type = sketch_type
        self.compare_num = compare_num
        self.memory_source = memory_source

        # Initialize analysis components
        self.analyzer = ExperimentAnalyzer(
            sketch_type
        )  # Calculates metrics and statistics
        self.visualizer = ExperimentVisualizer()  # Creates plots and charts
        self.summary_writer = SummaryWriter()  # Writes text summaries

        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
        print(f"\nPost-processing {sketch_type} experiments")
        print(f"Input:  {self.experiments_dir}")
        print(f"Output: {self.output_dir}")
        print(f"Memory Source: {self.memory_source}")

    def run_global_analysis(self) -> None:
        """
        Execute global cross-query analysis workflow.

        Creates only cross-query visualizations that compare ALL query types:
        - memory_per_query_type.png: Shows memory usage across different query types
        """
        # Step 1: Parse all experiments (no filtering by query type)
        print("\n[1/2] Parsing all experiment outputs...")
        results = self._parse_experiment_output(
            self.experiments_dir, self.sketch_type, filter_by_query_type=False
        )
        print(f"      ✓ Found {len(results)} experiments")

        # Step 2: Analyze and aggregate metrics (throughput, CPU, memory, accuracy)
        print("\n[2/2] Creating global cross-query visualizations...")
        summary = self._aggregate_results(results)

        # Step 3: Create global memory per query type comparison plot
        self._create_memory_per_query_plot(summary)

        print(f"\n✓ Global analysis complete! Results saved to: {self.output_dir}\n")

    def run_analysis(self, filter_by_query_type: bool = True) -> None:
        """
        Execute the complete experiment analysis workflow.

        Args:
            filter_by_query_type: If True, only process experiments matching self.sketch_type.
                                  If False, process all experiments (for cross-query analysis).

        Pipeline:
        1. Parse experiments: Load output files, monitoring data, and precompute results
        2. Aggregate results: Calculate throughput, CPU, memory, and accuracy metrics
        3. Generate comparisons: Create pairwise or 3-way algorithm comparisons with plots
        4. Create overall summary: Write consolidated text report for all experiments
        5. Create visualizations: Memory vs cardinality plots
        6. Create visualizations: Throughput vs CPU cores plots
        """
        # Step 1: Parse and load data from experiment folders
        results = self._parse_experiments(filter_by_query_type)

        # Step 2: Analyze and aggregate metrics (throughput, CPU, memory, accuracy)
        summary = self._aggregate_results(results)

        # Step 3: Generate comparison plots and summaries
        self._generate_comparisons(results, summary)

        # Step 4: Create overall summary text file
        self._create_overall_summary(summary)

        # Step 5: Create memory vs cardinality scaling plots
        self._create_memory_vs_cardinality_plot(results, summary)

        # Step 6: Create throughput vs CPU cores scaling plots
        self._create_throughput_vs_cpu_plot(summary)

        # Step 7: Create memory vs tumbling window scaling plots
        self._create_memory_vs_window_plot(results, summary)

        # Step 8: Create memory vs items per window scaling plots
        self._create_memory_vs_items_per_window_plot(results, summary)

        print(f"\n✓ Post-processing complete! Results saved to: {self.output_dir}\n")

    def _parse_experiments(
        self, filter_by_query_type: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        """Parse experiment outputs and return structured data."""
        query_desc = self.sketch_type if filter_by_query_type else "all query types"
        print(f"\n[1/5] Parsing {query_desc} experiment outputs...")
        results = self._parse_experiment_output(
            self.experiments_dir, self.sketch_type, filter_by_query_type
        )
        print(f"      ✓ Found {len(results)} experiments")
        return results

    def _parse_experiment_output(
        self,
        experiments_dir: str,
        sketch_type: str = "freq",
        filter_by_query_type: bool = True,
    ) -> Dict[str, Dict[str, Any]]:
        """Parse experiment output using the appropriate parser."""
        from pathlib import Path

        results = {}
        experiments_path = Path(experiments_dir)

        # Filter experiment folders based on filter_by_query_type setting
        experiment_folders = []
        for d in experiments_path.iterdir():
            if d.is_dir() and d.name.startswith("experiment"):
                if filter_by_query_type:
                    # Parse folder name: experiment_{id}_{query_type}_{algorithm}_...
                    parts = d.name.split("_")
                    if len(parts) >= 3 and parts[2] == sketch_type:
                        experiment_folders.append(d)
                else:
                    # Include all experiment folders (for cross-query analysis)
                    experiment_folders.append(d)

        # Create parser based on sketch type
        parser = create_parser(sketch_type, experiments_dir)
        precompute_results = parser.parse_experiment_output()

        for folder in experiment_folders:
            folder_name = folder.name

            # Determine memory data source based on memory_source setting
            if self.memory_source == "monitor":
                # Memory will be populated from job_monitor.json
                initial_memory_data = []
            elif self.memory_source == "sink":
                # Memory comes from Flink sink output
                initial_memory_data = precompute_results.get(folder_name, {}).get(
                    "memory_data", []
                )
            else:  # memory_source == "none"
                # No memory data
                initial_memory_data = []

            results[folder_name] = {
                "throughput_in_data": {},
                "throughput_out_data": {},
                "cpu_usage_data": [],
                "backpressure_data": {},
                "source_backpressure_averages": {},
                "overall_avg_backpressure": 0.0,
                "precompute_hashmaps": precompute_results.get(folder_name, {}).get(
                    "precompute_data", []
                ),
                "memory_data": initial_memory_data,
                "experiment_metadata": precompute_results.get(folder_name, {}).get(
                    "experiment_metadata", {}
                ),
            }

            # Parse monitoring data
            monitoring_file = folder / "monitoring" / "job_monitor.json"
            if monitoring_file.exists():
                monitoring_data = self._parse_monitoring_data(str(monitoring_file))
                results[folder_name]["throughput_in_data"] = monitoring_data[
                    "throughput_in_data"
                ]
                results[folder_name]["throughput_out_data"] = monitoring_data[
                    "throughput_out_data"
                ]
                results[folder_name]["cpu_usage_data"] = monitoring_data[
                    "cpu_usage_data"
                ]
                results[folder_name]["backpressure_data"] = monitoring_data[
                    "backpressure_data"
                ]
                results[folder_name]["source_backpressure_averages"] = monitoring_data[
                    "source_backpressure_averages"
                ]
                results[folder_name]["overall_avg_backpressure"] = monitoring_data[
                    "overall_avg_backpressure"
                ]

                # Populate memory from monitor if memory_source is "monitor"
                if self.memory_source == "monitor":
                    jvm_heap_data = monitoring_data.get("jvm_heap_memory_data", [])
                    if jvm_heap_data:
                        # Take max JVM heap usage across the monitoring period
                        heap_values = [
                            entry["jvm_heap_used_mb"] for entry in jvm_heap_data
                        ]
                        max_heap_mb = max(heap_values) if heap_values else 0.0
                        # Convert MB to bytes to match expected format
                        max_heap_bytes = max_heap_mb * 1024 * 1024
                        # Store as memory_data in same format as sink output
                        results[folder_name]["memory_data"] = [
                            {"memory_bytes": max_heap_bytes}
                        ]

        return results

    def _parse_monitoring_data(self, monitoring_file: str) -> Dict[str, Any]:
        """
        Parse monitoring data from job_monitor.json file.

        The job monitor collects metrics from Flink REST API during job execution:
        - Throughput (records in/out per second) for each operator vertex
        - CPU usage (percentage) from TaskManager
        - Backpressure ratio (0.0 to 1.0) indicating if operators are bottlenecked

        Returns structured data organized by metric type and vertex.
        """
        # Initialize data structures for different metric types
        throughput_in_data = {}  # {vertex_name: [{timestamp, throughput}]}
        throughput_out_data = {}  # {vertex_name: [{timestamp, throughput}]}
        cpu_usage_data = []  # [{timestamp, cpu_usage}]
        backpressure_data = {}  # {vertex_name: [{timestamp, backpressure_ratio}]}
        jvm_heap_memory_data = []  # [{timestamp, jvm_heap_used_mb}]

        with open(monitoring_file, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    record = json.loads(line)

                    # Skip records that don't have metric data (e.g., status messages)
                    if "message" in record:
                        continue

                    vertex_name = record.get("vertex_name")

                    # Initialize lists only if vertex hasn't been seen before
                    if vertex_name not in throughput_in_data:
                        throughput_in_data[vertex_name] = []
                        throughput_out_data[vertex_name] = []
                        backpressure_data[vertex_name] = []

                    timestamp = record["metric_timestamp"]

                    # Collect throughput metrics (records/sec flowing into this vertex)
                    throughput_in_data[vertex_name].append(
                        {
                            "timestamp": timestamp,
                            "throughput": record["num_records_in_per_second"],
                        }
                    )

                    # Collect output throughput (records/sec flowing out of this vertex)
                    throughput_out_data[vertex_name].append(
                        {
                            "timestamp": timestamp,
                            "throughput": record["num_records_out_per_second"],
                        }
                    )

                    # Collect backpressure data (0.0 = no pressure, 1.0 = fully blocked)
                    backpressure_data[vertex_name].append(
                        {
                            "timestamp": timestamp,
                            "backpressure_ratio": record.get("backpressure_ratio", 0.0),
                        }
                    )

                    # Collect CPU usage data (same for all vertices since it's TaskManager-wide)
                    cpu_usage_data.append(
                        {
                            "timestamp": timestamp,
                            "cpu_usage": record["cpu_usage_percent"],
                        }
                    )

                    # Collect JVM heap memory data (same for all vertices since it's TaskManager-wide)
                    if "jvm_heap_used_mb" in record:
                        jvm_heap_memory_data.append(
                            {
                                "timestamp": timestamp,
                                "jvm_heap_used_mb": record["jvm_heap_used_mb"],
                            }
                        )

        # Calculate average backpressure for data source vertices
        # High backpressure at sources indicates downstream operators are slow
        source_backpressure_averages = {}
        for vertex_name, bp_data in backpressure_data.items():
            # Check if vertex is a data source (typically contains "Source" or "Generator")
            if "Source" in vertex_name or "Generator" in vertex_name:
                bp_ratios = [entry["backpressure_ratio"] for entry in bp_data]
                if bp_ratios:
                    source_backpressure_averages[vertex_name] = sum(bp_ratios) / len(
                        bp_ratios
                    )

        # Calculate overall average backpressure across all data sources
        if source_backpressure_averages:
            overall_avg_backpressure = sum(source_backpressure_averages.values()) / len(
                source_backpressure_averages
            )
        else:
            overall_avg_backpressure = 0.0

        return {
            "throughput_in_data": throughput_in_data,
            "throughput_out_data": throughput_out_data,
            "cpu_usage_data": cpu_usage_data,
            "backpressure_data": backpressure_data,
            "source_backpressure_averages": source_backpressure_averages,
            "overall_avg_backpressure": overall_avg_backpressure,
            "jvm_heap_memory_data": jvm_heap_memory_data,
        }

    def _aggregate_results(
        self, results: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """Aggregate experiment results with performance and accuracy metrics."""
        print("\n[2/5] Analyzing and aggregating results...")
        return self.analyzer.aggregate_experiment_results(results)

    def _generate_comparisons(
        self, results: Dict[str, Dict[str, Any]], summary: Dict[str, Dict[str, Any]]
    ) -> None:
        """Generate comparisons and visualizations based on compare_num setting."""
        if self.compare_num == 2:
            # Pairwise comparison: group by query type and cardinality, then pair experiments
            pairs = self.analyzer.create_experiment_pairs(results)
            print(f"\n[3/5] Generating {len(pairs)} pair comparisons...")
            self._process_experiment_pairs(pairs, summary)
        else:  # compare_num == 3
            # 3-way comparison: baseline vs datasketches vs custom
            print("\n[3/5] Generating 3-way comparisons...")
            self._generate_three_way_comparison(results, summary)

    def _generate_three_way_comparison(
        self, results: Dict[str, Dict[str, Any]], summary: Dict[str, Dict[str, Any]]
    ) -> None:
        """Generate 3-way comparison grouped by query type with all 3 algorithms."""
        # Group experiments by query type, then by algorithm
        query_groups = {}
        for exp_name, exp_data in results.items():
            parts = exp_name.split("_")
            if len(parts) < 4:
                continue
            query_type = parts[2]  # freq, quantile, topk, etc.
            algorithm = parts[3]  # baseline, datasketches, custom

            if query_type not in query_groups:
                query_groups[query_type] = {
                    "baseline": [],
                    "datasketches": [],
                    "custom": [],
                }
            query_groups[query_type][algorithm].append((exp_name, exp_data))

        # Process each query type separately
        for query_type in sorted(query_groups.keys()):
            algo_groups = query_groups[query_type]
            baseline_exps = sorted(algo_groups["baseline"])
            datasketches_exps = sorted(algo_groups["datasketches"])
            custom_exps = sorted(algo_groups["custom"])

            # Generate comparisons for this query type
            for i, (
                (baseline_name, baseline_data),
                (datasketches_name, datasketches_data),
                (custom_name, custom_data),
            ) in enumerate(zip(baseline_exps, datasketches_exps, custom_exps)):
                # Use the same naming convention as pair comparisons but include all 3
                pair_name = f"pair_{i+1}_{baseline_name}_vs_{datasketches_name}_vs_{custom_name}"

                # Silently process - no logging needed for each pair

                # Create 3-way visualization with original pair naming
                visualization_file = self.output_dir / f"{pair_name}.png"
                self.visualizer.visualize_three_way_comparison(
                    baseline_name,
                    baseline_data,
                    datasketches_name,
                    datasketches_data,
                    custom_name,
                    custom_data,
                    str(visualization_file),
                )

                # Create 3-way summary with original pair naming
                summary_file = self.output_dir / f"{pair_name}_summary.txt"
                baseline_stats = summary.get(baseline_name, {})
                datasketches_stats = summary.get(datasketches_name, {})
                custom_stats = summary.get(custom_name, {})

                self.summary_writer.write_three_way_summary(
                    baseline_name,
                    baseline_stats,
                    datasketches_name,
                    datasketches_stats,
                    custom_name,
                    custom_stats,
                    str(summary_file),
                )

                # Memory over time plot removed - not needed for individual pairs

    def _process_experiment_pairs(
        self, pairs, summary: Dict[str, Dict[str, Any]]
    ) -> None:
        """Process each experiment pair to generate visualizations and summaries."""
        pair_processor = ExperimentPairProcessor(
            self.output_dir, self.visualizer, self.summary_writer, self.experiments_dir
        )

        for i, pair in enumerate(pairs):
            pair_processor.process_pair(i + 1, pair, summary)

    def _create_overall_summary(self, summary: Dict[str, Dict[str, Any]]) -> None:
        """Create the overall experiment summary report."""
        print("\n[4/5] Writing overall summary...")
        sorted_summary = dict(
            sorted(
                summary.items(),
                key=lambda item: self.analyzer._experiment_sort_key(item[0]),
            )
        )
        overall_summary_file = self.output_dir / "overall_summary.txt"
        self.summary_writer.write_overall_summary(
            sorted_summary, str(overall_summary_file)
        )
        print(f"      ✓ {overall_summary_file}")

    def _create_memory_per_query_plot(self, summary: Dict[str, Dict[str, Any]]) -> None:
        """Create global memory per query type comparison plot."""
        print("\n[5/5] Creating visualizations...")
        memory_plot_file = self.output_dir / "memory_per_query_type.png"
        self.visualizer.visualize_memory_per_query(summary, str(memory_plot_file))
        print(f"      ✓ {memory_plot_file}")

    @staticmethod
    def _parse_cardinality_from_folder(folder_name: str) -> int:
        """
        Extract cardinality from experiment folder name.

        Expected format: experiment_N_query_algorithm_resource_value_cardinality_C
        Returns -1 for infinite cardinality, or the cardinality value.
        """
        parts = folder_name.split("_")
        try:
            # Find the index of 'cardinality' keyword
            if "cardinality" in parts:
                cardinality_idx = parts.index("cardinality")
                if cardinality_idx + 1 < len(parts):
                    cardinality = int(parts[cardinality_idx + 1])
                    return cardinality
        except (ValueError, IndexError):
            logging.warning(
                f"Could not parse cardinality from folder name: {folder_name}"
            )
        return None

    @staticmethod
    def _parse_algorithm_from_folder(folder_name: str) -> str:
        """
        Extract algorithm type from experiment folder name.

        Expected format: experiment_N_query_algorithm_resource_value_cardinality_C
        """
        parts = folder_name.split("_")
        try:
            # The algorithm is typically after the query name
            # Format: experiment_N_query_algorithm_...
            if len(parts) >= 4:
                return parts[3]  # Algorithm is the 4th part (index 3)
        except (ValueError, IndexError):
            logging.warning(
                f"Could not parse algorithm from folder name: {folder_name}"
            )
        return None

    @staticmethod
    def _parse_cpu_from_folder(folder_name: str) -> int:
        """
        Extract CPU cores from experiment folder name.

        Expected format: experiment_N_query_algorithm_cpu_C_...
        Returns the number of CPU cores, or None if not found.
        """
        parts = folder_name.split("_")
        try:
            # Find the index of 'cpu' keyword
            if "cpu" in parts:
                cpu_idx = parts.index("cpu")
                if cpu_idx + 1 < len(parts):
                    cpu_cores = int(
                        float(parts[cpu_idx + 1])
                    )  # Handle both int and float (e.g., 1.0)
                    return cpu_cores
        except (ValueError, IndexError):
            logging.warning(
                f"Could not parse CPU cores from folder name: {folder_name}"
            )
        return None

    @staticmethod
    def _parse_window_from_folder(folder_name: str) -> int:
        """
        Extract tumbling window size from experiment folder name.

        Expected format: ...window_SIZE
        Returns the window size in seconds, or None if not found.
        """
        parts = folder_name.split("_")
        try:
            # Find the index of 'window' keyword
            if "window" in parts:
                window_idx = parts.index("window")
                if window_idx + 1 < len(parts):
                    window_size = int(parts[window_idx + 1])
                    return window_size
        except (ValueError, IndexError):
            logging.warning(
                f"Could not parse window size from folder name: {folder_name}"
            )
        return None

    @staticmethod
    def _parse_items_per_window_from_folder(folder_name: str) -> int:
        """
        Extract items per window from experiment folder name.

        Expected format: ...itemsWindow_N
        Returns the number of items per window, or None if not found.
        """
        parts = folder_name.split("_")
        try:
            # Find the index of 'itemsWindow' keyword
            if "itemsWindow" in parts:
                items_idx = parts.index("itemsWindow")
                if items_idx + 1 < len(parts):
                    items_per_window = int(parts[items_idx + 1])
                    return items_per_window
        except (ValueError, IndexError):
            logging.warning(
                f"Could not parse items per window from folder name: {folder_name}"
            )
        return None

    def _create_memory_vs_cardinality_plot(
        self, results: Dict[str, Dict[str, Any]], summary: Dict[str, Dict[str, Any]]
    ) -> None:
        """
        Create memory vs cardinality plots per query type, comparing baseline, datasketches, and custom.

        Groups experiments by query type, then by cardinality and algorithm.
        """
        # Group experiments by query type, then cardinality and algorithm
        # Structure: {query_type: {cardinality: {algorithm: avg_memory}}}
        # Also collect accuracy data: {query_type: {cardinality: {algorithm: accuracy_error_percent}}}
        query_cardinality_data = {}
        query_accuracy_data = {}

        for folder_name, exp_data in results.items():
            parts = folder_name.split("_")
            if len(parts) < 3:
                continue

            query_type = parts[2]  # freq, quantile, etc.
            cardinality = self._parse_cardinality_from_folder(folder_name)
            algorithm = self._parse_algorithm_from_folder(folder_name)

            if cardinality is None or algorithm is None:
                continue

            # Get memory data from experiment
            memory_data = exp_data.get("memory_data", [])
            if not memory_data:
                continue

            # Calculate max memory usage
            max_memory = max(m.get("memory_bytes", 0) for m in memory_data)

            # Initialize nested structure for memory
            if query_type not in query_cardinality_data:
                query_cardinality_data[query_type] = {}
            if cardinality not in query_cardinality_data[query_type]:
                query_cardinality_data[query_type][cardinality] = {}

            # Store algorithm memory
            query_cardinality_data[query_type][cardinality][algorithm] = max_memory

            # Get accuracy data from summary (only for non-baseline algorithms)
            if algorithm != "baseline" and folder_name in summary:
                exp_summary = summary[folder_name]
                accuracy_metrics = exp_summary.get("accuracy_metrics", {})

                # Extract the appropriate accuracy metric
                accuracy_value = None
                if "avg_accuracy_percent" in accuracy_metrics:
                    # Use 1 - relative_error (higher is better)
                    accuracy_value = accuracy_metrics["avg_accuracy_percent"]
                elif "avg_precision_percent" in accuracy_metrics:
                    # For TopK, use precision instead
                    accuracy_value = accuracy_metrics["avg_precision_percent"]

                if accuracy_value is not None:
                    # Initialize nested structure for accuracy
                    if query_type not in query_accuracy_data:
                        query_accuracy_data[query_type] = {}
                    if cardinality not in query_accuracy_data[query_type]:
                        query_accuracy_data[query_type][cardinality] = {}

                    # Store accuracy value
                    query_accuracy_data[query_type][cardinality][
                        algorithm
                    ] = accuracy_value

        if not query_cardinality_data:
            logging.warning("No cardinality data found for memory vs cardinality plot")
            return

        # Create a plot for each query type
        for query_type, cardinality_data in query_cardinality_data.items():
            memory_cardinality_file = (
                self.output_dir / f"memory_vs_cardinality_{query_type}.png"
            )

            # Get corresponding accuracy data for this query type
            accuracy_data = query_accuracy_data.get(query_type, {})

            self.visualizer.visualize_memory_vs_cardinality(
                cardinality_data,
                str(memory_cardinality_file),
                query_type,
                accuracy_data,
            )
            print(f"      ✓ {memory_cardinality_file}")

    def _create_throughput_vs_cpu_plot(
        self, summary: Dict[str, Dict[str, Any]]
    ) -> None:
        """
        Create throughput vs CPU cores plots per query type, comparing baseline, datasketches, and custom.

        Groups experiments by query type, then by CPU cores and algorithm.
        """
        # Group experiments by query type, then CPU cores and algorithm
        # Structure: {query_type: {cpu_cores: {algorithm: avg_throughput}}}
        query_cpu_data = {}

        for folder_name, exp_stats in summary.items():
            parts = folder_name.split("_")
            if len(parts) < 3:
                continue

            query_type = parts[2]  # freq, quantile, etc.
            cpu_cores = self._parse_cpu_from_folder(folder_name)
            algorithm = self._parse_algorithm_from_folder(folder_name)

            if cpu_cores is None or algorithm is None:
                continue

            # Get throughput data from experiment summary
            # Use average throughput in (input throughput)
            avg_throughput_in = exp_stats.get("avg_throughput_in", 0)
            if avg_throughput_in == 0:
                continue

            # Initialize nested structure
            if query_type not in query_cpu_data:
                query_cpu_data[query_type] = {}
            if cpu_cores not in query_cpu_data[query_type]:
                query_cpu_data[query_type][cpu_cores] = {}

            # Store algorithm throughput
            query_cpu_data[query_type][cpu_cores][algorithm] = avg_throughput_in

        if not query_cpu_data:
            logging.warning("No CPU data found for throughput vs CPU cores plot")
            return

        # Create a plot for each query type
        for query_type, cpu_data in query_cpu_data.items():
            throughput_cpu_file = (
                self.output_dir / f"throughput_vs_cpu_{query_type}.png"
            )
            self.visualizer.visualize_throughput_vs_cpu(
                cpu_data, str(throughput_cpu_file), query_type
            )
            print(f"      ✓ {throughput_cpu_file}")

    def _create_memory_vs_window_plot(
        self, results: Dict[str, Dict[str, Any]], summary: Dict[str, Dict[str, Any]]
    ) -> None:
        """
        Create memory vs tumbling window size plots per query type, comparing baseline, datasketches, and custom.

        Groups experiments by query type, then by window size and algorithm.
        """
        # Group experiments by query type, then window size and algorithm
        # Structure: {query_type: {window_size: {algorithm: avg_memory}}}
        # Also collect accuracy data: {query_type: {window_size: {algorithm: accuracy_error_percent}}}
        query_window_data = {}
        query_window_accuracy_data = {}

        for folder_name, exp_data in results.items():
            parts = folder_name.split("_")
            if len(parts) < 3:
                continue

            query_type = parts[2]  # freq, quantile, etc.
            window_size = self._parse_window_from_folder(folder_name)
            algorithm = self._parse_algorithm_from_folder(folder_name)

            if window_size is None or algorithm is None:
                continue

            # Get memory data from experiment
            memory_data = exp_data.get("memory_data", [])
            if not memory_data:
                continue

            # Calculate max memory usage
            max_memory = max(m.get("memory_bytes", 0) for m in memory_data)

            # Initialize nested structure for memory
            if query_type not in query_window_data:
                query_window_data[query_type] = {}
            if window_size not in query_window_data[query_type]:
                query_window_data[query_type][window_size] = {}

            # Store algorithm memory
            query_window_data[query_type][window_size][algorithm] = max_memory

            # Get accuracy data from summary (only for non-baseline algorithms)
            if algorithm != "baseline" and folder_name in summary:
                exp_summary = summary[folder_name]
                accuracy_metrics = exp_summary.get("accuracy_metrics", {})

                # Extract the appropriate accuracy metric
                accuracy_value = None
                if "avg_accuracy_percent" in accuracy_metrics:
                    # Use 1 - relative_error (higher is better)
                    accuracy_value = accuracy_metrics["avg_accuracy_percent"]
                elif "avg_precision_percent" in accuracy_metrics:
                    # For TopK, use precision instead
                    accuracy_value = accuracy_metrics["avg_precision_percent"]

                if accuracy_value is not None:
                    # Initialize nested structure for accuracy
                    if query_type not in query_window_accuracy_data:
                        query_window_accuracy_data[query_type] = {}
                    if window_size not in query_window_accuracy_data[query_type]:
                        query_window_accuracy_data[query_type][window_size] = {}

                    # Store accuracy value
                    query_window_accuracy_data[query_type][window_size][
                        algorithm
                    ] = accuracy_value

        if not query_window_data:
            logging.warning("No window size data found for memory vs window plot")
            return

        # Create a plot for each query type
        for query_type, window_data in query_window_data.items():
            memory_window_file = self.output_dir / f"memory_vs_window_{query_type}.png"

            # Get corresponding accuracy data for this query type
            accuracy_data = query_window_accuracy_data.get(query_type, {})

            self.visualizer.visualize_memory_vs_window(
                window_data, str(memory_window_file), query_type, accuracy_data
            )
            print(f"      ✓ {memory_window_file}")

    def _create_memory_vs_items_per_window_plot(
        self, results: Dict[str, Dict[str, Any]], summary: Dict[str, Dict[str, Any]]
    ) -> None:
        """
        Create memory vs items per window plots per query type, comparing baseline, datasketches, and custom.

        Groups experiments by query type, then by items per window and algorithm.
        """
        # Group experiments by query type, then items per window and algorithm
        # Structure: {query_type: {items_per_window: {algorithm: max_memory}}}
        # Also collect accuracy data: {query_type: {items_per_window: {algorithm: accuracy_error_percent}}}
        query_items_data = {}
        query_items_accuracy_data = {}

        for folder_name, exp_data in results.items():
            parts = folder_name.split("_")
            if len(parts) < 3:
                continue

            query_type = parts[2]  # freq, quantile, etc.
            items_per_window = self._parse_items_per_window_from_folder(folder_name)
            algorithm = self._parse_algorithm_from_folder(folder_name)

            if items_per_window is None or algorithm is None:
                continue

            # Get memory data from experiment
            memory_data = exp_data.get("memory_data", [])
            if not memory_data:
                continue

            # Calculate max memory usage
            max_memory = max(m.get("memory_bytes", 0) for m in memory_data)

            # Initialize nested structure for memory
            if query_type not in query_items_data:
                query_items_data[query_type] = {}
            if items_per_window not in query_items_data[query_type]:
                query_items_data[query_type][items_per_window] = {}

            # Store algorithm memory
            query_items_data[query_type][items_per_window][algorithm] = max_memory

            # Get accuracy data from summary (only for non-baseline algorithms)
            if algorithm != "baseline" and folder_name in summary:
                exp_summary = summary[folder_name]
                accuracy_metrics = exp_summary.get("accuracy_metrics", {})

                # Extract the appropriate accuracy metric
                accuracy_value = None
                if "avg_accuracy_percent" in accuracy_metrics:
                    # Use 1 - relative_error (higher is better)
                    accuracy_value = accuracy_metrics["avg_accuracy_percent"]
                elif "avg_precision_percent" in accuracy_metrics:
                    # For TopK, use precision instead
                    accuracy_value = accuracy_metrics["avg_precision_percent"]

                if accuracy_value is not None:
                    # Initialize nested structure for accuracy
                    if query_type not in query_items_accuracy_data:
                        query_items_accuracy_data[query_type] = {}
                    if items_per_window not in query_items_accuracy_data[query_type]:
                        query_items_accuracy_data[query_type][items_per_window] = {}

                    # Store accuracy value
                    query_items_accuracy_data[query_type][items_per_window][
                        algorithm
                    ] = accuracy_value

        if not query_items_data:
            logging.warning(
                "No items per window data found for memory vs items per window plot"
            )
            return

        # Create a plot for each query type
        for query_type, items_data in query_items_data.items():
            memory_items_file = (
                self.output_dir / f"memory_vs_items_per_window_{query_type}.png"
            )

            # Get corresponding accuracy data for this query type
            accuracy_data = query_items_accuracy_data.get(query_type, {})

            self.visualizer.visualize_memory_vs_items_per_window(
                items_data, str(memory_items_file), query_type, accuracy_data
            )
            print(f"      ✓ {memory_items_file}")


class ExperimentPairProcessor:
    """Handles processing of individual experiment pairs."""

    def __init__(
        self,
        output_dir: Path,
        visualizer: ExperimentVisualizer,
        summary_writer: SummaryWriter,
        experiments_dir: str,
    ):
        self.output_dir = output_dir
        self.visualizer = visualizer
        self.summary_writer = summary_writer
        self.experiments_dir = experiments_dir

    def process_pair(
        self, pair_number: int, pair_data, summary: Dict[str, Dict[str, Any]]
    ) -> None:
        """
        Process a single experiment pair.

        Creates comparison visualization and summary report for two experiments.
        Typically compares baseline vs sketch algorithm at same resource level.
        """
        (exp1_name, exp1_data), (exp2_name, exp2_data) = pair_data
        pair_name = f"pair_{pair_number}_{exp1_name}_vs_{exp2_name}"

        # Create side-by-side visualization comparing throughput, CPU, backpressure
        self._create_visualization(
            pair_name, exp1_name, exp1_data, exp2_name, exp2_data
        )

        # Write text summary with statistics and accuracy metrics
        self._create_pair_summary(pair_name, exp1_name, exp2_name, summary)

    def _create_visualization(
        self,
        pair_name: str,
        exp1_name: str,
        exp1_data: Dict[str, Any],
        exp2_name: str,
        exp2_data: Dict[str, Any],
    ) -> None:
        """Create visualization for the experiment pair."""
        visualization_file = self.output_dir / f"{pair_name}.png"
        self.visualizer.visualize_experiment_pair(
            exp1_name, exp1_data, exp2_name, exp2_data, str(visualization_file)
        )
        logging.debug(f"Saved visualization: {visualization_file}")

    def _create_pair_summary(
        self,
        pair_name: str,
        exp1_name: str,
        exp2_name: str,
        summary: Dict[str, Dict[str, Any]],
    ) -> None:
        """Create summary report for the experiment pair."""
        summary_file = self.output_dir / f"{pair_name}_summary.txt"
        exp1_stats = summary.get(exp1_name, {})
        exp2_stats = summary.get(exp2_name, {})

        self.summary_writer.write_pair_summary(
            pair_name, exp1_name, exp1_stats, exp2_name, exp2_stats, str(summary_file)
        )
        logging.debug(f"Wrote summary file: {summary_file}")


class ConfigurationManager:
    """Manages configuration and setup for the experiment analysis."""

    @staticmethod
    def setup_logging() -> None:
        """Configure logging for the application."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[logging.StreamHandler()],
        )

    @staticmethod
    def detect_query_types(experiments_dir: str) -> list:
        """
        Auto-detect query types from experiment folder names.

        Args:
            experiments_dir: Directory containing experiment folders

        Returns:
            List of unique query types found (e.g., ['freq', 'quantile'])
        """
        from pathlib import Path

        experiments_path = Path(experiments_dir)
        query_types = set()

        # Scan all experiment folders
        for folder in experiments_path.iterdir():
            if folder.is_dir() and folder.name.startswith("experiment"):
                # Parse folder name: experiment_{id}_{query_type}_{algorithm}_...
                parts = folder.name.split("_")
                if len(parts) >= 3:
                    query_type = parts[2]  # query_type is at index 2
                    query_types.add(query_type)

        return sorted(list(query_types))

    @staticmethod
    def parse_arguments():
        """Parse and return command line arguments."""
        import argparse

        parser = argparse.ArgumentParser(
            description="Parse and aggregate experiment results"
        )
        parser.add_argument(
            "--experiments_dir", required=True, help="Path to experiments directory"
        )
        parser.add_argument(
            "--output_dir",
            required=True,
            help="Directory to store visualizations and output files",
        )
        parser.add_argument(
            "--compare_num",
            type=int,
            required=True,
            choices=[2, 3],
            help="Number of experiments to compare (2: pairwise, 3: compare datasketches and custom to baseline)",
        )
        parser.add_argument(
            "--memory-source",
            type=str,
            required=True,
            choices=["none", "monitor", "sink"],
            help="Source for memory data: 'none' (no memory analysis), 'monitor' (from job_monitor.json), 'sink' (from flink output)",
        )

        return parser.parse_args()
