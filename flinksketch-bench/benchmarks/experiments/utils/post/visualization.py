#!/usr/bin/env python3
#
# Copyright 2025 ProjectASAP contributors
# SPDX-License-Identifier: Apache-2.0
#


import matplotlib.pyplot as plt
import numpy as np
from typing import Dict, Any
from .experiment_analysis import ThroughputCalculator
from .memory_visualization import MemoryVisualizer

# Set up matplotlib for presentation-quality plots
plt.rcParams.update(
    {
        "font.size": 12,
        "axes.labelsize": 15,
        "axes.titlesize": 18,
        "xtick.labelsize": 13,
        "ytick.labelsize": 13,
        "legend.fontsize": 12,
        "legend.title_fontsize": 13,
        "figure.titlesize": 18,
        "lines.linewidth": 2.5,
        "lines.markersize": 6,
        "axes.grid": False,
        "figure.facecolor": "white",
        "axes.facecolor": "#f8f9fa",
        "savefig.dpi": 300,
        "savefig.bbox": "tight",
    }
)


class ExperimentVisualizer:
    """Class responsible for creating experiment visualizations."""

    def __init__(self):
        self.throughput_calc = ThroughputCalculator()

    def visualize_experiment_pair(
        self,
        exp1_name: str,
        exp1_data: Dict[str, Any],
        exp2_name: str,
        exp2_data: Dict[str, Any],
        output_file: str,
    ):
        """Create a visualization comparing two experiments."""
        plt.figure(figsize=(20, 8))  # Made much wider for better x-axis visibility

        # Define colors for each experiment
        exp1_color = "#2E86AB"  # Deep blue
        exp2_color = "#E63946"  # Vibrant red

        # Calculate start times for each experiment
        exp1_start_time = self.throughput_calc.get_experiment_start_time(exp1_data)
        exp2_start_time = self.throughput_calc.get_experiment_start_time(exp2_data)

        # Plot experiments with single color per experiment
        self._plot_experiment_throughput_single_color(
            exp1_name,
            exp1_data,
            exp1_start_time,
            exp1_color,
            linestyle_in="-",
            linestyle_out="--",
            alpha=0.9,
        )
        self._plot_experiment_throughput_single_color(
            exp2_name,
            exp2_data,
            exp2_start_time,
            exp2_color,
            linestyle_in="-",
            linestyle_out="--",
            alpha=0.9,
        )

        # Finalize plot
        self._finalize_plot(exp1_name, exp2_name, output_file)

    def _collect_all_vertices(
        self, exp1_data: Dict[str, Any], exp2_data: Dict[str, Any]
    ) -> set:
        """Collect all unique vertices from both experiments."""
        all_vertices = set()
        for exp_data in [exp1_data, exp2_data]:
            all_vertices.update(exp_data.get("throughput_in_data", {}).keys())
            all_vertices.update(exp_data.get("throughput_out_data", {}).keys())
        return all_vertices

    def _assign_vertex_colors(self, all_vertices: set, colors) -> Dict[str, Any]:
        """Assign colors to vertices."""
        vertex_colors = {}
        for color_idx, vertex in enumerate(sorted(all_vertices)):
            vertex_colors[vertex] = colors[color_idx % len(colors)]
        return vertex_colors

    def _plot_experiment_throughput_single_color(
        self,
        exp_name: str,
        exp_data: Dict[str, Any],
        start_time: float,
        color: str,
        linestyle_in: str,
        linestyle_out: str,
        alpha: float = 1.0,
    ):
        """Plot throughput data for a single experiment with a single color."""
        # Plot IN throughput
        self._plot_throughput_direction_single_color(
            exp_name,
            exp_data.get("throughput_in_data", {}),
            start_time,
            color,
            "In",
            linestyle_in,
            alpha,
        )

        # Plot OUT throughput
        self._plot_throughput_direction_single_color(
            exp_name,
            exp_data.get("throughput_out_data", {}),
            start_time,
            color,
            "Out",
            linestyle_out,
            alpha,
        )

    def _plot_throughput_direction_single_color(
        self,
        exp_name: str,
        throughput_data: Dict[str, Any],
        start_time: float,
        color: str,
        direction: str,
        linestyle: str,
        alpha: float,
    ):
        """Plot throughput data for a specific direction (In/Out) with single color."""
        for vertex_name, measurements in throughput_data.items():
            if measurements:  # Check if measurements exist
                relative_times, throughputs = (
                    self.throughput_calc.prepare_throughput_data(
                        measurements, start_time
                    )
                )

                # Filter out series that are constantly zero
                if any(throughput > 0 for throughput in throughputs):
                    plt.plot(
                        relative_times,
                        throughputs,
                        color=color,
                        label=f"{exp_name} - {vertex_name} ({direction})",
                        linestyle=linestyle,
                        linewidth=3.0,
                        alpha=alpha,
                    )

    def _plot_experiment_throughput(
        self,
        exp_name: str,
        exp_data: Dict[str, Any],
        start_time: float,
        vertex_colors: Dict[str, Any],
        linestyle_in: str,
        linestyle_out: str,
        alpha: float = 1.0,
    ):
        """Plot throughput data for a single experiment."""
        # Plot IN throughput
        self._plot_throughput_direction(
            exp_name,
            exp_data.get("throughput_in_data", {}),
            start_time,
            vertex_colors,
            "In",
            linestyle_in,
            alpha,
        )

        # Plot OUT throughput
        self._plot_throughput_direction(
            exp_name,
            exp_data.get("throughput_out_data", {}),
            start_time,
            vertex_colors,
            "Out",
            linestyle_out,
            alpha,
        )

    def _plot_throughput_direction(
        self,
        exp_name: str,
        throughput_data: Dict[str, Any],
        start_time: float,
        vertex_colors: Dict[str, Any],
        direction: str,
        linestyle: str,
        alpha: float,
    ):
        """Plot throughput data for a specific direction (In/Out)."""
        for vertex_name, measurements in throughput_data.items():
            if measurements:  # Check if measurements exist
                relative_times, throughputs = (
                    self.throughput_calc.prepare_throughput_data(
                        measurements, start_time
                    )
                )

                # Filter out series that are constantly zero
                if any(throughput > 0 for throughput in throughputs):
                    plt.plot(
                        relative_times,
                        throughputs,
                        color=vertex_colors[vertex_name],
                        label=f"{exp_name} - {vertex_name} ({direction})",
                        linestyle=linestyle,
                        linewidth=3.0,
                        alpha=alpha,
                    )

    def _finalize_plot(self, exp1_name: str, exp2_name: str, output_file: str):
        """Finalize the plot with labels, legend, and save."""
        ax = plt.gca()

        # Get current x-axis limits
        x_min, x_max = ax.get_xlim()

        # Set fixed width for x-axis with more granular ticks
        # Use 10-second intervals for better readability
        if x_max - x_min > 0:
            tick_interval = max(10, int((x_max - x_min) / 20))  # At least 20 ticks
            ticks = np.arange(0, x_max + tick_interval, tick_interval)
            ax.set_xticks(ticks)
            ax.set_xticklabels([f"{int(t)}" for t in ticks], rotation=45, ha="right")

        plt.xlabel("Time (seconds)", fontsize=16, fontweight="bold", labelpad=12)
        plt.ylabel(
            "Throughput (records/second)", fontsize=16, fontweight="bold", labelpad=12
        )
        plt.title(
            f"Throughput Comparison: {exp1_name} vs {exp2_name}",
            fontsize=19,
            fontweight="bold",
            pad=20,
        )
        plt.legend(
            bbox_to_anchor=(1.05, 1),
            loc="upper left",
            fontsize=12,
            frameon=True,
            fancybox=True,
            shadow=True,
            framealpha=0.95,
        )
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches="tight")
        plt.close()

    def visualize_three_way_comparison(
        self,
        baseline_name: str,
        baseline_data: Dict[str, Any],
        datasketches_name: str,
        datasketches_data: Dict[str, Any],
        custom_name: str,
        custom_data: Dict[str, Any],
        output_file: str,
    ):
        """Create a visualization comparing three experiments: baseline, datasketches, and custom."""
        plt.figure(figsize=(24, 10))  # Made much wider for better x-axis visibility

        # Define single color for each experiment
        baseline_color = "#E63946"  # Vibrant red
        datasketches_color = "#2E86AB"  # Deep blue
        custom_color = "#06A77D"  # Teal green

        # Calculate start times for each experiment
        baseline_start_time = self.throughput_calc.get_experiment_start_time(
            baseline_data
        )
        datasketches_start_time = self.throughput_calc.get_experiment_start_time(
            datasketches_data
        )
        custom_start_time = self.throughput_calc.get_experiment_start_time(custom_data)

        # Plot all three experiments with distinct colors
        # Baseline: Red, solid/dashed lines (reference standard)
        self._plot_experiment_throughput_single_color(
            baseline_name,
            baseline_data,
            baseline_start_time,
            baseline_color,
            linestyle_in="-",
            linestyle_out="--",
            alpha=0.9,
        )
        # DataSketch: Blue, solid/dashed lines (optimized)
        self._plot_experiment_throughput_single_color(
            datasketches_name,
            datasketches_data,
            datasketches_start_time,
            datasketches_color,
            linestyle_in="-",
            linestyle_out="--",
            alpha=0.9,
        )
        # Custom: Green, solid/dashed lines (experimental)
        self._plot_experiment_throughput_single_color(
            custom_name,
            custom_data,
            custom_start_time,
            custom_color,
            linestyle_in="-",
            linestyle_out="--",
            alpha=0.9,
        )

        # Finalize plot
        self._finalize_three_way_plot(
            baseline_name, datasketches_name, custom_name, output_file
        )

    def _finalize_three_way_plot(
        self,
        baseline_name: str,
        datasketches_name: str,
        custom_name: str,
        output_file: str,
    ):
        """Finalize the three-way comparison plot with labels, legend, and save."""
        ax = plt.gca()

        # Get current x-axis limits
        x_min, x_max = ax.get_xlim()

        # Set fixed width for x-axis with more granular ticks
        # Use 10-second intervals for better readability
        if x_max - x_min > 0:
            tick_interval = max(10, int((x_max - x_min) / 20))  # At least 20 ticks
            ticks = np.arange(0, x_max + tick_interval, tick_interval)
            ax.set_xticks(ticks)
            ax.set_xticklabels([f"{int(t)}" for t in ticks], rotation=45, ha="right")

        plt.xlabel("Time (seconds)", fontsize=16, fontweight="bold", labelpad=12)
        plt.ylabel(
            "Throughput (records/second)", fontsize=16, fontweight="bold", labelpad=12
        )
        plt.title(
            f"Throughput Comparison: {baseline_name} vs {datasketches_name} vs {custom_name}",
            fontsize=19,
            fontweight="bold",
            pad=20,
        )
        plt.legend(
            bbox_to_anchor=(1.05, 1),
            loc="upper left",
            fontsize=12,
            frameon=True,
            fancybox=True,
            shadow=True,
            framealpha=0.95,
        )
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches="tight")
        plt.close()

    def visualize_memory_per_query(self, summary: dict, output_file: str):
        """Create bar chart showing memory usage per query type across algorithms."""
        MemoryVisualizer.visualize_memory_per_query(summary, output_file)

    def visualize_memory_bar_chart(
        self,
        experiment_names: list,
        experiment_data_list: list,
        experiment_summaries: list,
        output_file: str,
    ):
        """Create a bar chart comparing memory usage across experiments."""
        MemoryVisualizer.visualize_memory_bar_chart(
            experiment_names, experiment_data_list, experiment_summaries, output_file
        )

    def visualize_memory_over_time(
        self,
        experiment_names: list,
        experiment_data_list: list,
        output_file: str,
    ):
        """Create a line plot showing memory usage over time for multiple experiments."""
        MemoryVisualizer.visualize_memory_over_time(
            experiment_names, experiment_data_list, output_file
        )

    def visualize_memory_vs_cardinality(
        self,
        cardinality_data: Dict[int, Dict[str, float]],
        output_file: str,
        query_type: str = "all",
        accuracy_data: Dict[int, Dict[str, float]] = None,
    ):
        """Create a memory vs cardinality plot comparing baseline, datasketches, and custom."""
        MemoryVisualizer.visualize_memory_vs_cardinality(
            cardinality_data, output_file, query_type, accuracy_data
        )

    def visualize_memory_vs_window(
        self,
        window_data: Dict[int, Dict[str, float]],
        output_file: str,
        query_type: str = "all",
        accuracy_data: Dict[int, Dict[str, float]] = None,
    ):
        """Create a memory vs tumbling window size plot comparing baseline, datasketches, and custom."""
        MemoryVisualizer.visualize_memory_vs_window(
            window_data, output_file, query_type, accuracy_data
        )

    def visualize_memory_vs_items_per_window(
        self,
        items_data: Dict[int, Dict[str, float]],
        output_file: str,
        query_type: str = "all",
        accuracy_data: Dict[int, Dict[str, float]] = None,
    ):
        """Create a memory vs items per window plot comparing baseline, datasketches, and custom."""
        MemoryVisualizer.visualize_memory_vs_items_per_window(
            items_data, output_file, query_type, accuracy_data
        )

    def visualize_throughput_vs_cpu(
        self,
        cpu_data: Dict[int, Dict[str, float]],
        output_file: str,
        query_type: str = "all",
    ):
        """
        Create a throughput vs CPU cores plot comparing baseline, datasketches, and custom.

        Args:
            cpu_data: Dict mapping CPU cores to algorithm throughput
                     {cpu_cores: {"baseline": throughput, "datasketches": throughput, "custom": throughput}}
            output_file: Path to save the plot
            query_type: Query type for the plot title (e.g., "freq", "quantile")
        """
        # Define colors for each algorithm - vibrant for presentations
        colors = {
            "baseline": "#E63946",  # Vibrant red
            "datasketches": "#2E86AB",  # Deep blue
            "custom": "#06A77D",  # Teal green
        }

        fig, ax = plt.subplots(figsize=(18, 8))

        # Group data by algorithm
        algorithms_data = {}
        for cpu_cores, algorithms in sorted(cpu_data.items()):
            for algorithm, throughput in algorithms.items():
                if algorithm not in algorithms_data:
                    algorithms_data[algorithm] = {"cpu_cores": [], "throughputs": []}
                algorithms_data[algorithm]["cpu_cores"].append(cpu_cores)
                algorithms_data[algorithm]["throughputs"].append(throughput)

        # Plot each algorithm
        for algorithm in sorted(algorithms_data.keys()):
            data = algorithms_data[algorithm]
            color = colors.get(algorithm, "#F77F00")

            # Get display name for algorithm
            display_name = MemoryVisualizer._get_algorithm_display_name(algorithm)

            ax.plot(
                data["cpu_cores"],
                data["throughputs"],
                color=color,
                linewidth=8.0,
                marker="o",
                markersize=16,
                markeredgecolor="white",
                markeredgewidth=3.5,
            )

            # Add algorithm label on the line (at the middle of the trendline)
            if data["cpu_cores"] and data["throughputs"]:
                mid_idx = len(data["cpu_cores"]) // 2
                mid_x = data["cpu_cores"][mid_idx]
                mid_y = data["throughputs"][mid_idx]

                # Place label below the line for higher values, above for lower values
                # This prevents clipping at the top of the graph
                if algorithm == "baseline":
                    # Baseline is usually lower, put above
                    y_offset = mid_y * 1.15
                    v_align = "bottom"
                else:
                    # FlinkSketch/custom are usually higher, put below
                    y_offset = mid_y * 0.85
                    v_align = "top"

                ax.text(
                    mid_x,
                    y_offset,
                    display_name,
                    color=color,
                    fontsize=26,
                    fontweight="bold",
                    va=v_align,
                    ha="center",
                    clip_on=False,  # Don't clip - we positioned it correctly
                )

        # Configure plot
        ax.set_xlabel(
            "CPU Cores (log scale)",
            fontsize=28,
            fontweight="bold",
            labelpad=15,
        )
        ax.set_ylabel(
            "Throughput\n(records/sec)",
            fontsize=28,
            fontweight="bold",
            labelpad=15,
            rotation=0,
            ha="right",
            va="center",
        )
        # No title needed - simpler and cleaner

        # Set x-axis to log scale base 2 (powers of 2: 1, 2, 4, 8, 16, ...)
        ax.set_xscale("log", base=2)

        # Get all CPU core values from the data
        all_cpu_cores = sorted(set(cpu_cores for cpu_cores in cpu_data.keys()))

        # Set x-ticks to show only the CPU cores that exist in the data
        ax.set_xticks(all_cpu_cores)
        ax.set_xticklabels([str(int(cpu)) for cpu in all_cpu_cores])

        ax.tick_params(axis="both", labelsize=24)
        # Make y-axis labels horizontal
        for label in ax.get_yticklabels():
            label.set_rotation(0)
        # Legend removed - algorithm names are labeled directly on the lines

        # No grid for cleaner look

        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"Throughput vs CPU cores plot saved to {output_file}")


class SummaryWriter:
    """Class responsible for generating experiment summaries and reports."""

    def write_experiment_summary(
        self, experiment_name: str, stats: Dict[str, Any], output_file
    ):
        """Write experiment summary to file."""
        content = self._generate_experiment_content(experiment_name, stats)
        output_file.write(content)

    def _generate_experiment_content(
        self, experiment_name: str, stats: Dict[str, Any]
    ) -> str:
        """Generate content for a single experiment."""
        content = f"{experiment_name}:\n"

        # Add experiment metadata if available
        content += self._format_metadata(stats.get("experiment_metadata", {}))

        # Add performance metrics
        content += self._format_performance_metrics(stats)

        # Add accuracy metrics
        content += self._format_accuracy_metrics(stats.get("accuracy_metrics", {}))

        content += "\n"
        return content

    def _format_metadata(self, metadata: Dict[str, Any]) -> str:
        """Format experiment metadata."""
        content = ""
        if metadata:
            content += f"  Type: {metadata.get('aggregation_type', 'Unknown')}\n"
            content += f"  Package: {metadata.get('aggregation_package', 'Unknown')}\n"
            if metadata.get("parameters"):
                params_str = ", ".join(
                    [f"{k}={v}" for k, v in metadata.get("parameters", {}).items()]
                )
                content += f"  Parameters: {params_str}\n"
            content += f"  Window Size: {metadata.get('window_size', 'Unknown')}ms\n"
        return content

    def _format_performance_metrics(self, stats: Dict[str, Any]) -> str:
        """Format performance metrics."""
        content = f"  Avg Throughput In: {stats.get('avg_throughput_in', 0):.2f} records/sec\n"
        content += f"  Max Throughput In: {stats.get('max_throughput_in', 0):.2f} records/sec\n"
        content += f"  Throughput In Samples: {stats.get('throughput_in_samples', 0)}\n"
        content += f"  Avg Throughput Out: {stats.get('avg_throughput_out', 0):.2f} records/sec\n"
        content += f"  Max Throughput Out: {stats.get('max_throughput_out', 0):.2f} records/sec\n"
        content += (
            f"  Throughput Out Samples: {stats.get('throughput_out_samples', 0)}\n"
        )
        content += f"  Avg CPU Usage: {stats.get('avg_cpu_usage', 0):.2f}%\n"
        content += f"  Max CPU Usage: {stats.get('max_cpu_usage', 0):.2f}%\n"
        content += f"  Avg Source Backpressure: {stats.get('avg_source_backpressure', 0):.4f} ({stats.get('avg_source_backpressure', 0)*100:.2f}%)\n"

        # Add memory usage metrics
        avg_memory = stats.get("avg_memory_usage", 0)
        max_memory = stats.get("max_memory_usage", 0)
        memory_samples = stats.get("memory_samples", 0)

        if memory_samples > 0:
            content += f"  Avg Memory Usage: {self._format_memory_size(avg_memory)}\n"
            content += f"  Max Memory Usage: {self._format_memory_size(max_memory)}\n"
            content += f"  Memory Samples: {memory_samples}\n"
        else:
            content += "  Memory Usage: No data available\n"

        return content

    def _format_memory_size(self, bytes_value: float) -> str:
        """Format memory size in human-readable format."""
        if bytes_value == 0:
            return "0 B"

        units = ["B", "KB", "MB", "GB", "TB"]
        size = float(bytes_value)
        unit_index = 0

        while size >= 1024.0 and unit_index < len(units) - 1:
            size /= 1024.0
            unit_index += 1

        return f"{size:.2f} {units[unit_index]}"

    def _format_accuracy_metrics(self, accuracy_metrics: Dict[str, Any]) -> str:
        """Format accuracy metrics."""
        content = ""
        if accuracy_metrics:
            if "error" in accuracy_metrics:
                content += f"  Accuracy Metrics: Error - {accuracy_metrics['error']}\n"
            else:
                content += f"  Avg Relative Error: {accuracy_metrics.get('avg_relative_error_percent', 0):.5f}%\n"

                # Add precision if available (for TopK)
                if "avg_precision" in accuracy_metrics:
                    content += f"  Avg Precision: {accuracy_metrics.get('avg_precision', 0):.5f} ({accuracy_metrics.get('avg_precision_percent', 0):.2f}%)\n"

                # Add recall error if available (for TopK)
                if "avg_recall_error" in accuracy_metrics:
                    content += f"  Avg Recall Error: {accuracy_metrics.get('avg_recall_error', 0):.5f} ({accuracy_metrics.get('avg_recall_error_percent', 0):.2f}%)\n"

                content += f"  Total Windows Compared: {accuracy_metrics.get('total_windows', 0)}\n"
        else:
            content += "  Accuracy Metrics: No comparison data available\n"

        return content

    def write_pair_summary(
        self,
        pair_name: str,
        exp1_name: str,
        exp1_stats: Dict[str, Any],
        exp2_name: str,
        exp2_stats: Dict[str, Any],
        summary_file: str,
    ):
        """Write pair comparison summary."""
        with open(summary_file, "w") as f:
            f.write(f"Experiment Pair Comparison: {exp1_name} vs {exp2_name}\n")
            f.write("=" * 60 + "\n\n")

            self.write_experiment_summary(exp1_name, exp1_stats, f)
            self.write_experiment_summary(exp2_name, exp2_stats, f)

    def write_overall_summary(
        self, summary: Dict[str, Dict[str, Any]], output_file: str
    ):
        """Write overall experiment summary."""
        with open(output_file, "w") as f:
            f.write("Overall Experiment Results Summary\n")
            f.write("=" * 40 + "\n\n")

            for experiment_name, stats in summary.items():
                self.write_experiment_summary(experiment_name, stats, f)

    def write_three_way_summary(
        self,
        baseline_name: str,
        baseline_stats: Dict[str, Any],
        datasketches_name: str,
        datasketches_stats: Dict[str, Any],
        custom_name: str,
        custom_stats: Dict[str, Any],
        summary_file: str,
    ):
        """Write three-way comparison summary."""
        with open(summary_file, "w") as f:
            f.write(
                f"Three-Way Experiment Comparison: {baseline_name} vs {datasketches_name} vs {custom_name}\n"
            )
            f.write("=" * 80 + "\n\n")

            # Write baseline stats
            self.write_experiment_summary(baseline_name, baseline_stats, f)

            # Write datasketches stats
            self.write_experiment_summary(datasketches_name, datasketches_stats, f)

            # Write custom stats
            self.write_experiment_summary(custom_name, custom_stats, f)

            # Add performance comparison section
            f.write("Performance Comparison Summary:\n")
            f.write("-" * 40 + "\n")

            # Compare throughput improvements vs baseline
            baseline_avg_throughput = baseline_stats.get("avg_throughput_in", 0)
            datasketches_avg_throughput = datasketches_stats.get("avg_throughput_in", 0)
            custom_avg_throughput = custom_stats.get("avg_throughput_in", 0)

            if baseline_avg_throughput > 0:
                datasketches_improvement = (
                    (datasketches_avg_throughput - baseline_avg_throughput)
                    / baseline_avg_throughput
                ) * 100
                custom_improvement = (
                    (custom_avg_throughput - baseline_avg_throughput)
                    / baseline_avg_throughput
                ) * 100

                f.write("Throughput vs Baseline:\n")
                f.write(
                    f"  DataSketch: {datasketches_improvement:+.2f}% ({datasketches_avg_throughput:.2f} vs {baseline_avg_throughput:.2f} records/sec)\n"
                )
                f.write(
                    f"  Custom: {custom_improvement:+.2f}% ({custom_avg_throughput:.2f} vs {baseline_avg_throughput:.2f} records/sec)\n"
                )

            # Compare CPU usage vs baseline
            baseline_avg_cpu = baseline_stats.get("avg_cpu_usage", 0)
            datasketches_avg_cpu = datasketches_stats.get("avg_cpu_usage", 0)
            custom_avg_cpu = custom_stats.get("avg_cpu_usage", 0)

            if baseline_avg_cpu > 0:
                datasketches_cpu_change = (
                    (datasketches_avg_cpu - baseline_avg_cpu) / baseline_avg_cpu
                ) * 100
                custom_cpu_change = (
                    (custom_avg_cpu - baseline_avg_cpu) / baseline_avg_cpu
                ) * 100

                f.write("CPU Usage vs Baseline:\n")
                f.write(
                    f"  DataSketch: {datasketches_cpu_change:+.2f}% ({datasketches_avg_cpu:.2f}% vs {baseline_avg_cpu:.2f}%)\n"
                )
                f.write(
                    f"  Custom: {custom_cpu_change:+.2f}% ({custom_avg_cpu:.2f}% vs {baseline_avg_cpu:.2f}%)\n"
                )

            # Compare memory usage vs baseline
            baseline_avg_memory = baseline_stats.get("avg_memory_usage", 0)
            datasketches_avg_memory = datasketches_stats.get("avg_memory_usage", 0)
            custom_avg_memory = custom_stats.get("avg_memory_usage", 0)

            if baseline_avg_memory > 0 and (
                datasketches_avg_memory > 0 or custom_avg_memory > 0
            ):
                f.write("Memory Usage vs Baseline:\n")
                if datasketches_avg_memory > 0:
                    datasketches_memory_change = (
                        (datasketches_avg_memory - baseline_avg_memory)
                        / baseline_avg_memory
                    ) * 100
                    f.write(
                        f"  DataSketch: {datasketches_memory_change:+.2f}% ({self._format_memory_size(datasketches_avg_memory)} vs {self._format_memory_size(baseline_avg_memory)})\n"
                    )
                if custom_avg_memory > 0:
                    custom_memory_change = (
                        (custom_avg_memory - baseline_avg_memory) / baseline_avg_memory
                    ) * 100
                    f.write(
                        f"  Custom: {custom_memory_change:+.2f}% ({self._format_memory_size(custom_avg_memory)} vs {self._format_memory_size(baseline_avg_memory)})\n"
                    )

            f.write("\n")
