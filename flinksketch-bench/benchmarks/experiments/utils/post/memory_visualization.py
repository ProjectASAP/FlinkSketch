#!/usr/bin/env python3
#
# Copyright 2025 ProjectASAP contributors
# SPDX-License-Identifier: Apache-2.0
#


import matplotlib.pyplot as plt
import numpy as np
from typing import Dict


class MemoryVisualizer:
    """Class responsible for creating memory-related visualizations."""

    @staticmethod
    def _get_algorithm_display_name(algorithm: str) -> str:
        """
        Convert algorithm name to display name for legends.

        Args:
            algorithm: Internal algorithm name (baseline, datasketches, custom)

        Returns:
            Display name for legend (Baseline or FlinkSketch)
        """
        if algorithm == "baseline":
            return "Baseline"
        else:
            # datasketches and custom map to FlinkSketch
            return "FlinkSketch"

    @staticmethod
    def visualize_memory_per_query(summary: dict, output_file: str):
        """Create bar chart showing memory usage per query type across algorithms."""
        query_type_data = {}  # {query_type: {algorithm: memory_bytes}}

        for exp_name, exp_stats in summary.items():
            parts = exp_name.split("_")
            if len(parts) < 4:
                continue

            query_type, algorithm = parts[2], parts[3]
            avg_memory = exp_stats.get("avg_memory_usage", 0)
            if avg_memory == 0:
                continue

            if query_type not in query_type_data:
                query_type_data[query_type] = {}
            query_type_data[query_type][algorithm] = avg_memory

        if not query_type_data:
            return

        # Define colors - more vibrant and distinct for presentations
        color_palette = {
            "baseline": "#E63946",  # Vibrant red
            "datasketches": "#2E86AB",  # Deep blue
            "custom": "#06A77D",  # Teal green
        }

        # Prepare data for matplotlib
        query_types = sorted(query_type_data.keys())
        algorithms = sorted(
            set(alg for algos in query_type_data.values() for alg in algos.keys())
        )

        fig, ax = plt.subplots(figsize=(18, 8))

        # Bar configuration
        bar_width = 0.26
        n_algorithms = len(algorithms)
        x = np.arange(len(query_types))

        # Create bars for each algorithm and store memories for later positioning
        algorithm_memories = {}  # {algorithm: [memories per query type]}
        for i, algorithm in enumerate(algorithms):
            memories = [query_type_data[qt].get(algorithm, 0) for qt in query_types]
            algorithm_memories[algorithm] = memories
            offset = (i - (n_algorithms - 1) / 2) * bar_width
            ax.bar(
                x + offset,
                memories,
                bar_width,
                color=color_palette.get(algorithm, "#F77F00"),
                alpha=0.85,
                edgecolor="white",
                linewidth=3.0,
            )

        # Add improvement factor annotations after all bars are created
        for i, algorithm in enumerate(algorithms):
            if algorithm != "baseline":
                offset = (i - (n_algorithms - 1) / 2) * bar_width
                for j, qt in enumerate(query_types):
                    baseline_mem = query_type_data[qt].get("baseline", 0)
                    algo_mem = query_type_data[qt].get(algorithm, 0)

                    if baseline_mem > 0 and algo_mem > 0:
                        improvement = baseline_mem / algo_mem

                        # Position text just above this specific bar
                        # For log scale, a small multiplicative factor works better than additive
                        # Use a smaller multiplier (1.15x) to keep text closer to the bar
                        text_y_position = algo_mem * 1.15

                        ax.text(
                            x[j] + offset,
                            text_y_position,
                            f"{improvement:.1f}×",
                            ha="center",
                            va="bottom",
                            fontweight="bold",
                            fontsize=18,
                            color="#2C3E50",
                            bbox=dict(
                                boxstyle="round,pad=0.4",
                                facecolor="white",
                                alpha=0.9,
                                edgecolor="gray",
                                linewidth=2.0,
                            ),
                        )

        # Configure plot
        ax.set_yscale("log")

        # Set y-axis limits to prevent text from compressing the plot
        # Find min and max memory values across all data
        all_memories = [
            mem
            for qt_data in query_type_data.values()
            for mem in qt_data.values()
            if mem > 0
        ]
        if all_memories:
            min_mem = min(all_memories)
            max_mem = max(all_memories)
            # Set limits with padding: lower by 2x, upper by 3x (for text annotations)
            ax.set_ylim(min_mem / 2, max_mem * 3)

        ax.set_xlabel("Query Type", fontsize=28, fontweight="bold", labelpad=15)
        ax.set_ylabel(
            "Memory\nUsage\n(log scale)",
            fontsize=28,
            fontweight="bold",
            labelpad=15,
            rotation=0,
            ha="right",
            va="center",
        )
        # No title needed - simpler and cleaner

        ax.set_xticks(x)
        ax.set_xticklabels([qt.capitalize() for qt in query_types], fontsize=24)
        ax.tick_params(axis="y", labelsize=24)
        # Make y-axis labels horizontal
        for label in ax.get_yticklabels():
            label.set_rotation(0)

        # Add manual legend with larger text
        from matplotlib.patches import Patch

        legend_elements = [
            Patch(
                facecolor=color_palette.get(alg, "#F77F00"),
                edgecolor="white",
                linewidth=3.0,
                label=MemoryVisualizer._get_algorithm_display_name(alg),
            )
            for alg in algorithms
        ]
        ax.legend(
            handles=legend_elements,
            fontsize=22,
            frameon=True,
            fancybox=True,
            shadow=True,
            framealpha=0.95,
            loc="upper right",
        )

        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"Memory per query plot saved to {output_file}")

    @staticmethod
    def visualize_memory_bar_chart(
        experiment_names: list,
        experiment_data_list: list,
        experiment_summaries: list,
        output_file: str,
    ):
        """Create a bar chart comparing memory usage across experiments.

        Args:
            experiment_names: List of experiment names (e.g., baseline, datasketches, custom)
            experiment_data_list: List of experiment data dictionaries
            experiment_summaries: List of summary statistics for each experiment
            output_file: Path to save the plot
        """
        # Prepare data
        short_names = []
        avg_mems = []
        max_mems = []

        for name, summary in zip(experiment_names, experiment_summaries):
            avg_mem = summary.get("avg_memory_usage", 0)
            max_mem = summary.get("max_memory_usage", 0)

            # Extract algorithm and convert to display name
            if "baseline" in name:
                algorithm = "baseline"
            elif "datasketches" in name:
                algorithm = "datasketches"
            elif "custom" in name or "ddsketch" in name:
                algorithm = "custom"
            else:
                algorithm = name

            short_name = MemoryVisualizer._get_algorithm_display_name(algorithm)
            short_names.append(short_name)
            avg_mems.append(avg_mem)
            max_mems.append(max_mem)

        # Check if there's any actual data
        has_data = sum(avg_mems) + sum(max_mems) > 0

        fig, ax = plt.subplots(figsize=(14, 7))

        if has_data:
            x = np.arange(len(short_names))
            width = 0.38

            # Create bars
            bars1 = ax.bar(
                x - width / 2,
                avg_mems,
                width,
                label="Average Memory",
                color="#2E86AB",
                alpha=0.85,
                edgecolor="white",
                linewidth=1.5,
            )
            bars2 = ax.bar(
                x + width / 2,
                max_mems,
                width,
                label="Maximum Memory",
                color="#E63946",
                alpha=0.85,
                edgecolor="white",
                linewidth=1.5,
            )

            # Add value labels on bars
            def format_memory(bytes_val):
                if bytes_val >= 1024 * 1024:
                    return f"{bytes_val/(1024*1024):.2f} MB"
                elif bytes_val >= 1024:
                    return f"{bytes_val/1024:.2f} KB"
                elif bytes_val > 0:
                    return f"{bytes_val:.0f} B"
                else:
                    return ""

            for bars in [bars1, bars2]:
                for bar in bars:
                    height = bar.get_height()
                    if height > 0:
                        ax.text(
                            bar.get_x() + bar.get_width() / 2.0,
                            height,
                            format_memory(height),
                            ha="center",
                            va="bottom",
                            fontsize=11,
                            fontweight="bold",
                        )

            ax.set_xlabel("Algorithm", fontsize=16, fontweight="bold", labelpad=12)
            ax.set_ylabel(
                "Memory Usage (bytes)", fontsize=16, fontweight="bold", labelpad=12
            )
            ax.set_title(
                "Memory Usage Comparison", fontsize=19, fontweight="bold", pad=20
            )
            ax.set_xticks(x)
            ax.set_xticklabels(short_names, fontsize=13)
            ax.tick_params(axis="y", labelsize=13)
            ax.legend(
                title="Metric",
                fontsize=12,
                title_fontsize=13,
                frameon=True,
                fancybox=True,
                shadow=True,
                framealpha=0.95,
            )
        else:
            # Create a simple message plot when no data
            ax.text(
                0.5,
                0.5,
                "No memory data available\n(Check verbose=true and output_mode includes 'memory')",
                ha="center",
                va="center",
                fontsize=12,
                color="red",
                transform=ax.transAxes,
            )
            ax.set_title("Memory Usage Comparison", fontsize=14, fontweight="bold")
            ax.axis("off")

        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"Memory bar chart saved to {output_file}")

    @staticmethod
    def visualize_memory_over_time(
        experiment_names: list,
        experiment_data_list: list,
        output_file: str,
    ):
        """Create a line plot showing memory usage over time for multiple experiments.

        Args:
            experiment_names: List of experiment names (e.g., baseline, datasketches, custom)
            experiment_data_list: List of experiment data dictionaries with memory_data
            output_file: Path to save the plot
        """
        # Extract algorithm and convert to display names for legend
        short_names = []
        for name in experiment_names:
            if "baseline" in name:
                algorithm = "baseline"
            elif "datasketches" in name:
                algorithm = "datasketches"
            elif "custom" in name or "ddsketch" in name:
                algorithm = "custom"
            else:
                algorithm = name

            short_names.append(MemoryVisualizer._get_algorithm_display_name(algorithm))

        # Find global minimum timestamp for consistent normalization
        all_timestamps = [
            entry["timestamp"]
            for exp_data in experiment_data_list
            for entry in exp_data.get("memory_data", [])
        ]

        fig, ax = plt.subplots(figsize=(16, 8))

        if not all_timestamps:
            # No data - create a message plot
            ax.text(
                0.5,
                0.5,
                "No memory data available\n(Check verbose=true and output_mode includes 'memory')",
                ha="center",
                va="center",
                fontsize=14,
                color="red",
                transform=ax.transAxes,
            )
            ax.set_title(
                "Memory Usage Over Time", fontsize=19, fontweight="bold", pad=20
            )
            ax.axis("off")
            plt.tight_layout()
            plt.savefig(output_file, dpi=300, bbox_inches="tight")
            plt.close()
            print(f"Memory over time plot saved to {output_file}")
            return

        global_min_timestamp = min(all_timestamps)

        # Define colors - vibrant for presentations
        colors = {
            "Baseline": "#E63946",  # Vibrant red
            "FlinkSketch - datasketches": "#2E86AB",  # Deep blue
            "FlinkSketch - custom": "#06A77D",  # Teal green
        }

        # Plot each experiment
        for short_name, exp_data in zip(short_names, experiment_data_list):
            memory_data = exp_data.get("memory_data", [])
            if not memory_data:
                continue

            times = []
            memories = []
            for entry in memory_data:
                normalized_time = (entry["timestamp"] - global_min_timestamp) / 1000.0
                times.append(normalized_time)
                memories.append(entry["memory_bytes"])

            color = colors.get(short_name, "#F77F00")
            ax.plot(
                times,
                memories,
                label=short_name,
                color=color,
                linewidth=3.0,
                alpha=0.9,
                marker="o",
                markersize=4,
            )

        # Configure plot
        ax.set_yscale("log")
        ax.set_xlabel("Time (seconds)", fontsize=16, fontweight="bold", labelpad=12)
        ax.set_ylabel(
            "Memory Usage (bytes, log scale)",
            fontsize=16,
            fontweight="bold",
            labelpad=12,
        )
        ax.set_title("Memory Usage Over Time", fontsize=19, fontweight="bold", pad=20)
        ax.tick_params(axis="both", labelsize=13)
        ax.legend(
            title="Experiment",
            fontsize=12,
            title_fontsize=13,
            loc="best",
            frameon=True,
            fancybox=True,
            shadow=True,
            framealpha=0.95,
        )

        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"Memory over time plot saved to {output_file}")

    @staticmethod
    def visualize_memory_vs_cardinality(
        cardinality_data: Dict[int, Dict[str, float]],
        output_file: str,
        query_type: str = "all",
        accuracy_data: Dict[int, Dict[str, float]] = None,
    ):
        """
        Create a memory vs cardinality plot comparing baseline, datasketches, and custom.

        Args:
            cardinality_data: Dict mapping cardinality to algorithm memory usage
                             {cardinality: {"baseline": mem, "datasketches": mem, "custom": mem}}
            output_file: Path to save the plot
            query_type: Query type for the plot title (e.g., "freq", "quantile")
            accuracy_data: Dict mapping cardinality to algorithm accuracy metrics
                          {cardinality: {"datasketches": accuracy%, "custom": accuracy%}}
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
        for cardinality, algorithms in sorted(cardinality_data.items()):
            for algorithm, memory_bytes in algorithms.items():
                if algorithm not in algorithms_data:
                    algorithms_data[algorithm] = {"cardinalities": [], "memories": []}
                algorithms_data[algorithm]["cardinalities"].append(cardinality)
                algorithms_data[algorithm]["memories"].append(
                    memory_bytes
                )  # Keep in bytes

        # Plot each algorithm
        for algorithm in sorted(algorithms_data.keys()):
            data = algorithms_data[algorithm]
            color = colors.get(algorithm, "#F77F00")
            ax.plot(
                data["cardinalities"],
                data["memories"],
                color=color,
                linewidth=8.0,
                marker="o",
                markersize=16,
                markeredgecolor="white",
                markeredgewidth=3.5,
            )

            # Add algorithm label on the line (at the middle of the trendline)
            if data["cardinalities"] and data["memories"]:
                mid_idx = len(data["cardinalities"]) // 2
                mid_x = data["cardinalities"][mid_idx]
                mid_y = data["memories"][mid_idx]
                label_text = MemoryVisualizer._get_algorithm_display_name(algorithm)
                ax.text(
                    mid_x,
                    mid_y * 2.5,  # Reduced from 3.5 to stay within bounds
                    label_text,
                    color=color,
                    fontsize=26,
                    fontweight="bold",
                    va="bottom",
                    ha="center",
                    clip_on=True,  # Clip text to axes bounds
                )

        # Add accuracy annotations on data points (only for non-baseline algorithms)
        if accuracy_data:
            for cardinality in sorted(cardinality_data.keys()):
                algorithms_accuracy = accuracy_data.get(cardinality, {})
                algorithms_memory = cardinality_data.get(cardinality, {})

                for algorithm, accuracy_value in algorithms_accuracy.items():
                    if algorithm in algorithms_memory:
                        memory_value = algorithms_memory[algorithm]

                        # Format accuracy text based on metric type
                        # Show as accuracy percentage (1 - relative_error)
                        # Higher values are better - display only the number
                        if accuracy_value >= 99.0:
                            # Very high accuracy (99%+)
                            accuracy_text = f"{accuracy_value:.2f}%"
                        elif accuracy_value >= 90.0:
                            # High accuracy (90-99%)
                            accuracy_text = f"{accuracy_value:.1f}%"
                        else:
                            # Lower accuracy (<90%)
                            accuracy_text = f"{accuracy_value:.1f}%"

                        color = colors.get(algorithm, "#F77F00")

                        ax.annotate(
                            accuracy_text,
                            xy=(cardinality, memory_value),
                            xytext=(0, 15),  # Further above data point
                            textcoords="offset points",
                            ha="center",
                            va="bottom",
                            fontsize=16,
                            fontweight="bold",
                            color=color,
                            bbox=dict(
                                boxstyle="round,pad=0.4",
                                facecolor="white",
                                alpha=0.85,
                                edgecolor=color,
                                linewidth=2.5,
                            ),
                        )

        # Configure plot
        ax.set_yscale("log")
        ax.set_xscale("log")
        ax.set_xlabel(
            "Cardinality (Number of Distinct Keys) (log scale)",
            fontsize=28,
            fontweight="bold",
            labelpad=15,
        )
        # Position y-axis label in the middle, horizontal with two lines
        ax.set_ylabel(
            "Memory\nUsage\n(log scale)",
            fontsize=28,
            fontweight="bold",
            labelpad=15,
            rotation=0,
            ha="right",
            va="center",
        )
        # Format query type for title display
        query_type_display = query_type.capitalize()
        if query_type_display == "Freq":
            query_type_display = "Frequency"

        # No title needed - simpler and cleaner

        # Format x-axis labels with K/M suffixes
        def format_cardinality(x, p):
            if x >= 1_000_000:
                return f"{x/1_000_000:.1f}M"
            elif x >= 1_000:
                return f"{x/1_000:.0f}K"
            else:
                return f"{int(x)}"

        # Apply custom formatter (ticklabel_format not compatible with log scale)
        ax.xaxis.set_major_formatter(plt.FuncFormatter(format_cardinality))

        # Set y-axis to show only powers of 10 (1KB, 10KB, 100KB, 1MB, 10MB, etc.)
        from matplotlib.ticker import FixedLocator, FuncFormatter

        # Define specific tick locations for nice round numbers
        # 1KB, 10KB, 100KB, 1MB, 10MB, 100MB, 1GB, 10GB
        tick_locations = [
            1 * 1024,  # 1KB
            10 * 1024,  # 10KB
            100 * 1024,  # 100KB
            1 * 1024 * 1024,  # 1MB
            10 * 1024 * 1024,  # 10MB
            100 * 1024 * 1024,  # 100MB
            1 * 1024 * 1024 * 1024,  # 1GB
            10 * 1024 * 1024 * 1024,  # 10GB
        ]
        ax.yaxis.set_major_locator(FixedLocator(tick_locations))

        # Format y-axis labels as KB/MB with only 1, 10, 100 values
        def format_memory_units(y, p):
            if y <= 0:
                return ""
            kb = y / 1024
            mb = y / (1024 * 1024)
            gb = y / (1024 * 1024 * 1024)

            if gb >= 1:
                return f"{int(round(gb))}GB"
            elif mb >= 1:
                return f"{int(round(mb))}MB"
            else:
                return f"{int(round(kb))}KB"

        ax.yaxis.set_major_formatter(FuncFormatter(format_memory_units))

        # Make y-axis labels horizontal
        ax.tick_params(axis="both", labelsize=24)
        for label in ax.get_yticklabels():
            label.set_rotation(0)
        # Legend removed - algorithm names are labeled directly on the lines

        # Adjust y-axis limits to accommodate annotations
        # Get all memory values
        all_memories = [
            mem for alg_data in algorithms_data.values() for mem in alg_data["memories"]
        ]
        if all_memories:
            min_mem = min(all_memories)
            max_mem = max(all_memories)
            # Set limits with padding: lower by 1.5x, upper by 4x (for text labels and annotations)
            ax.set_ylim(min_mem / 1.5, max_mem * 4)

        # Add memory reduction arrow at the rightmost cardinality point
        if "baseline" in algorithms_data and len(algorithms_data) > 1:
            # Get the rightmost cardinality (maximum cardinality)
            rightmost_cardinality = max(cardinality_data.keys())
            baseline_memory = cardinality_data[rightmost_cardinality].get("baseline")

            if baseline_memory:
                # Find the best (lowest memory) non-baseline algorithm at this cardinality
                non_baseline_algorithms = {
                    alg: mem
                    for alg, mem in cardinality_data[rightmost_cardinality].items()
                    if alg != "baseline"
                }

                if non_baseline_algorithms:
                    best_algorithm = min(
                        non_baseline_algorithms, key=non_baseline_algorithms.get
                    )
                    best_memory = non_baseline_algorithms[best_algorithm]

                    # Calculate reduction factor
                    reduction_factor = baseline_memory / best_memory

                    # Arrow is at the same x-position as the rightmost data points
                    arrow_x = rightmost_cardinality

                    # Calculate arrow endpoints (70% of total distance, centered)
                    # Use geometric mean for log scale
                    geometric_center = (baseline_memory * best_memory) ** 0.5
                    total_log_distance = np.log(baseline_memory) - np.log(best_memory)
                    arrow_log_distance = total_log_distance * 0.7

                    # Calculate arrow endpoints symmetrically around geometric center
                    arrow_top = np.exp(
                        np.log(geometric_center) + arrow_log_distance / 2
                    )
                    arrow_bottom = np.exp(
                        np.log(geometric_center) - arrow_log_distance / 2
                    )

                    # Draw double-sided vertical arrow (70% of distance)
                    ax.annotate(
                        "",
                        xy=(arrow_x, arrow_bottom),
                        xytext=(arrow_x, arrow_top),
                        arrowprops=dict(
                            arrowstyle="<->",
                            color="#2C3E50",
                            lw=5.5,
                            shrinkA=0,
                            shrinkB=0,
                        ),
                    )

                    # Add text annotation for reduction factor
                    # Position text to the left of the arrow using annotate
                    mid_y = (
                        baseline_memory * best_memory
                    ) ** 0.5  # Geometric mean for log scale
                    ax.annotate(
                        f"{reduction_factor:.1f}× reduction",
                        xy=(arrow_x, mid_y),
                        xytext=(-15, 0),
                        textcoords="offset points",
                        ha="right",
                        va="center",
                        fontsize=20,
                        fontweight="bold",
                        color="#2C3E50",
                        bbox=dict(
                            boxstyle="round,pad=0.5",
                            facecolor="white",
                            alpha=0.95,
                            edgecolor="#2C3E50",
                            linewidth=3,
                        ),
                    )

        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"Memory vs cardinality plot saved to {output_file}")

    @staticmethod
    def visualize_memory_vs_window(
        window_data: Dict[int, Dict[str, float]],
        output_file: str,
        query_type: str = "all",
        accuracy_data: Dict[int, Dict[str, float]] = None,
    ):
        """
        Create a memory vs tumbling window size plot comparing baseline, datasketches, and custom.

        Args:
            window_data: Dict mapping window size to algorithm memory usage
                        {window_size: {"baseline": mem, "datasketches": mem, "custom": mem}}
            output_file: Path to save the plot
            query_type: Query type for the plot title (e.g., "freq", "quantile")
            accuracy_data: Dict mapping window size to algorithm accuracy metrics
                          {window_size: {"datasketches": accuracy%, "custom": accuracy%}}
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
        for window_size, algorithms in sorted(window_data.items()):
            for algorithm, memory_bytes in algorithms.items():
                if algorithm not in algorithms_data:
                    algorithms_data[algorithm] = {"window_sizes": [], "memories": []}
                algorithms_data[algorithm]["window_sizes"].append(window_size)
                algorithms_data[algorithm]["memories"].append(memory_bytes)

        # Plot each algorithm
        for algorithm in sorted(algorithms_data.keys()):
            data = algorithms_data[algorithm]
            color = colors.get(algorithm, "#F77F00")
            ax.plot(
                data["window_sizes"],
                data["memories"],
                color=color,
                linewidth=8.0,
                marker="o",
                markersize=16,
                markeredgecolor="white",
                markeredgewidth=3.5,
            )

            # Add algorithm label on the line (at the middle of the trendline)
            if data["window_sizes"] and data["memories"]:
                mid_idx = len(data["window_sizes"]) // 2
                mid_x = data["window_sizes"][mid_idx]
                mid_y = data["memories"][mid_idx]
                label_text = MemoryVisualizer._get_algorithm_display_name(algorithm)
                ax.text(
                    mid_x,
                    mid_y * 2.5,  # Reduced from 3.5 to stay within bounds
                    label_text,
                    color=color,
                    fontsize=26,
                    fontweight="bold",
                    va="bottom",
                    ha="center",
                    clip_on=True,  # Clip text to axes bounds
                )

        # Add accuracy annotations on data points (only for non-baseline algorithms)
        if accuracy_data:
            for window_size in sorted(window_data.keys()):
                algorithms_accuracy = accuracy_data.get(window_size, {})
                algorithms_memory = window_data.get(window_size, {})

                for algorithm, accuracy_value in algorithms_accuracy.items():
                    if algorithm in algorithms_memory:
                        memory_value = algorithms_memory[algorithm]

                        # Format accuracy text based on metric type
                        # Show as accuracy percentage (1 - relative_error)
                        # Higher values are better - display as "..% acc"
                        if accuracy_value >= 99.0:
                            accuracy_text = f"{accuracy_value:.2f}%"
                        elif accuracy_value >= 90.0:
                            accuracy_text = f"{accuracy_value:.1f}%"
                        else:
                            accuracy_text = f"{accuracy_value:.1f}%"

                        color = colors.get(algorithm, "#F77F00")

                        ax.annotate(
                            accuracy_text,
                            xy=(window_size, memory_value),
                            xytext=(0, 15),  # Further above data point
                            textcoords="offset points",
                            ha="center",
                            va="bottom",
                            fontsize=16,
                            fontweight="bold",
                            color=color,
                            bbox=dict(
                                boxstyle="round,pad=0.4",
                                facecolor="white",
                                alpha=0.85,
                                edgecolor=color,
                                linewidth=2.5,
                            ),
                        )

        # Configure plot
        ax.set_yscale("log")
        ax.set_xlabel(
            "Tumbling Window Size (seconds)",
            fontsize=28,
            fontweight="bold",
            labelpad=15,
        )
        # Position y-axis label in the middle, horizontal with two lines
        ax.set_ylabel(
            "Memory\nUsage\n(log scale)",
            fontsize=28,
            fontweight="bold",
            labelpad=15,
            rotation=0,
            ha="right",
            va="center",
        )
        # Format query type for title display
        query_type_display = query_type.capitalize()
        if query_type_display == "Freq":
            query_type_display = "Frequency"

        # No title needed - simpler and cleaner

        # Format x-axis labels - window sizes are in seconds, display directly
        ax.ticklabel_format(style="plain", axis="x")

        # Set y-axis to show only powers of 10 (1KB, 10KB, 100KB, 1MB, 10MB, etc.)
        from matplotlib.ticker import FixedLocator, FuncFormatter

        # Define specific tick locations for nice round numbers
        # 1KB, 10KB, 100KB, 1MB, 10MB, 100MB, 1GB, 10GB
        tick_locations = [
            1 * 1024,  # 1KB
            10 * 1024,  # 10KB
            100 * 1024,  # 100KB
            1 * 1024 * 1024,  # 1MB
            10 * 1024 * 1024,  # 10MB
            100 * 1024 * 1024,  # 100MB
            1 * 1024 * 1024 * 1024,  # 1GB
            10 * 1024 * 1024 * 1024,  # 10GB
        ]
        ax.yaxis.set_major_locator(FixedLocator(tick_locations))

        # Format y-axis labels as KB/MB with only 1, 10, 100 values
        def format_memory_units(y, p):
            if y <= 0:
                return ""
            kb = y / 1024
            mb = y / (1024 * 1024)
            gb = y / (1024 * 1024 * 1024)

            if gb >= 1:
                return f"{int(round(gb))}GB"
            elif mb >= 1:
                return f"{int(round(mb))}MB"
            else:
                return f"{int(round(kb))}KB"

        ax.yaxis.set_major_formatter(FuncFormatter(format_memory_units))

        # Make y-axis labels horizontal
        ax.tick_params(axis="both", labelsize=24)
        for label in ax.get_yticklabels():
            label.set_rotation(0)
        # Legend removed - algorithm names are labeled directly on the lines

        # Adjust y-axis limits to accommodate annotations
        all_memories = [
            mem for alg_data in algorithms_data.values() for mem in alg_data["memories"]
        ]
        if all_memories:
            min_mem = min(all_memories)
            max_mem = max(all_memories)
            # Set limits with padding: lower by 1.5x, upper by 4x (for text labels and annotations)
            ax.set_ylim(min_mem / 1.5, max_mem * 4)

        # Add memory reduction arrow at the rightmost window size
        if "baseline" in algorithms_data and len(algorithms_data) > 1:
            rightmost_window = max(window_data.keys())
            baseline_memory = window_data[rightmost_window].get("baseline")

            if baseline_memory:
                non_baseline_algorithms = {
                    alg: mem
                    for alg, mem in window_data[rightmost_window].items()
                    if alg != "baseline"
                }

                if non_baseline_algorithms:
                    best_algorithm = min(
                        non_baseline_algorithms, key=non_baseline_algorithms.get
                    )
                    best_memory = non_baseline_algorithms[best_algorithm]

                    reduction_factor = baseline_memory / best_memory

                    arrow_x = rightmost_window

                    # Calculate arrow endpoints (70% of distance, centered)
                    geometric_center = (baseline_memory * best_memory) ** 0.5
                    total_log_distance = np.log(baseline_memory) - np.log(best_memory)
                    arrow_log_distance = total_log_distance * 0.7

                    arrow_top = np.exp(
                        np.log(geometric_center) + arrow_log_distance / 2
                    )
                    arrow_bottom = np.exp(
                        np.log(geometric_center) - arrow_log_distance / 2
                    )

                    # Draw double-sided vertical arrow (70% of distance)
                    ax.annotate(
                        "",
                        xy=(arrow_x, arrow_bottom),
                        xytext=(arrow_x, arrow_top),
                        arrowprops=dict(
                            arrowstyle="<->",
                            color="#2C3E50",
                            lw=5.5,
                            shrinkA=0,
                            shrinkB=0,
                        ),
                    )

                    # Add text annotation for reduction factor
                    mid_y = (baseline_memory * best_memory) ** 0.5
                    ax.annotate(
                        f"{reduction_factor:.1f}× reduction",
                        xy=(arrow_x, mid_y),
                        xytext=(-15, 0),
                        textcoords="offset points",
                        ha="right",
                        va="center",
                        fontsize=20,
                        fontweight="bold",
                        color="#2C3E50",
                        bbox=dict(
                            boxstyle="round,pad=0.5",
                            facecolor="white",
                            alpha=0.95,
                            edgecolor="#2C3E50",
                            linewidth=3,
                        ),
                    )

        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"Memory vs window size plot saved to {output_file}")

    @staticmethod
    def visualize_memory_vs_items_per_window(
        items_data: Dict[int, Dict[str, float]],
        output_file: str,
        query_type: str = "all",
        accuracy_data: Dict[int, Dict[str, float]] = None,
    ):
        """
        Create a memory vs items per window plot comparing baseline, datasketches, and custom.

        Args:
            items_data: Dict mapping items per window to algorithm memory usage
                       {items_per_window: {"baseline": mem, "datasketches": mem, "custom": mem}}
            output_file: Path to save the plot
            query_type: Query type for the plot title (e.g., "freq", "quantile")
            accuracy_data: Dict mapping items per window to algorithm accuracy metrics
                          {items_per_window: {"datasketches": accuracy%, "custom": accuracy%}}
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
        for items_per_window, algorithms in sorted(items_data.items()):
            for algorithm, memory_bytes in algorithms.items():
                if algorithm not in algorithms_data:
                    algorithms_data[algorithm] = {
                        "items_per_window": [],
                        "memories": [],
                    }
                algorithms_data[algorithm]["items_per_window"].append(items_per_window)
                algorithms_data[algorithm]["memories"].append(memory_bytes)

        # Plot each algorithm
        for algorithm in sorted(algorithms_data.keys()):
            data = algorithms_data[algorithm]
            color = colors.get(algorithm, "#F77F00")
            ax.plot(
                data["items_per_window"],
                data["memories"],
                color=color,
                linewidth=8.0,
                marker="o",
                markersize=16,
                markeredgecolor="white",
                markeredgewidth=3.5,
            )

            # Add algorithm label on the line (at the middle of the trendline)
            if data["items_per_window"] and data["memories"]:
                mid_idx = len(data["items_per_window"]) // 2
                mid_x = data["items_per_window"][mid_idx]
                mid_y = data["memories"][mid_idx]
                label_text = MemoryVisualizer._get_algorithm_display_name(algorithm)
                ax.text(
                    mid_x,
                    mid_y * 2.5,  # Reduced from 3.5 to stay within bounds
                    label_text,
                    color=color,
                    fontsize=26,
                    fontweight="bold",
                    va="bottom",
                    ha="center",
                    clip_on=True,  # Clip text to axes bounds
                )

        # Add accuracy annotations on data points (only for non-baseline algorithms)
        if accuracy_data:
            for items_per_window in sorted(items_data.keys()):
                algorithms_accuracy = accuracy_data.get(items_per_window, {})
                algorithms_memory = items_data.get(items_per_window, {})

                for algorithm, accuracy_value in algorithms_accuracy.items():
                    if algorithm in algorithms_memory:
                        memory_value = algorithms_memory[algorithm]

                        # Format accuracy text based on metric type
                        # Show as accuracy percentage (1 - relative_error)
                        # Higher values are better - display as "..% acc"
                        if accuracy_value >= 99.0:
                            accuracy_text = f"{accuracy_value:.2f}%"
                        elif accuracy_value >= 90.0:
                            accuracy_text = f"{accuracy_value:.1f}%"
                        else:
                            accuracy_text = f"{accuracy_value:.1f}%"

                        color = colors.get(algorithm, "#F77F00")

                        ax.annotate(
                            accuracy_text,
                            xy=(items_per_window, memory_value),
                            xytext=(0, 15),  # Further above data point
                            textcoords="offset points",
                            ha="center",
                            va="bottom",
                            fontsize=16,
                            fontweight="bold",
                            color=color,
                            bbox=dict(
                                boxstyle="round,pad=0.4",
                                facecolor="white",
                                alpha=0.85,
                                edgecolor=color,
                                linewidth=2.5,
                            ),
                        )

        # Configure plot
        ax.set_yscale("log")
        ax.set_xscale("log")
        ax.set_xlabel(
            "Amount of Data Aggregated (log scale)",
            fontsize=28,
            fontweight="bold",
            labelpad=15,
        )
        # Position y-axis label in the middle, horizontal with two lines
        ax.set_ylabel(
            "Memory\nUsage\n(log scale)",
            fontsize=28,
            fontweight="bold",
            labelpad=15,
            rotation=0,
            ha="right",
            va="center",
        )

        # Collect all unique items_per_window values from the data
        all_items_per_window = sorted(
            set(
                ipw
                for alg_data in algorithms_data.values()
                for ipw in alg_data["items_per_window"]
            )
        )

        # Set x-axis ticks to only show actual data points
        ax.set_xticks(all_items_per_window)

        # Format x-axis labels with K/M suffixes
        def format_items(x, p):
            if x >= 1_000_000:
                return f"{x/1_000_000:.1f}M"
            elif x >= 1_000:
                return f"{x/1_000:.0f}K"
            else:
                return f"{int(x)}"

        # Apply custom formatter (ticklabel_format not compatible with log scale)
        ax.xaxis.set_major_formatter(plt.FuncFormatter(format_items))

        # Set y-axis to show only powers of 10 (1KB, 10KB, 100KB, 1MB, 10MB, etc.)
        from matplotlib.ticker import FixedLocator, FuncFormatter

        # Define specific tick locations for nice round numbers
        # 1KB, 10KB, 100KB, 1MB, 10MB, 100MB, 1GB, 10GB
        tick_locations = [
            1 * 1024,  # 1KB
            10 * 1024,  # 10KB
            100 * 1024,  # 100KB
            1 * 1024 * 1024,  # 1MB
            10 * 1024 * 1024,  # 10MB
            100 * 1024 * 1024,  # 100MB
            1 * 1024 * 1024 * 1024,  # 1GB
            10 * 1024 * 1024 * 1024,  # 10GB
        ]
        ax.yaxis.set_major_locator(FixedLocator(tick_locations))

        # Format y-axis labels as KB/MB with only 1, 10, 100 values
        def format_memory_units(y, p):
            if y <= 0:
                return ""
            kb = y / 1024
            mb = y / (1024 * 1024)
            gb = y / (1024 * 1024 * 1024)

            if gb >= 1:
                return f"{int(round(gb))}GB"
            elif mb >= 1:
                return f"{int(round(mb))}MB"
            else:
                return f"{int(round(kb))}KB"

        ax.yaxis.set_major_formatter(FuncFormatter(format_memory_units))

        # Make y-axis labels horizontal
        ax.tick_params(axis="both", labelsize=24)
        for label in ax.get_yticklabels():
            label.set_rotation(0)
        # Legend removed - algorithm names are labeled directly on the lines

        # Adjust y-axis limits to accommodate annotations
        all_memories = [
            mem for alg_data in algorithms_data.values() for mem in alg_data["memories"]
        ]
        if all_memories:
            min_mem = min(all_memories)
            max_mem = max(all_memories)
            # Set limits with padding: lower by 1.5x, upper by 4x (for text labels and annotations)
            ax.set_ylim(min_mem / 1.5, max_mem * 4)

        # Add memory reduction arrow at the rightmost items per window
        if "baseline" in algorithms_data and len(algorithms_data) > 1:
            rightmost_items = max(items_data.keys())
            baseline_memory = items_data[rightmost_items].get("baseline")

            if baseline_memory:
                non_baseline_algorithms = {
                    alg: mem
                    for alg, mem in items_data[rightmost_items].items()
                    if alg != "baseline"
                }

                if non_baseline_algorithms:
                    best_algorithm = min(
                        non_baseline_algorithms, key=non_baseline_algorithms.get
                    )
                    best_memory = non_baseline_algorithms[best_algorithm]

                    reduction_factor = baseline_memory / best_memory

                    arrow_x = rightmost_items

                    # Calculate arrow endpoints (70% of distance, centered)
                    geometric_center = (baseline_memory * best_memory) ** 0.5
                    total_log_distance = np.log(baseline_memory) - np.log(best_memory)
                    arrow_log_distance = total_log_distance * 0.7

                    arrow_top = np.exp(
                        np.log(geometric_center) + arrow_log_distance / 2
                    )
                    arrow_bottom = np.exp(
                        np.log(geometric_center) - arrow_log_distance / 2
                    )

                    # Draw double-sided vertical arrow (70% of distance)
                    ax.annotate(
                        "",
                        xy=(arrow_x, arrow_bottom),
                        xytext=(arrow_x, arrow_top),
                        arrowprops=dict(
                            arrowstyle="<->",
                            color="#2C3E50",
                            lw=5.5,
                            shrinkA=0,
                            shrinkB=0,
                        ),
                    )

                    # Add text annotation for reduction factor
                    mid_y = (baseline_memory * best_memory) ** 0.5
                    ax.annotate(
                        f"{reduction_factor:.1f}× reduction",
                        xy=(arrow_x, mid_y),
                        xytext=(-15, 0),
                        textcoords="offset points",
                        ha="right",
                        va="center",
                        fontsize=20,
                        fontweight="bold",
                        color="#2C3E50",
                        bbox=dict(
                            boxstyle="round,pad=0.5",
                            facecolor="white",
                            alpha=0.95,
                            edgecolor="#2C3E50",
                            linewidth=3,
                        ),
                    )

        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"Memory vs items per window plot saved to {output_file}")
