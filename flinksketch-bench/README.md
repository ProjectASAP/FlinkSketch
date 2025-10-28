# FlinkSketch Benchmarking Framework

This module contains the benchmarking infrastructure for FlinkSketch, including baseline implementations, experiment orchestration, and analysis tools.

## Quick Start

### 1. Environment Setup

Run the environment setup script to install all required dependencies (Python, Java, Maven, Docker, Flink):

```bash
bash flinksketch-bench/benchmarks/setup/env_setup.sh
```

**Optional:** Install async-profiler for performance monitoring:

```bash
bash flinksketch-bench/benchmarks/setup/async_profiler_setup.sh
```

Set the FLINK_HOME environment variable to your Flink installation directory:

```bash
export FLINK_HOME=/path/to/your/flink
```

### 2. Build the Project

```bash
mvn clean install
```

### 3. Configure Your Experiment

See configuration templates and documentation:

```bash
# View config templates
ls flinksketch-bench/benchmarks/config/templates/aggregations/

# Read configuration guide
cat flinksketch-bench/benchmarks/config/README.md
```

Create or modify experiment configs in `flinksketch-bench/benchmarks/config/`.

### 4. Run Experiments

```bash
cd flinksketch-bench/benchmarks/experiments

# Run experiment with your config
uv run start_experiment.py --config ../config/experiment_config.yaml
```

### 5. Analyze Results

Post-process and visualize experiment results:

```bash
uv run post_experiment.py \
  --experiments_dir experiments/your_experiment_folder \
  --output_dir output/analysis_results \
  --compare_num 3 \
  --memory-source monitor
```

**Flags**:
- `--experiments_dir`: Path to directory containing experiment result folders
- `--output_dir`: Directory where visualizations and experiments analysis will be saved
- `--compare_num`: Comparison strategy - `2` for pairwise comparison, `3` to compare both datasketches and custom implementations against baseline
- `--memory-source`: Memory data source - `none` (skip memory analysis), `monitor` (JVM memory from job_monitor.json), or `sink` (fine-grained sketch memory from Flink output)

Results include accuracy metrics, performance comparisons, and visualizations.

## Module Structure

```
flinksketch-bench/
├── benchmarks/
│   ├── config/          # Experiment configuration files and templates
│   ├── experiments/     # Python scripts for running and analyzing experiments
│   ├── setup/          # Environment setup scripts
│   └── sweeps/         # Automated parameter sweep scripts
└── src/main/java/      # Baseline implementations and benchmarking utilities
    └── dev/projectasap/flinksketch/
        ├── DataStreamJob.java    # Main Flink job entry point
        ├── datamodel/           # Benchmarking-specific data models
        ├── sinks/              # Custom sinks for experiment output
        ├── sketches/baseline/  # Exact algorithms for comparison
        ├── utils/              # Benchmarking utilities
        └── windowfunctions/    # Custom window functions
```
