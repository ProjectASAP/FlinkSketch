# FlinkSketch Examples

This module contains examples demonstrating how to use FlinkSketch for approximate stream processing in Apache Flink.

## Table of Contents

- [Quick Start](#quick-start)
- [Examples by Category](#examples-by-category)
  - [Quickstart Examples](#quickstart-examples)
  - [Basic Examples](#basic-examples)
  - [Evaluation Examples](#evaluation-examples)
- [Examples by Sketch Type](#examples-by-sketch-type)
- [Running Examples](#running-examples)

---

## Quick Start

### See the Benefit (Before & After)

Compare the same pipeline with and without FlinkSketch to see the memory efficiency gains:

```bash
# WITHOUT FlinkSketch - stores all data in memory
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.quickstart.BeforeFlinkSketch"

# WITH FlinkSketch - uses space-efficient sketches
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.quickstart.AfterFlinkSketch"
```

---

## Examples by Category

### Quickstart Examples

**Purpose:** Quick demonstrations of FlinkSketch's value proposition.

| Example | Description | Path |
|---------|-------------|------|
| BeforeFlinkSketch | Shows memory overhead without FlinkSketch | `examples.quickstart.BeforeFlinkSketch` |
| AfterFlinkSketch | Shows memory efficiency with FlinkSketch | `examples.quickstart.AfterFlinkSketch` |

**Run command:**
```bash
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.quickstart.AfterFlinkSketch"
```

---

### Basic Examples

**Purpose:** Learn individual sketch usage with minimal code examples.

#### Single Statistic per Sketch

Examples where each sketch computes one type of statistic:

**Frequency Estimation**

```bash
# CountMinSketch - Classic frequency estimation
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.basic.frequency.CountMinSketchInsertExample"
```

**Quantile Estimation**

```bash
# DDSketch - Relative-error quantile estimation
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.basic.quantile.DDSketchInsertExample"
```

**Top-K / Heavy Hitters** (What are the most frequent X?)

```bash
# CountSketch with heap - L2-heavy hitter tracking
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.basic.topk.CountSketchBasicExample"
```

#### Multiple Statistics per Sketch

Examples where one sketch answers multiple query types:

**Univmon - Universal Monitoring Framework**

Demonstrates frequency, cardinality, and top-K queries from a single sketch:

```bash
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.basic.multiple_statistics_per_sketch.UnivmonExample"
```

#### Multidimensional Queries

Examples for per-key statistics across multiple dimensions:

**HydraKLL - Per-Key Quantiles**

```bash
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.basic.multidimensional_queries.HydraKLLInsertExample"
```

---

### Evaluation Examples

**Purpose:** Measure sketch characteristics and compare against exact computation.

#### Integration Examples

Full Flink pipeline examples comparing sketch-based vs exact computation:

```bash
# CountMinSketch integration
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.evaluation.integration.CountMinSketchIntegrationExample"

# DDSketch integration
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.evaluation.integration.DDSketchIntegrationExample"
```

#### Accuracy Examples

Compare sketch approximations vs exact results:

```bash
# CountMinSketch accuracy
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.evaluation.accuracy.CountMinSketchAccuracyExample"

# DDSketch accuracy
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.evaluation.accuracy.DDSketchAccuracyExample"
```

#### Memory Examples

Analyze memory footprint vs exact storage:

```bash
# CountMinSketch memory
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.evaluation.memory.CountMinSketchMemoryExample"

# DDSketch memory
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.evaluation.memory.DDSketchMemoryExample"

# Univmon memory
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.evaluation.memory.UnivmonMemoryExample"

# DataSketches HLL memory
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.evaluation.memory.DataSketchHLLMemoryExample"

# DataSketches KLL memory
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.evaluation.memory.DataSketchKLLMemoryExample"

# DataSketches Items memory
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.evaluation.memory.DataSketchItemsMemoryExample"
```

---

## Examples by Sketch Type

### CountMinSketch

| Type | Example | Path |
|------|---------|------|
| Basic | Frequency estimation | `examples.basic.frequency.CountMinSketchInsertExample` |
| Integration | Full pipeline comparison | `examples.evaluation.integration.CountMinSketchIntegrationExample` |
| Accuracy | Accuracy analysis | `examples.evaluation.accuracy.CountMinSketchAccuracyExample` |
| Memory | Memory footprint | `examples.evaluation.memory.CountMinSketchMemoryExample` |

### CountSketch

| Type | Example | Path |
|------|---------|------|
| Basic | Top-K heavy hitters | `examples.basic.topk.CountSketchBasicExample` |

### DDSketch

| Type | Example | Path |
|------|---------|------|
| Basic | Quantile estimation | `examples.basic.quantile.DDSketchInsertExample` |
| Integration | Full pipeline comparison | `examples.evaluation.integration.DDSketchIntegrationExample` |
| Accuracy | Accuracy analysis | `examples.evaluation.accuracy.DDSketchAccuracyExample` |
| Memory | Memory footprint | `examples.evaluation.memory.DDSketchMemoryExample` |

### Univmon

| Type | Example | Path |
|------|---------|------|
| Basic | Multi-query (freq/card/topk) | `examples.basic.multiple_statistics_per_sketch.UnivmonExample` |
| Memory | Memory footprint | `examples.evaluation.memory.UnivmonMemoryExample` |

**Note:** Individual Univmon integration examples (Frequency, Cardinality, TopK) are deprecated. Use the unified `UnivmonExample` instead.

### HydraKLL

| Type | Example | Path |
|------|---------|------|
| Basic | Per-key quantiles | `examples.basic.multidimensional_queries.HydraKLLInsertExample` |

### DataSketches (HLL, KLL, Items)

| Sketch | Type | Example | Path |
|--------|------|---------|------|
| HLL | Memory | Cardinality memory | `examples.evaluation.memory.DataSketchHLLMemoryExample` |
| KLL | Memory | Quantile memory | `examples.evaluation.memory.DataSketchKLLMemoryExample` |
| Items | Memory | Top-K memory | `examples.evaluation.memory.DataSketchItemsMemoryExample` |

---

## Running Examples

### Basic Run Command

All examples follow this pattern:

```bash
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="<full.package.path.ExampleName>"
```

### Example: Running CountMinSketch

```bash
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.basic.frequency.CountMinSketchInsertExample"
```
