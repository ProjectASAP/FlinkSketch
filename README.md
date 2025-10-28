<h1 align="center">FlinkSketch: Democratizing the Benefits of Sketches for the Flink Community</h1>

## Overview
FlinkSketch is a library of sketching algorithms for Flink. Currently, it can be used in Flink's DataStream API.
<!-- 2-3 sentence description of what problem this library solves -->

## Quick Start

### 1. Clone and Install FlinkSketch

```bash
# Clone the repository
git clone https://github.com/ProjectASAP/FlinkSketch.git
cd FlinkSketch

# Build and install to local Maven repository
mvn clean install
```

### 2. Create a New Flink Project

```bash
# Create a new directory for your project
cd ..
mkdir my-flink-sketch-app
cd my-flink-sketch-app

# Create the basic Maven directory structure
mkdir -p src/main/java/com/mycompany/app
```

### 3. Add Dependencies to pom.xml

Create a `pom.xml` file in your project root:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.mycompany</groupId>
    <artifactId>my-flink-sketch-app</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>11</maven.compiler.source>
        <maven.compiler.target>11</maven.compiler.target>
        <flink.version>1.20.0</flink.version>
    </properties>

    <dependencies>
        <!-- FlinkSketch Core -->
        <dependency>
            <groupId>dev.projectasap</groupId>
            <artifactId>flinksketch-core</artifactId>
            <version>0.1</version>
        </dependency>

        <!-- Flink Dependencies -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.8.1</version>
            </plugin>
        </plugins>
    </build>
</project>
```

### 4. Write Your Flink Job

Copy the code from [QuickStart.java](flinksketch-examples/src/main/java/dev/projectasap/flinksketch/examples/quickstart/QuickStart.java) to `src/main/java/com/mycompany/app/App.java`.

**Key operations:**

Ingestion:
```java
// Apply CountMinSketch with bounded memory
DataStream<PrecomputedOutput> output = dataStream
    .keyBy(item -> 0)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .aggregate(
        new CountMinSketch("count", params),
        new KeyedWindowProcessor(config, "insertion", "countminsketch", false)
    );
```

Querying:
```java
// Query the sketch for specific keys
ObjectNode queryParams = objectMapper.createObjectNode();
queryParams.put("key", "apple");
JsonNode frequency = result.precompute.query(queryParams);
```

### 5. Compile and Run

```bash
# Compile your project
mvn clean compile

# Run your application
mvn exec:java -Dexec.mainClass="com.mycompany.app.App"
```

For more sketch types (quantiles, cardinality, top-K) and advanced usage, see the Usage Examples section below.

## Repository Structure

FlinkSketch is organized into three Maven modules:

- **flinksketch-core**: Core sketch implementations
  - `custom` - Custom sketch implementations (CountMinSketch, HydraKLL, Univmon, etc.)
  - `datasketches` - Apache DataSketches library wrappers
  - `ddsketch` - DDSketch library wrappers

- **flinksketch-bench**: Benchmarking infrastructure and baseline implementations
  - `baseline` - Exact algorithms for accuracy comparison
  - Config files for experiments
  - DataStreamJob for running benchmarks
  - For details, see the [flinksketch-bench README](flinksketch-bench/README.md)

- **flinksketch-examples**: Example code demonstrating sketch usage
  - For details, see the [flinksketch-examples README](flinksketch-examples/README.md)

## Prerequisites

- Java 11 or higher
- Apache Maven 3.x
- Apache Flink 1.20.0 (provided at runtime)

## Installation

FlinkSketch consists of three Maven modules. Most users will only need `flinksketch-core`, which contains all sketch implementations.

### Option 1: Use Core Library (Recommended for Most Users)

To use FlinkSketch in your project, first build and install it locally:

```bash
# Clone the repository
git clone https://github.com/ProjectASAP/FlinkSketch.git
cd FlinkSketch

# Build and install to local Maven repository
mvn install
```

Then add the dependency to your project's `pom.xml`:

```xml
<dependency>
  <groupId>dev.projectasap</groupId>
  <artifactId>flinksketch-core</artifactId>
  <version>0.1</version>
</dependency>
```

This gives you access to all sketch implementations including:
- Custom sketches (CountMinSketch, HydraKLL, Univmon)
- Apache DataSketches wrappers (HLL, KLL, Items)
- DDSketch wrappers

### Option 2: Build All Modules

If you want to run benchmarks or explore examples, build all modules:

```bash
# Clone the repository
git clone https://github.com/ProjectASAP/FlinkSketch.git
cd FlinkSketch

# Build all modules (core, bench, examples)
mvn clean install
```

This builds:
- `flinksketch-core` - Core sketch library
- `flinksketch-bench` - Benchmarking infrastructure
- `flinksketch-examples` - Usage examples

## Supported Analytics

FlinkSketch enables various streaming analytics capabilities through efficient sketching algorithms. Each analytics type is supported by one or more sketch implementations:

### Frequency Estimation
Approximate frequency counts for items in a stream.
- **CountMinSketch** (custom) - Approximate frequency counts with configurable accuracy-space tradeoff
- **CountSketch** (custom) - Frequency estimation with L2-heavy hitter tracking
- **Univmon** (custom) - Universal framework supporting multiple query types including frequency
- **DataSketchItemsSketch** (Apache DataSketches) - Frequent items tracking

### Cardinality Estimation
Count distinct elements in a stream.
- **DataSketchHllSketch** (Apache DataSketches) - HyperLogLog for approximate distinct counts
- **Univmon** (custom) - Universal monitoring framework supporting cardinality estimation

### Quantile Estimation
Compute percentiles and quantiles over streaming data.
- **HydraKLL** (custom) - Quantile estimation for different subpopulations of data
- **DataSketchKllFloatsSketch** (Apache DataSketches) - KLL quantiles sketch for floating-point values
- **DDSketchQuantile** (Datadog DDSketch) - Quantile estimation with relative-error guarantees, supports both bounded and unbounded memory

### Top-K / Heavy Hitters
Identify most frequent items in a stream.
- **CountSketch** (custom) - L2-heavy hitter tracking with configurable k
- **Univmon** (custom) - Universal monitoring framework supporting top-k queries
- **DataSketchItemsSketch** (Apache DataSketches) - Frequent items identification

### Multidimensional Queries
Analyze data across multiple dimensions or subpopulations.
- **HydraKLL** (custom) - Quantile estimation for different subpopulations of data

### Multiple Statistics per Sketch
Compute multiple analytics from a single sketch instance.
- **Univmon** (custom) - Universal monitoring framework supporting frequency, cardinality, and top-k queries simultaneously

All sketches implement Flink's `AggregateFunction` interface and can be used in streaming data pipelines. See the Usage Examples section below for integration details.

**For detailed sketch parameters, tuning guidance, and selection criteria, see [docs/SKETCH_REFERENCE.md](docs/SKETCH_REFERENCE.md).**

## Usage Examples

FlinkSketch includes comprehensive examples organized into categories:

### Quick Start
```bash
# See FlinkSketch's memory efficiency (Before vs After)
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.quickstart.AfterFlinkSketch"
```

### Basic Examples
```bash
# CountMinSketch for frequency estimation
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.basic.frequency.CountMinSketchInsertExample"

# DDSketch for quantile estimation
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.basic.quantile.DDSketchInsertExample"

# CountSketch for top-K heavy hitters
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.basic.topk.CountSketchBasicExample"

# Univmon for multiple query types (frequency, cardinality, top-K)
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.basic.multiple_statistics_per_sketch.UnivmonExample"
```

### Evaluation Examples
```bash
# Integration: Full Flink pipeline comparison
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.evaluation.integration.CountMinSketchIntegrationExample"

# Memory: Analyze memory footprint
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.evaluation.memory.DDSketchMemoryExample"

# Accuracy: Compare approximations vs exact results
mvn -q exec:exec -pl flinksketch-examples \
  -Dexample.mainClass="dev.projectasap.flinksketch.examples.evaluation.accuracy.CountMinSketchAccuracyExample"
```

**For complete examples, run commands, and learning path, see [flinksketch-examples/README.md](flinksketch-examples/README.md).**

## Verifying Installation

After installation, verify FlinkSketch is working correctly by running one of the example programs:

```bash
# Run the quick start example to see FlinkSketch in action
mvn -q exec:exec -pl flinksketch-examples -Dexample.mainClass="dev.projectasap.flinksketch.examples.quickstart.AfterFlinkSketch"

# Or run a simple CountMinSketch example
mvn -q exec:exec -pl flinksketch-examples -Dexample.mainClass="dev.projectasap.flinksketch.examples.basic.frequency.CountMinSketchInsertExample"
```

If the example runs successfully and prints frequency results, your installation is working correctly.

Alternatively, create a simple test project:

```bash
# In a new directory outside FlinkSketch
mkdir flinksketch-test && cd flinksketch-test

# Create a minimal pom.xml with flinksketch-core dependency
# Add a simple Java class that uses a sketch
# Run: mvn clean compile
```

## Developer Guide

### Adding a New Sketch

**Quick Overview**: To add a sketch, create two Java classes in `flinksketch-core`:

1. **Aggregate Function** - `flinksketch-core/src/main/java/dev/projectasap/flinksketch/sketches/{package}/YourSketch.java`
   - Implements `AggregateFunction<AtomicDataItem, YourSketchAccumulator, Summary>`
   - Constructor accepts `(String aggregationSubType, Map<String, String> parameters)`
   - Handles sketch initialization, adding elements, merging, and result extraction

2. **Accumulator** - `flinksketch-core/src/main/java/dev/projectasap/flinksketch/sketches/{package}/YourSketchAccumulator.java`
   - Implements `Summary<YourSketchAccumulator>`
   - Required methods:
     - `add(String key, Integer value)` - Insert elements
     - `merge(YourSketchAccumulator other)` - Merge two sketches
     - `query(JsonNode params)` - Answer queries
     - `serializeToJson()` / `serializeToBytes()` - Serialization
     - `get_memory()` - Memory tracking

**Packages**:
- `custom` - Custom implementations (CountMinSketch, HydraKLL)
- `datasketches` - Apache DataSketches wrappers
- `ddsketch` - DDSketch wrappers

**Benchmarking** (optional):
1. Create config: `flinksketch-bench/config/templates/aggregations/{package}/your_sketch_config.yaml`
2. Create baseline (if needed): `flinksketch-bench/src/main/java/...sketches/baseline/YourBaseline/`
3. Create experiment config: `flinksketch-bench/config/experiment_your_sketch.yaml`
4. Run: `python flinksketch-bench/benchmarks/experiments/start_experiment.py --config <config_file>`

For details, see the [flinksketch-bench README](flinksketch-bench/README.md)

**For detailed instructions**, including templates, query patterns, and adding new query types, see [docs/ADDING_SKETCHES.md](docs/ADDING_SKETCHES.md)

## Contributing

Contributions are welcome! We will be adding more detailed contribution guidelines soon.

In the meantime, feel free to:
- Open issues for bugs or feature requests
- Submit pull requests for improvements
- Reach out via the contact information below with questions

## Contact

We're happy to help you integrate FlinkSketch into your workloads and provide support for any questions you may have. Please reach out!

For questions, issues, or contributions:
- **Issues**: [GitHub Issues](https://github.com/ProjectASAP/FlinkSketch/issues)
- **Email**: [contact@projectasap.dev](mailto:contact@projectasap.dev)

## Contributors

<table>
  <tr>
    <td align="center" style="padding: 15px;">
      <a href="https://github.com/milindsrivastava1997">
        <img src="https://github.com/milindsrivastava1997.png" width="100px;" alt="Milind Srivastava"/><br />
      </a>
      <strong style="font-size: 16px;">Milind Srivastava</strong><br />
      <sub><a href="https://milindsrivastava1997.github.io/">Website</a> • <a href="https://github.com/milindsrivastava1997">GitHub</a></sub>
    </td>
    <td align="center" style="padding: 15px;">
      <a href="https://github.com/STWMichael">
        <img src="https://github.com/STWMichael.png" width="100px;" alt="Songting (Michael) Wang"/><br />
      </a>
      <strong style="font-size: 16px;">Songting Wang</strong><br />
      <sub><a href="https://www.linkedin.com/in/songting-wang-828453247">LinkedIn</a> • <a href="https://github.com/STWMichael">GitHub</a></sub>
    </td>
    <td align="center" style="padding: 15px;">
      <a href="https://github.com/varundonde">
        <img src="https://github.com/varundonde.png" width="100px;" alt="Varun Donde"/><br />
      </a>
      <strong style="font-size: 16px;">Varun Donde</strong><br />
      <sub><a href="https://www.linkedin.com/in/varun-donde">LinkedIn</a> • <a href="https://github.com/varundonde">GitHub</a></sub>
    </td>
    <td align="center" style="padding: 15px;">
      <a href="https://github.com/pareekshithkrishna">
        <img src="https://github.com/pareekshithkrishna.png" width="100px;" alt="Pareekshith Krishna"/><br />
      </a>
      <strong style="font-size: 16px;">Pareekshith Krishna</strong><br />
      <sub><a href="https://www.linkedin.com/in/pareekshith-krishna">LinkedIn</a> • <a href="https://github.com/pareekshithkrishna">GitHub</a></sub>
    </td>
  </tr>
</table>

## License

FlinkSketch is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.

Copyright 2025 ProjectASAP contributors
