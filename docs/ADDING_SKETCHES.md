# Adding New Sketches to FlinkSketch

This guide explains how to add new sketch algorithms and test them against baselines.

## Part 1: Adding a Sketch (Required)

To add a new sketch algorithm, create two Java files in the appropriate package:

### 1. Create Aggregate Function

**Location**: `flinksketch-core/src/main/java/dev/projectasap/flinksketch/sketches/{package}/YourSketch/YourSketch.java`

**Packages**:
- `baseline` - Exact algorithms (HashMap-based, sorting-based) - **Note**: Located in `flinksketch-bench`
- `custom` - Custom sketch implementations (CountMinSketch, HydraKLL, etc.)
- `datasketches` - Apache DataSketches library wrappers
- `ddsketch` - DDSketch library wrappers

**Template**:
```java
package dev.projectasap.flinksketch.sketches.{package};

import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.Summary;
import java.util.Map;
import org.apache.flink.api.common.functions.AggregateFunction;

public class YourSketch
    implements AggregateFunction<AtomicDataItem, YourSketchAccumulator, Summary> {

  private final int param1;
  private final String param2;

  public YourSketch(String aggregationSubType, Map<String, String> parameters) {
    this.param1 = Integer.parseInt(parameters.get("param1"));
    this.param2 = parameters.get("param2");
  }

  @Override
  public YourSketchAccumulator createAccumulator() {
    return new YourSketchAccumulator(param1, param2);
  }

  @Override
  public YourSketchAccumulator add(AtomicDataItem value, YourSketchAccumulator acc) {
    acc.add(value.key, value.value);
    return acc;
  }

  @Override
  public YourSketchAccumulator merge(YourSketchAccumulator a, YourSketchAccumulator b) {
    return a.merge(b);
  }

  @Override
  public Summary getResult(YourSketchAccumulator acc) {
    return acc;
  }
}
```

### 2. Create Accumulator

**Location**: `flinksketch-core/src/main/java/dev/projectasap/flinksketch/sketches/{package}/YourSketch/YourSketchAccumulator.java`

**Template**:
```java
package dev.projectasap.flinksketch.sketches.{package};

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.projectasap.flinksketch.datamodel.Summary;

public class YourSketchAccumulator implements Summary<YourSketchAccumulator> {

  // Sketch state
  private int param1;
  private String param2;

  public YourSketchAccumulator(int param1, String param2) {
    this.param1 = param1;
    this.param2 = param2;
    // Initialize sketch data structures
  }

  @Override
  public void add(String key, Integer value) {
    // Add element to sketch
  }

  @Override
  public YourSketchAccumulator merge(YourSketchAccumulator other) {
    // Merge two sketches
    YourSketchAccumulator merged = new YourSketchAccumulator(param1, param2);
    // Perform merge logic
    return merged;
  }

  @Override
  public byte[] serializeToBytes() {
    // Serialize sketch to bytes (for byte output format)
    // Return empty array if not needed
    return new byte[0];
  }

  @Override
  public JsonNode serializeToJson() {
    // Serialize sketch state to JSON
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode node = mapper.createObjectNode();
    // Add sketch data
    return node;
  }

  @Override
  public JsonNode query(JsonNode params) {
    // Handle query requests
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode result = mapper.createObjectNode();

    if (params != null && params.has("key")) {
      String key = params.get("key").asText();
      // Perform query and add to result
    }

    return result;
  }

  @Override
  public long get_memory() {
    // Return memory usage in bytes
    return 0L;
  }
}
```

---

## Part 2: Benchmarking a Sketch (Optional)

To benchmark and compare your sketch against baselines:

### 1. Create Sketch Config

**Location**: `flinksketch-bench/config/templates/aggregations/{package}/your_sketch_config.yaml`

**Example**:
```yaml
aggregations:
  - aggregationId: 1
    aggregationSubType: ""           # Optional subtype (e.g., "count" vs "sum")
    aggregationType: YourSketch      # Must match class name
    aggregationPackage: custom       # baseline | custom | datasketch | ddsketch
    parameters:
      param1: 2048
      param2: value
    tumblingWindowSize: 10           # Window size in seconds
```

### 2. Create Baseline Implementation (if needed)

If your sketch approximates some query (quantiles, frequency, cardinality), create an exact baseline:

**Files**:
- `flinksketch-bench/src/main/java/dev/projectasap/flinksketch/sketches/baseline/YourBaseline/YourBaseline.java`
- `flinksketch-bench/src/main/java/dev/projectasap/flinksketch/sketches/baseline/YourBaseline/YourBaselineAccumulator.java`

**Pattern**:
- Use exact data structures (ArrayList, HashMap, TreeMap)
- Store all data for perfect accuracy
- Implement same `query()` interface as your sketch

### 3. Create Baseline Config

**Location**: `flinksketch-bench/config/templates/aggregations/baseline/your_baseline_config.yaml`

**Example**:
```yaml
aggregations:
  - aggregationId: 1
    aggregationSubType: ""
    aggregationType: YourBaseline
    aggregationPackage: baseline
    parameters: {}                    # Usually no parameters needed
    tumblingWindowSize: 10
```

### 4. Create Experiment Config

**Location**: `flinksketch-bench/config/experiment_your_sketch.yaml`

**Example**:
```yaml
experiments:
  resource:
    name: cpu                         # cpu | memory
    range: [2, 4, 8]                  # Values to test
    step: 2                           # Multiplier (for ranges like [2, 16])

  query:
    name: your_query_type             # Descriptive name
    baseline:
      config_file_path: 'flinksketch-bench/config/templates/aggregations/baseline/your_baseline_config.yaml'
    custom:
      config_file_path: 'flinksketch-bench/config/templates/aggregations/custom/your_sketch_config.yaml'
    keys: ['key1', 'key2', 'key3']    # Query keys for testing

flink:
  jar_path: 'flinksketch-bench/target/flinksketch-bench-0.1-shaded.jar'
  output_format: json                 # json | byte
  verbose: true
  pipeline: insertion_querying        # insertion | insertion_querying
  output_mode: query_memory           # sketch | query | memory | query_memory | sketch_query_memory
  parallelism: 2
  enable_merging: true                # Merge parallel window results

  datagen_rows_per_second: 10000
  datagen_duration_seconds: 60
  datagen_key_cardinality: 100

docker:
  default_resources:
    jobmanager:
      cpu: 1.0
      memory: '2g'
    taskmanager:
      cpu: 2.0
      memory: '4g'

monitoring:
  host: 'localhost'
  port: 8081
  interval: 10                        # Seconds between metrics collection
  timer: 120                          # Total experiment duration
  flink_bin: 'bin/flink'
  log_file_name: 'job_monitor.json'
  architecture: 'x86_64'              # x86_64 | arm64

paths:
  experiments_base: 'experiments/'
```

### 5. Run Experiments

```bash
# Set Flink home
export FLINK_HOME=/path/to/flink

# Build project
mvn clean package

# Run experiments
python flinksketch-bench/benchmarks/experiments/start_experiment.py --config flinksketch-bench/config/experiment_your_sketch.yaml
```

---

## Quick Reference: Package Names in Code

The `aggregationPackage` field in YAML maps to Java package names:

| YAML Value     | Java Package                                              | Module |
|----------------|-----------------------------------------------------------|--------|
| `baseline`     | `dev.projectasap.flinksketch.sketches.baseline` | `flinksketch-bench` |
| `custom`       | `dev.projectasap.flinksketch.sketches.custom`   | `flinksketch-core` |
| `datasketches`   | `dev.projectasap.flinksketch.sketches.datasketches` | `flinksketch-core` |
| `ddsketch`     | `dev.projectasap.flinksketch.sketches.ddsketch` | `flinksketch-core` |

The system uses reflection to instantiate: `{package}.{aggregationType}(aggregationSubType, parameters)`

---

## Common Query Patterns

**Global quantiles** (KLL, DDSketch):
- Input: `{"key": 0.5}` (rank from 0.0 to 1.0)
- Returns: Quantile value at that rank across all values

**Per-key quantiles** (HydraKLL, PerKeyQuantiles):
- Input: `{"key": "key1", "quantile": 0.5}`
- Returns: `{"key1": value}` - Quantile value for specific key's distribution

**Frequency** (CountMinSketch, FreqCount):
- Input: `{"key": "item1"}`
- Returns: Estimated/exact frequency count

**Cardinality** (HyperLogLog, Cardinality):
- Input: `{}`
- Returns: Number of distinct keys

**Top-K** (TopK):
- Input: `{"k": 10}`
- Returns: Top K most frequent items

---

## Part 3: Adding a New Query Type (Optional)

If your sketch answers a **new type of query** not already supported (e.g., neither freq, quantile, cardinality, topk, nor entropy), you'll need to add infrastructure support for the new query type.

### When Do You Need a New Query Type?

Add a new query type when:
- Your sketch answers a fundamentally different question than existing types
- The query parameters or response format differs significantly from existing types
- You need custom accuracy metrics for comparison against baselines

**Example:** `perKeyQuantile` was added because:
- It differs from `quantile` (global quantiles) by computing per-key distributions
- Query format: `{"key": "someKey", "quantile": 0.5}` vs `{"quantile": 0.5}`
- Response format: `{"someKey": value}` vs `{"rank_0.5": value}`

### Steps to Add a New Query Type

#### Step 1: Implement Query Interface in Java Accumulators

Both your sketch accumulator and baseline accumulator must implement the `query()` method:

**Location:** Your accumulator classes (both baseline and sketch)

**Pattern:**
```java
@Override
public JsonNode query(JsonNode params) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode result = mapper.createObjectNode();

    // Parse query parameters
    if (params != null && params.has("your_param")) {
        String yourParam = params.get("your_param").asText();

        // Compute result
        double queryResult = computeYourQuery(yourParam);
        result.put(yourParam, queryResult);

        return result;
    }

    // Error handling
    ObjectNode error = mapper.createObjectNode();
    error.put("error", "Query must contain 'your_param'");
    return error;
}
```

**Important:** Sketch and baseline must return the **same JSON structure** for comparison.

#### Step 2: Create Python Output Parser

**Location:** `flinksketch-bench/benchmarks/experiments/utils/post/output_parsers.py`

**Action:** Add a new parser class that extends `BaseOutputParser`:

```python
class YourQueryTypeOutputParser(BaseOutputParser):
    """Parser for your query type experiments."""

    def parse_precompute_record(self, precompute_data: Dict[str, Any]) -> Any:
        """Parse your query type precompute record."""
        return precompute_data

    def calculate_accuracy(
        self, sketch_data: List[Dict[str, Any]], baseline_data: List[Dict[str, Any]]
    ) -> Dict[str, float]:
        """
        Calculate accuracy metrics comparing sketch and baseline data.

        MUST return a dict with at minimum:
        - avg_relative_error: float
        - total_windows: int
        - avg_relative_error_percent: float
        - max_window_error: float
        - min_window_error: float
        """
        window_errors = []

        for sketch_rec, baseline_rec in zip(sketch_data, baseline_data):
            # Extract values from JSON response
            sketch_value = sketch_rec.get("your_field")
            baseline_value = baseline_rec.get("your_field")

            if sketch_value is None or baseline_value is None:
                continue

            # Calculate relative error
            if baseline_value == 0:
                if sketch_value != 0:
                    window_errors.append(1.0)  # 100% error
            else:
                window_errors.append(
                    abs(sketch_value - baseline_value) / abs(baseline_value)
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
            "max_window_error": max(window_errors),
            "min_window_error": min(window_errors),
        }
```

#### Step 3: Register in Parser Factory

**Location:** `flinksketch-bench/benchmarks/experiments/utils/post/output_parsers.py`

**Action:** Update the `create_parser()` function (around line 450):

```python
def create_parser(sketch_type: str, experiments_dir: str) -> BaseOutputParser:
    """Factory function to create the appropriate parser based on sketch type."""
    if sketch_type == "freq":
        return FreqSketchOutputParser(experiments_dir)
    elif sketch_type == "quantile":
        return QuantileSketchOutputParser(experiments_dir)
    elif sketch_type == "yourQueryType":  # ADD THIS
        return YourQueryTypeOutputParser(experiments_dir)
    # ... other types
    else:
        raise ValueError(
            f"Unknown sketch type: '{sketch_type}'. Supported types: freq, quantile, yourQueryType, ..."
        )
```

#### Step 4: Update Documentation

**Locations to update:**

1. **flinksketch-bench/src/main/java/dev/projectasap/flinksketch/DataStreamJob.java** (line 119) - Update query type list in comment:
```java
"Query name: freq, quantile, yourQueryType, cardinality, topk, entropy"
```

2. **flinksketch-bench/config/templates/experiment_config.yaml** (line 12) - Update template comment:
```yaml
# Query type: 'freq' | 'quantile' | 'yourQueryType' | 'cardinality' | 'topk' | 'entropy'
```

3. **ADDING_SKETCHES.md** (Common Query Patterns section) - Add your pattern:
```markdown
**Your Query Type** (YourSketch):
- Input: `{"your_param": "value"}`
- Returns: Your response format
```

#### Step 5: Create Experiment Config

**Location:** `flinksketch-bench/config/experiment_your_query.yaml`

**Example:**
```yaml
experiments:
  query:
    name: yourQueryType              # Use your new query type name
    baseline:
      config_file_path: 'flinksketch-bench/config/templates/aggregations/baseline/your_baseline_config.yaml'
    custom:
      config_file_path: 'flinksketch-bench/config/templates/aggregations/custom/your_sketch_config.yaml'
    keys: ['param1', 'param2']       # Query parameters to test

flink:
  output_format: json
  pipeline: insertion_querying       # Required for querying
  output_mode: query_memory
  # ... other flink config
```

#### Step 6: Run and Verify

```bash
# Build project
mvn clean package

# Run experiments
python flinksketch-bench/benchmarks/experiments/start_experiment.py --config flinksketch-bench/config/experiment_your_query.yaml

# Analyze results (will auto-detect 'yourQueryType' from folder names)
python flinksketch-bench/benchmarks/experiments/post_experiment.py --experiments_dir experiments/your_experiment_folder
```

### Query Type Naming Conventions

- **Use camelCase:** `perKeyQuantile`, `topK`, `freq`
- **Be descriptive:** Name should clearly indicate what the query computes
- **Must match across:**
  - Experiment config: `query.name: yourQueryType`
  - Folder names: `experiment_1_yourQueryType_...`
  - Python parser: `if sketch_type == "yourQueryType"`

### Complete Example: perKeyQuantile

This query type computes quantiles independently for each key (vs global quantiles).

**Java (HydraKLLAccumulator.java):**
```java
@Override
public JsonNode query(JsonNode params) {
    if (params != null && params.has("key") && params.has("quantile")) {
        String key = params.get("key").asText();
        float quantile = params.get("quantile").floatValue();
        queryResult.put(key, queryQuantile(key, quantile));
        return queryResult;
    }
    // error handling...
}
```

**Python (output_parsers.py):**
```python
class PerKeyQuantileSketchOutputParser(BaseOutputParser):
    def calculate_accuracy(self, sketch_data, baseline_data):
        # Compare per-key quantile values
        for sketch_rec, baseline_rec in zip(sketch_data, baseline_data):
            all_keys = set(sketch_rec.keys()) | set(baseline_rec.keys())
            for key in all_keys:
                error = abs(sketch_rec[key] - baseline_rec[key]) / baseline_rec[key]
                # ... accumulate errors
```

**Config (experiment_hydrakll.yaml):**
```yaml
experiments:
  query:
    name: perKeyQuantile
    keys: ['key1', 'key2', 'key3']  # Test these keys
```

---

## Common Query Patterns (Reference)

**Global quantiles** (KLL, DDSketch):
- Input: `{"quantile": 0.5}` (rank from 0.0 to 1.0)
- Returns: `{"rank_0.5": value}` - Quantile value at that rank across all values
