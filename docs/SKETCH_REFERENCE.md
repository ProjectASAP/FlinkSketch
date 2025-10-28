# Sketch Reference Guide

This guide provides documentation on sketch implementations in FlinkSketch. It assumes you are familiar with the main [README](../README.md) and Flink's DataStream API.

## What are Sketches?

Sketches are probabilistic data structures that provide approximate answers to queries using bounded memory. They are essential for streaming analytics where:
- Exact computation would require unbounded memory
- Approximate answers with known error bounds are acceptable
- Fast query response times are critical
- Multiple parallel streams need to be merged

FlinkSketch implements sketches as Flink `AggregateFunction`s, making them:
- **Mergeable**: Combine results from parallel streams
- **Bounded memory**: Fixed or configurable maximum size
- **Fast**: Constant or logarithmic query time
- **Distributable**: Work seamlessly in Flink's distributed environment

---

## Quick Selection Guide

### Sketch Selection by Use Case

| Use Case | Recommended Sketches | Notes |
|----------|---------------------|-------|
| Frequency counting | CountMinSketch, CountSketch |  |
| Global quantiles | DDSketchQuantile, DataSketchKllFloatsSketch |  |
| Per-key quantiles | HydraKLL | Provides quantiles for specific keys/subpopulations of data |
| Cardinality | DataSketchHllSketch |  |
| Top-K heavy hitters | CountSketch, Univmon |  |
| Multiple query types | Univmon | Single sketch for frequency + cardinality + top-k |
| Frequent items | DataSketchItemsSketch | |

---

## Frequency Estimation Sketches

### CountMinSketch (custom)

**Purpose**: Approximate frequency counting with configurable accuracy-space tradeoff

**When to use**: Simple frequency queries, one-sided error acceptable (never underestimates)

**Parameters**:
- `rows` (int, required): Number of hash functions
  - More rows = better accuracy
  - Typical: 3-7 rows
- `cols` (int, required): Hash table width per row
  - **Must be power of 2**
  - More cols = better accuracy
  - **Constraint**: `rows < cols`
  - Typical: 2048-8192 for moderate accuracy
- `subType` (string, optional): "count" or "sum"

**Parameter Setup Example**:
```java
Map<String, String> params = new HashMap<>();
params.put("rows", "5");
params.put("cols", "4096");
new CountMinSketch("count", params);
```

**Query Format**: `{"key": "item_name"}` → Returns frequency count

**Memory**: `O(rows × cols × 4 bytes)` (integer array)

**Tuning Guidance**:
- Start with rows=5, cols=4096 for general use
- Increase cols for higher accuracy (doubles cols ≈ halves error)
- Increase rows for better worst-case guarantees

---

### CountSketch (custom)

**Purpose**: Frequency estimation + L2-heavy hitter tracking

**When to use**: Need both frequency counts AND top-k items, can handle small overestimation/underestimation

**Parameters**:
- `rows` (int, required): Number of hash functions (typical: 3-7)
- `cols` (int, required): Hash table width, **must be power of 2**, `rows < cols`
- `trackHH` (boolean, required): Enable heavy hitter tracking
- `k` (int, required if trackHH=true): Number of heavy hitters to track
- `subType` (string, optional): Aggregation type

**Parameter Setup Example**:
```java
Map<String, String> params = new HashMap<>();
params.put("rows", "5");
params.put("cols", "4096");
params.put("trackHH", "true");
params.put("k", "10");
new CountSketch("", params);
```

**Query Format**:
- Frequency: `{"key": "item_name"}` → frequency
- Top-k: `{"k": 10}` → list of heavy hitters

**Memory**: Base `O(rows × cols × 4 bytes)` + heavy hitter tracking overhead

**Tuning Guidance**:
- Use trackHH=true only if you need top-k queries
- k should be much smaller than expected distinct items
- Similar rows/cols tuning as CountMinSketch

---

### Univmon (custom)

**Purpose**: Universal monitoring - frequency, cardinality, AND top-k from single sketch

**When to use**: Need multiple analytics types simultaneously, willing to trade some space for flexibility

**Parameters**:
- `rows` (int, required): Number of hash functions (typical: 3-7)
- `cols` (int, required): Hash table width, **must be power of 2**, `rows < cols`
- `levels` (int, required): Number of recursive levels = log(n) where n is stream length
- `k` (int, required): Number of L2 heavy hitters to track

**Parameter Setup Example**:
```java
Map<String, String> params = new HashMap<>();
params.put("rows", "5");
params.put("cols", "4096");
params.put("levels", "10");  // For ~10^3 items
params.put("k", "10");
new Univmon("", params);
```

**Query Format**: Supports multiple query types (see usage examples)

**Memory**: `O(levels × rows × cols × 4 bytes)`

**Tuning Guidance**:
- levels = ceil(log2(expected_stream_length))
- Higher levels = more memory but handles longer streams
- Use when you need 2+ of: frequency, cardinality, top-k

**Reference**: "One Sketch to Rule Them All: Rethinking Network Flow Monitoring with UnivMon"
- Liu et al., SIGCOMM 2016
- https://dl.acm.org/doi/pdf/10.1145/2934872.2934906

---

### DataSketchItemsSketch (Apache DataSketches)

**Purpose**: Frequent items tracking using Apache DataSketches library

**When to use**: Need frequent items tracking from Apache DataSketches library

**Parameters**:
- `maxMapSize` (int, required): Maximum size of internal map
  - Larger = more accuracy but more memory
  - Typical: 128-2048

**Parameter Setup Example**:
```java
Map<String, String> params = new HashMap<>();
params.put("maxMapSize", "256");
new DataSketchItemsSketch("", params);
```

**Query Format**: Supports frequent items queries

**Memory**: `O(maxMapSize)`

**Tuning Guidance**:
- Start with maxMapSize=256
- Increase if you need to track many frequent items accurately

**External Reference**: https://datasketches.apache.org/docs/Frequency/FrequentItemsOverview.html

---

## Quantile Estimation Sketches

### DDSketchQuantile (Datadog DDSketch)

**Purpose**: Approximate quantile estimation with **relative-error guarantees**

**When to use**: Need percentiles (p50, p95, p99) with known accuracy bounds

**Parameters**:
- `relativeAccuracy` (double, required): Relative error bound (e.g., 0.01 = 1% error)
  - Smaller = more accurate but more memory
  - Typical: 0.01-0.05
- `unbounded` (boolean, optional, default=false): Use unbounded (growing) sketch
- `maxNumBins` (int, required if unbounded=false): Maximum number of bins for bounded mode
  - Controls memory limit
  - Typical: 1024-4096

**Parameter Setup Example (Bounded)**:
```java
Map<String, String> params = new HashMap<>();
params.put("relativeAccuracy", "0.01");  // 1% error
params.put("unbounded", "false");
params.put("maxNumBins", "2048");
new DDSketchQuantile("", params);
```

**Parameter Setup Example (Unbounded)**:
```java
Map<String, String> params = new HashMap<>();
params.put("relativeAccuracy", "0.01");
params.put("unbounded", "true");
// maxNumBins not needed
new DDSketchQuantile("", params);
```

**Query Format**: `{"quantile": 0.95}` → Returns 95th percentile value

**Memory**:
- Bounded: Limited by maxNumBins
- Unbounded: Grows with data distribution range

**Tuning Guidance**:
- Use bounded mode for production with memory constraints
- relativeAccuracy=0.01 is good balance for most use cases
- Decrease relativeAccuracy if you need tighter error bounds on tail latencies

**External Reference**: https://www.datadoghq.com/blog/engineering/computing-accurate-percentiles-with-ddsketch/

---

### DataSketchKllFloatsSketch (Apache DataSketches)

**Purpose**: Approximate global quantile estimation using KLL algorithm

**When to use**: Need quantile sketch with rank-error (not relative-error) guarantees

**Parameters**:
- `k` (int, required): Sketch size parameter
  - Larger = more accuracy but more memory
  - Error bound ≈ 1/sqrt(k)
  - Typical: 200-400
  - Must be at least 8

**Parameter Setup Example**:
```java
Map<String, String> params = new HashMap<>();
params.put("k", "200");  // ~0.7% rank error
new DataSketchKllFloatsSketch("", params);
```

**Query Format**: `{"quantile": 0.95}` → Returns 95th percentile value

**Memory**: `O(k)` with compact storage

**Tuning Guidance**:
- k=200 gives ~0.7% rank error (good default)
- k=400 gives ~0.5% rank error (higher accuracy)
- Double k to reduce error by ~30%

**External Reference**: https://datasketches.apache.org/docs/Quantiles/KLLSketch.html

---

### HydraKLL (custom)

**Purpose**: **Per-key quantile estimation** - compute quantiles independently for each key/subpopulation

**When to use**: Need quantiles broken down by dimensions (e.g., latency per service, per region)

**Parameters**:
- `rows` (int, required): Number of hash functions (typical: 3-7)
- `cols` (int, required): Hash table width for key distribution
- `k` (int, required): KLL sketch size per key-bucket

**Parameter Setup Example**:
```java
Map<String, String> params = new HashMap<>();
params.put("rows", "5");
params.put("cols", "4096");
params.put("k", "200");  // Per-bucket KLL size
new HydraKLL("", params);
```

**Query Format**: `{"key": "service_A", "quantile": 0.95}` → Returns p95 for service_A

**Memory**: `O(rows × cols × k)` - significantly larger than global quantile sketches

**Tuning Guidance**:
- rows/cols control key distribution (similar to CountMinSketch)
- k controls accuracy of quantiles within each key
- Trade-off: Larger cols = better key separation, larger k = better quantile accuracy

**Reference**: "Enabling efficient and general subpopulation analytics in multidimensional data streams"
- Zhang et al., VLDB 2022
- https://dl.acm.org/doi/abs/10.14778/3551793.3551867

---

## Cardinality Estimation Sketches

### DataSketchHllSketch (Apache DataSketches)

**Purpose**: Approximate distinct count using HyperLogLog algorithm

**When to use**: Need to count unique elements (e.g., unique users, unique IPs)

**Parameters**:
- `lgK` (int, required): log2 of HLL array size
  - lgK=12 → 4096 buckets (~1.6% error)
  - lgK=14 → 16384 buckets (~0.8% error)
  - Typical: 12-16
  - Valid range: 4-21

**Parameter Setup Example**:
```java
Map<String, String> params = new HashMap<>();
params.put("lgK", "12");  // 1.6% error
new DataSketchHllSketch("", params);
```

**Query Format**: `{}` → Returns distinct count estimate

**Memory**: ~`2^lgK` bytes (compact storage)

**Tuning Guidance**:
- lgK=12 is good default (1.6% error)
- Increase by 2 to halve the error (lgK=14 for 0.8% error)
- HLL is very space-efficient; even lgK=16 is only ~128KB

**External Reference**: https://datasketches.apache.org/docs/HLL/HLL.html

---

### Univmon (for cardinality)

See [Univmon](#univmon-custom) in the Frequency Estimation section - Univmon also supports cardinality estimation alongside frequency and top-k.

---

## Parameter Tuning Quick Reference

### Common Parameter Patterns

**rows/cols parameters** (CountMinSketch, CountSketch, Univmon, HydraKLL):
- **Constraints**:
  - cols must be a power of 2 (e.g., 1024, 2048, 4096, 8192)
  - rows must be less than cols
  - Both must be positive
- **Tuning**:
  - rows: 3-7 for most use cases (more rows = better worst-case accuracy)
  - cols: 2048-8192 typical (more cols = better average accuracy)
  - Doubling cols approximately halves error rate

**k parameters** (sketch size):
- KLL sketches (DataSketchKllFloatsSketch, HydraKLL):
  - Typical: 200-400
  - Error ≈ 1/sqrt(k)
- Heavy hitter tracking (CountSketch, Univmon):
  - k = number of top items to track
  - Should be << total distinct items

---

### Reasonable Defaults

These configurations provide good accuracy while keeping memory usage low:

**CountMinSketch**:
```java
Map<String, String> params = new HashMap<>();
params.put("rows", "5");
params.put("cols", "2048");
new CountMinSketch("count", params);
```

**CountSketch**:
```java
Map<String, String> params = new HashMap<>();
params.put("rows", "5");
params.put("cols", "2048");
params.put("trackHH", "true");
params.put("k", "10");
new CountSketch("", params);
```

**Univmon**:
```java
Map<String, String> params = new HashMap<>();
params.put("rows", "5");
params.put("cols", "1024");
params.put("levels", "10");
params.put("k", "10");
new Univmon("", params);
```

**DataSketchItemsSketch**:
```java
Map<String, String> params = new HashMap<>();
params.put("maxMapSize", "256");
new DataSketchItemsSketch("", params);
```

**DDSketchQuantile**:
```java
Map<String, String> params = new HashMap<>();
params.put("relativeAccuracy", "0.01");
params.put("unbounded", "false");
params.put("maxNumBins", "2048");
new DDSketchQuantile("", params);
```

**DataSketchKllFloatsSketch**:
```java
Map<String, String> params = new HashMap<>();
params.put("k", "200");
new DataSketchKllFloatsSketch("", params);
```

**HydraKLL**:
```java
Map<String, String> params = new HashMap<>();
params.put("rows", "5");
params.put("cols", "4096");
params.put("k", "200");
new HydraKLL("", params);
```

**DataSketchHllSketch**:
```java
Map<String, String> params = new HashMap<>();
params.put("lgK", "12");
new DataSketchHllSketch("", params);
```

---

## Further Resources

- **For Developers**: [ADDING_SKETCHES.md](ADDING_SKETCHES.md) - How to implement new sketches
- **Working Examples**: [flinksketch-examples/](../flinksketch-examples/) - Full Flink pipeline examples
- **Main Documentation**: [README.md](../README.md) - Installation, quick start, repository structure

**External Libraries**:
- Apache DataSketches: https://datasketches.apache.org/
- DDSketch: https://www.datadoghq.com/blog/engineering/computing-accurate-percentiles-with-ddsketch/

**Research Papers**:
- Univmon: "One Sketch to Rule Them All: Rethinking Network Flow Monitoring with UnivMon" (SIGCOMM 2016)
  - https://dl.acm.org/doi/pdf/10.1145/2934872.2934906
- HydraKLL: "Enabling efficient and general subpopulation analytics in multidimensional data streams" (VLDB 2022)
  - https://dl.acm.org/doi/abs/10.14778/3551793.3551867
