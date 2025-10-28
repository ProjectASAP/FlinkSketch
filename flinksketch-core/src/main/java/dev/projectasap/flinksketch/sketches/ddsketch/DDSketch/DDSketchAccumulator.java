/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.sketches.ddsketch;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketches;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.projectasap.flinksketch.datamodel.Summary;

/**
 * Accumulator for Datadog DDSketch. Provides approximate quantile computation with relative-error
 * guarantees for floating-point values. Supports both bounded (collapsing) and unbounded variants.
 */
public class DDSketchAccumulator implements Summary<DDSketchAccumulator> {
  public DDSketch sketch;
  private final double relativeAccuracy;
  private final int maxNumBins;
  private final boolean unbounded;
  private long count;

  /**
   * Constructor for bounded DDSketch (with collapsing).
   *
   * @param relativeAccuracy the relative accuracy guaranteed by the sketch
   * @param maxNumBins maximum number of bins (bounds memory usage)
   */
  public DDSketchAccumulator(double relativeAccuracy, int maxNumBins) {
    this(relativeAccuracy, maxNumBins, false);
  }

  /**
   * Constructor with option to use unbounded DDSketch.
   *
   * @param relativeAccuracy the relative accuracy guaranteed by the sketch
   * @param maxNumBins maximum number of bins (only used if unbounded is false)
   * @param unbounded if true, use logarithmicUnboundedDense; if false, use collapsingLowestDense
   */
  public DDSketchAccumulator(double relativeAccuracy, int maxNumBins, boolean unbounded) {
    this.relativeAccuracy = relativeAccuracy;
    this.maxNumBins = maxNumBins;
    this.unbounded = unbounded;

    if (unbounded) {
      // Create an unbounded DDSketch that grows indefinitely to accommodate input range
      // Uses exactly logarithmic mapping for constant-time insertion
      this.sketch = DDSketches.logarithmicUnboundedDense(relativeAccuracy);
    } else {
      // Create a constant-space DDSketch that collapses lowest bins when limit is reached
      // This bounds memory to approximately 8 * maxNumBins bytes
      this.sketch = DDSketches.collapsingLowestDense(relativeAccuracy, maxNumBins);
    }
  }

  @Override
  public void add(String key, Integer value) {
    // DDSketch tracks quantiles of values, ignoring keys
    sketch.accept(value.doubleValue());
    count += 1L;
  }

  @Override
  public DDSketchAccumulator merge(DDSketchAccumulator other) {
    DDSketchAccumulator merged =
        new DDSketchAccumulator(this.relativeAccuracy, this.maxNumBins, this.unbounded);
    // Copy this sketch and merge the other into it
    merged.sketch = this.sketch.copy();
    merged.sketch.mergeWith(other.sketch);
    merged.count = this.count + other.count;
    return merged;
  }

  @Override
  public byte[] serializeToBytes() {
    return sketch.serialize().array();
  }

  @Override
  public String serializeToString() {
    return sketch.toString();
  }

  @Override
  public JsonNode serializeToJson() {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode rootNode = objectMapper.createObjectNode();

    // Sketch: return bytes as Base64-encoded string
    byte[] sketchBytes = sketch.serialize().array();
    rootNode.put("sketch_bytes", java.util.Base64.getEncoder().encodeToString(sketchBytes));

    return rootNode;
  }

  /**
   * Execute query operations on the DDSketch. Returns quantile value for the specified rank.
   *
   * @param params JsonNode containing query parameters - rank should be the percentile (0.0-1.0)
   */
  @Override
  public JsonNode query(JsonNode params) {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode queryResult = objectMapper.createObjectNode();

    // Only return result if specific rank is requested
    if (params != null && params.has("rank")) {
      double rank = params.get("rank").asDouble();
      if (rank >= 0.0 && rank <= 1.0) {
        double quantile = sketch.getValueAtQuantile(rank);
        queryResult.put("rank_" + rank, quantile);
      }
    }

    return queryResult;
  }

  @Override
  public long get_memory() {
    // Return the serialized size in bytes without actually serializing
    // return count;
    return sketch.serializedSize();
  }

  /**
   * Returns the count of items added to this accumulator.
   *
   * @return the total count of items
   */
  public long get_count() {
    return count;
  }
}
