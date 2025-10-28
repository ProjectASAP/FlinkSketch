/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.sketches.datasketches;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.projectasap.flinksketch.datamodel.Summary;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;

/**
 * Accumulator for Apache DataSketches KLL quantiles sketch. Provides approximate quantile
 * computation for floating-point values.
 */
public class KllFloatsSketchAccumulator implements Summary<KllFloatsSketchAccumulator> {
  public KllFloatsSketch sketch;
  private final int sketchSize;

  public KllFloatsSketchAccumulator(int sketchSize) {
    this.sketchSize = sketchSize;
    this.sketch = KllFloatsSketch.newHeapInstance(sketchSize);
  }

  @Override
  public void add(String key, Integer value) {
    // KLL tracks quantiles of values, ignoring keys
    sketch.update(value.floatValue());
  }

  @Override
  public KllFloatsSketchAccumulator merge(KllFloatsSketchAccumulator other) {
    KllFloatsSketchAccumulator merged = new KllFloatsSketchAccumulator(this.sketchSize);
    merged.sketch.merge(this.sketch);
    merged.sketch.merge(other.sketch);
    return merged;
  }

  @Override
  public byte[] serializeToBytes() {
    return sketch.toByteArray();
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
    byte[] sketchBytes = sketch.toByteArray();
    rootNode.put("sketch_bytes", java.util.Base64.getEncoder().encodeToString(sketchBytes));

    return rootNode;
  }

  public float queryQuantile(double rank, QuantileSearchCriteria criteria) {
    return sketch.getQuantile(rank, criteria);
  }

  /**
   * Execute query operations on the KLL sketch. Returns quantile value for the specified rank.
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
        float quantile = queryQuantile(rank, QuantileSearchCriteria.INCLUSIVE);
        queryResult.put("rank_" + rank, quantile);
      }
    }

    return queryResult;
  }

  @Override
  public long get_memory() {
    // Use the DataSketches KLL sketch's memory usage
    return sketch.getSerializedSizeBytes();
  }
}
