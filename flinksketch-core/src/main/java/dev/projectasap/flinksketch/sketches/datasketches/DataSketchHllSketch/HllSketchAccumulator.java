/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.sketches.datasketches;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.projectasap.flinksketch.datamodel.Summary;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.Union;

/**
 * Accumulator for Apache DataSketches HyperLogLog sketch. Uses HllSketch from Apache DataSketches
 * library for cardinality estimation.
 */
public class HllSketchAccumulator implements Summary<HllSketchAccumulator> {
  public HllSketch sketch;
  public final int lgK;

  // Constructor for accumulator
  public HllSketchAccumulator(int lgK) {
    this.sketch = new HllSketch(lgK);
    this.lgK = lgK;
  }

  @Override
  public void add(String key, Integer value) {
    // HLL tracks cardinality of keys
    sketch.update(key);
  }

  @Override
  public HllSketchAccumulator merge(HllSketchAccumulator other) {
    Union union = new Union(this.lgK);
    union.update(this.sketch);
    union.update(other.sketch);
    this.sketch = union.getResult();
    return this;
  }

  @Override
  public byte[] serializeToBytes() {
    byte[] serializedSketch = sketch.toCompactByteArray();
    return serializedSketch;
  }

  @Override
  public String serializeToString() {
    return sketch.toString();
  }

  @Override
  public JsonNode serializeToJson() {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode rootNode = objectMapper.createObjectNode();
    rootNode.put("lgK", lgK);
    rootNode.put("sketch", sketch.toString());
    return rootNode;
  }

  /**
   * Execute query operations on the HLL sketch. Returns the estimated distinct count.
   *
   * @param params JsonNode containing query parameters (optional)
   */
  @Override
  public JsonNode query(JsonNode params) {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode queryResult = objectMapper.createObjectNode();

    // HLL sketch provides cardinality estimation
    double estimate = sketch.getEstimate();
    queryResult.put("distinct_count", estimate);

    return queryResult;
  }

  @Override
  public long get_memory() {
    return sketch.getCompactSerializationBytes();
  }
}
