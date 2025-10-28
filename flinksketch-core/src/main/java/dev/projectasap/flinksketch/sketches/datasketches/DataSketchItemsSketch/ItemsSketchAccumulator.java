/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.sketches.datasketches;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.projectasap.flinksketch.datamodel.Summary;
import org.apache.datasketches.frequencies.ItemsSketch;

/**
 * Accumulator for Apache DataSketches Frequent Items sketch. Provides approximate frequent items
 * tracking.
 */
public class ItemsSketchAccumulator implements Summary<ItemsSketchAccumulator> {
  public ItemsSketch<String> sketch;
  private final int maxMapSize;

  public ItemsSketchAccumulator(int maxMapSize) {
    this.maxMapSize = maxMapSize;
    this.sketch = new ItemsSketch<>(maxMapSize);
  }

  @Override
  public void add(String key, Integer value) {
    sketch.update(key);
  }

  @Override
  public ItemsSketchAccumulator merge(ItemsSketchAccumulator other) {
    ItemsSketchAccumulator merged = new ItemsSketchAccumulator(this.maxMapSize);
    merged.sketch.merge(this.sketch);
    merged.sketch.merge(other.sketch);
    return merged;
  }

  @Override
  public byte[] serializeToBytes() {
    // Use DataSketches' built-in ArrayOfStringsSerDe
    return sketch.toByteArray(new org.apache.datasketches.common.ArrayOfStringsSerDe());
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
    byte[] sketchBytes =
        sketch.toByteArray(new org.apache.datasketches.common.ArrayOfStringsSerDe());
    rootNode.put("sketch_bytes", java.util.Base64.getEncoder().encodeToString(sketchBytes));

    return rootNode;
  }

  /**
   * Execute query operations on the Items sketch. Returns frequency for the specified key, or empty
   * if no key provided.
   */
  @Override
  public JsonNode query(JsonNode params) {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode queryResult = objectMapper.createObjectNode();

    // Only return result if specific key is requested
    if (params != null && params.has("key")) {
      String requestedKey = params.get("key").asText();
      long frequency = sketch.getEstimate(requestedKey);
      queryResult.put(requestedKey, frequency);
    }

    return queryResult;
  }

  @Override
  public long get_memory() {
    // Use the DataSketches Items sketch's memory usage
    // ItemsSketch doesn't have getSerializedSizeBytes, so estimate based on serialized bytes
    return sketch.toByteArray(new org.apache.datasketches.common.ArrayOfStringsSerDe()).length;
  }
}
