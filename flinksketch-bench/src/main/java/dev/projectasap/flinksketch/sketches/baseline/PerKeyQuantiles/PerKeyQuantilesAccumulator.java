/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.sketches.baseline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.projectasap.flinksketch.datamodel.Summary;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Accumulator for exact per-key quantile computation. Maintains separate value lists for each key
 * to compute exact quantiles independently per key.
 */
public class PerKeyQuantilesAccumulator implements Summary<PerKeyQuantilesAccumulator> {
  public Map<String, List<Float>> perKeyValues;
  private long count;

  public PerKeyQuantilesAccumulator() {
    this.perKeyValues = new HashMap<>();
  }

  @Override
  public void add(String key, Integer value) {
    perKeyValues.computeIfAbsent(key, k -> new ArrayList<>()).add(value.floatValue());
    count += 1;
  }

  @Override
  public PerKeyQuantilesAccumulator merge(PerKeyQuantilesAccumulator other) {
    PerKeyQuantilesAccumulator merged = new PerKeyQuantilesAccumulator();
    merged.perKeyValues.putAll(this.perKeyValues);

    for (Map.Entry<String, List<Float>> entry : other.perKeyValues.entrySet()) {
      merged
          .perKeyValues
          .computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
          .addAll(entry.getValue());
    }

    return merged;
  }

  /**
   * Computes the quantile value at the specified rank for a specific key.
   *
   * @param key the key to query
   * @param quantile the quantile (0.0 to 1.0) at which to compute the value
   * @return the quantile value at the specified rank for the key
   */
  public float getQuantile(String key, double quantile) {
    List<Float> values = perKeyValues.get(key);
    if (values == null || values.isEmpty()) {
      return Float.NaN;
    }

    List<Float> sorted = new ArrayList<>(values);
    Collections.sort(sorted);

    if (quantile <= 0.0) {
      return sorted.get(0);
    }
    if (quantile >= 1.0) {
      return sorted.get(sorted.size() - 1);
    }

    double index = quantile * (sorted.size() - 1);
    int lowerIndex = (int) Math.floor(index);
    int upperIndex = (int) Math.ceil(index);

    if (lowerIndex == upperIndex) {
      return sorted.get(lowerIndex);
    }

    double weight = index - lowerIndex;
    return (float) (sorted.get(lowerIndex) * (1 - weight) + sorted.get(upperIndex) * weight);
  }

  @Override
  public byte[] serializeToBytes() {
    // Calculate buffer size
    int totalSize = Integer.BYTES; // number of keys
    for (Map.Entry<String, List<Float>> entry : perKeyValues.entrySet()) {
      byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
      totalSize += Integer.BYTES + keyBytes.length; // key length + key
      totalSize += Integer.BYTES + (entry.getValue().size() * Float.BYTES); // value count + values
    }

    ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(perKeyValues.size());

    for (Map.Entry<String, List<Float>> entry : perKeyValues.entrySet()) {
      byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
      buffer.putInt(keyBytes.length);
      buffer.put(keyBytes);

      buffer.putInt(entry.getValue().size());
      for (Float value : entry.getValue()) {
        buffer.putFloat(value);
      }
    }

    return buffer.array();
  }

  @Override
  public JsonNode serializeToJson() {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode root = mapper.createObjectNode();

    for (Map.Entry<String, List<Float>> entry : perKeyValues.entrySet()) {
      ArrayNode valuesArray = mapper.createArrayNode();
      for (Float value : entry.getValue()) {
        valuesArray.add(value);
      }
      root.set(entry.getKey(), valuesArray);
    }

    return root;
  }

  /**
   * Execute query operations on the per-key quantiles data.
   *
   * @param params JsonNode containing query parameters - "key" for the key to query, "rank" for the
   *     quantile rank (0.0-1.0)
   */
  @Override
  public JsonNode query(JsonNode params) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode result = mapper.createObjectNode();

    if (params == null || !params.has("key") || !params.has("rank")) {
      ObjectNode error = mapper.createObjectNode();
      error.put("error", "Query must contain 'key' and 'rank' parameters");
      return error;
    }

    String key = params.get("key").asText();
    double rank = params.get("rank").asDouble();
    float quantileValue = getQuantile(key, rank);
    result.put(key, quantileValue);

    return result;
  }

  @Override
  public long get_memory() {
    long size = 0;
    for (Map.Entry<String, List<Float>> entry : perKeyValues.entrySet()) {
      size += entry.getKey().length() * Character.BYTES;
      size += entry.getValue().size() * Float.BYTES;
    }
    return size;
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
