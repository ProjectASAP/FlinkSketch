/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.sketches.baseline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.projectasap.flinksketch.datamodel.Summary;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Accumulator for exact frequency count computation. Maintains exact counts for all items using a
 * HashMap.
 */
public class ExactFrequencyAccumulator implements Summary<ExactFrequencyAccumulator> {
  public Map<String, Integer> frequencyCounts;

  public ExactFrequencyAccumulator() {
    this.frequencyCounts = new HashMap<>();
  }

  @Override
  public void add(String key, Integer value) {
    frequencyCounts.put(key, frequencyCounts.getOrDefault(key, 0) + value);
  }

  public void incrementCount(String key, int increment) {
    frequencyCounts.put(key, frequencyCounts.getOrDefault(key, 0) + increment);
  }

  @Override
  public ExactFrequencyAccumulator merge(ExactFrequencyAccumulator other) {
    ExactFrequencyAccumulator merged = new ExactFrequencyAccumulator();
    merged.frequencyCounts.putAll(this.frequencyCounts);

    for (Map.Entry<String, Integer> entry : other.frequencyCounts.entrySet()) {
      merged.frequencyCounts.put(
          entry.getKey(),
          merged.frequencyCounts.getOrDefault(entry.getKey(), 0) + entry.getValue());
    }

    return merged;
  }

  @Override
  public byte[] serializeToBytes() {
    int totalSize = Integer.BYTES; // for map size
    for (Map.Entry<String, Integer> entry : frequencyCounts.entrySet()) {
      totalSize +=
          Integer.BYTES
              + entry.getKey().getBytes().length
              + Integer.BYTES; // key length + key + value
    }

    ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(frequencyCounts.size());

    for (Map.Entry<String, Integer> entry : frequencyCounts.entrySet()) {
      byte[] keyBytes = entry.getKey().getBytes();
      buffer.putInt(keyBytes.length);
      buffer.put(keyBytes);
      buffer.putInt(entry.getValue());
    }

    return buffer.array();
  }

  @Override
  public JsonNode serializeToJson() {
    ObjectMapper objectMapper = new ObjectMapper();

    // Baseline: return raw data structure
    ObjectNode jsonNode = objectMapper.createObjectNode();
    for (Map.Entry<String, Integer> entry : frequencyCounts.entrySet()) {
      jsonNode.put(entry.getKey(), entry.getValue());
    }
    return jsonNode;
  }

  /**
   * Execute query operations on the frequency count data. Returns frequency for the specified key
   * (0 if key doesn't exist).
   */
  @Override
  public JsonNode query(JsonNode params) {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode queryResult = objectMapper.createObjectNode();

    if (params != null && params.has("key")) {
      String requestedKey = params.get("key").asText();
      Integer frequency = frequencyCounts.getOrDefault(requestedKey, 0);
      queryResult.put(requestedKey, frequency);
    }

    return queryResult;
  }

  @Override
  public long get_memory() {
    long approxSize = 0;
    for (Map.Entry<String, Integer> entry : frequencyCounts.entrySet()) {
      approxSize += entry.getKey().length() * Character.BYTES;
      approxSize += Integer.BYTES;
    }
    return approxSize;
  }

  @Override
  public Set<String> getKeySet() {
    return frequencyCounts.keySet();
  }
}
