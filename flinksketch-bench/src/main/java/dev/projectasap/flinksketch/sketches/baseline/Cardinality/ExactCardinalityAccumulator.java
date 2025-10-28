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
import java.util.HashSet;
import java.util.Set;

/**
 * Accumulator for exact cardinality computation. Maintains a set of distinct keys to compute exact
 * distinct count.
 */
public class ExactCardinalityAccumulator implements Summary<ExactCardinalityAccumulator> {
  public Set<String> distinctKeys;

  public ExactCardinalityAccumulator() {
    this.distinctKeys = new HashSet<>();
  }

  @Override
  public void add(String key, Integer value) {
    distinctKeys.add(key);
  }

  @Override
  public ExactCardinalityAccumulator merge(ExactCardinalityAccumulator other) {
    ExactCardinalityAccumulator merged = new ExactCardinalityAccumulator();
    merged.distinctKeys.addAll(this.distinctKeys);
    merged.distinctKeys.addAll(other.distinctKeys);
    return merged;
  }

  @Override
  public byte[] serializeToBytes() {
    int totalSize = Integer.BYTES; // for set size
    for (String key : distinctKeys) {
      totalSize += Integer.BYTES + key.getBytes().length; // key length + key
    }

    ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(distinctKeys.size());

    for (String key : distinctKeys) {
      byte[] keyBytes = key.getBytes();
      buffer.putInt(keyBytes.length);
      buffer.put(keyBytes);
    }

    return buffer.array();
  }

  @Override
  public String serializeToString() {
    return "Distinct count: " + distinctKeys.size();
  }

  @Override
  public JsonNode serializeToJson() {
    ObjectMapper objectMapper = new ObjectMapper();

    // Baseline: return exact distinct count
    ObjectNode jsonNode = objectMapper.createObjectNode();
    jsonNode.put("distinct_count", distinctKeys.size());
    return jsonNode;
  }

  /**
   * Execute query operations on the cardinality data. Returns the exact distinct count.
   *
   * @param params JsonNode containing query parameters (optional)
   */
  @Override
  public JsonNode query(JsonNode params) {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode queryResult = objectMapper.createObjectNode();

    // Return exact distinct count
    queryResult.put("distinct_count", distinctKeys.size());

    return queryResult;
  }

  @Override
  public long get_memory() {
    long approxSize = 0;
    for (String key : distinctKeys) {
      approxSize += key.length() * Character.BYTES;
    }
    return approxSize;
  }
}
