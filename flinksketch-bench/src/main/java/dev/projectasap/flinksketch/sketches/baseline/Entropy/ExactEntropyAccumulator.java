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

/**
 * Accumulator for computing exact Shannon entropy of a tumbling window. Maintains counts for each
 * key and computes entropy = -Σ p(x) log2(p(x)).
 */
public class ExactEntropyAccumulator implements Summary<ExactEntropyAccumulator> {
  private final Map<String, Integer> keyCounts;
  private long totalCount;

  public ExactEntropyAccumulator() {
    this.keyCounts = new HashMap<>();
    this.totalCount = 0;
  }

  @Override
  public void add(String key, Integer value) {
    keyCounts.put(key, keyCounts.getOrDefault(key, 0) + 1);
    totalCount++;
  }

  @Override
  public ExactEntropyAccumulator merge(ExactEntropyAccumulator other) {
    ExactEntropyAccumulator merged = new ExactEntropyAccumulator();
    merged.keyCounts.putAll(this.keyCounts);

    for (Map.Entry<String, Integer> entry : other.keyCounts.entrySet()) {
      merged.keyCounts.put(
          entry.getKey(), merged.keyCounts.getOrDefault(entry.getKey(), 0) + entry.getValue());
    }

    merged.totalCount = this.totalCount + other.totalCount;
    return merged;
  }

  /** Compute Shannon entropy (base 2). */
  private double computeEntropy() {
    if (totalCount == 0) return 0.0;

    double entropy = 0.0;
    for (int count : keyCounts.values()) {
      double p = (double) count / totalCount;
      entropy -= p * (Math.log(p) / Math.log(2)); // log base 2
    }
    return entropy;
  }

  @Override
  public byte[] serializeToBytes() {
    double entropy = computeEntropy();
    ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putDouble(entropy);
    return buffer.array();
  }

  @Override
  public String serializeToString() {
    return String.format("%.6f", computeEntropy());
  }

  @Override
  public JsonNode serializeToJson() {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode node = mapper.createObjectNode();
    node.put("total_entropy", computeEntropy());
    return node;
  }

  /** Return only the entropy value for the tumbling window. */
  @Override
  public JsonNode query(JsonNode params) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode result = mapper.createObjectNode();
    result.put("total_entropy", computeEntropy());
    return result;
  }

  @Override
  public long get_memory() {
    long approxSize = 0;
    for (String key : keyCounts.keySet()) {
      approxSize += key.length() * Character.BYTES + Integer.BYTES;
    }
    return approxSize;
  }
}
