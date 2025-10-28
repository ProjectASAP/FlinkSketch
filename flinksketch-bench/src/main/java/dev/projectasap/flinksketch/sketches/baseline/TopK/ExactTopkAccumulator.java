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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Accumulator for exact top-K computation. Maintains frequency counts for all items and provides
 * top-K results.
 */
public class ExactTopkAccumulator implements Summary<ExactTopkAccumulator> {
  public Map<String, Integer> frequencyCounts;
  private final int topCount;

  public ExactTopkAccumulator(int topCount) {
    this.frequencyCounts = new HashMap<>();
    this.topCount = topCount;
  }

  @Override
  public void add(String key, Integer value) {
    frequencyCounts.put(key, frequencyCounts.getOrDefault(key, 0) + value);
  }

  @Override
  public ExactTopkAccumulator merge(ExactTopkAccumulator other) {
    ExactTopkAccumulator merged = new ExactTopkAccumulator(this.topCount);
    merged.frequencyCounts.putAll(this.frequencyCounts);

    for (Map.Entry<String, Integer> entry : other.frequencyCounts.entrySet()) {
      merged.frequencyCounts.put(
          entry.getKey(),
          merged.frequencyCounts.getOrDefault(entry.getKey(), 0) + entry.getValue());
    }

    return merged;
  }

  /** Get the top-K items by frequency. */
  public List<Map.Entry<String, Integer>> getTopK() {
    return frequencyCounts.entrySet().stream()
        .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
        .limit(topCount)
        .collect(Collectors.toList());
  }

  @Override
  public byte[] serializeToBytes() {
    List<Map.Entry<String, Integer>> topK = getTopK();
    int totalSize = Integer.BYTES; // for list size
    for (Map.Entry<String, Integer> entry : topK) {
      totalSize +=
          Integer.BYTES
              + entry.getKey().getBytes().length
              + Integer.BYTES; // key length + key + value
    }

    ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(topK.size());

    for (Map.Entry<String, Integer> entry : topK) {
      byte[] keyBytes = entry.getKey().getBytes();
      buffer.putInt(keyBytes.length);
      buffer.put(keyBytes);
      buffer.putInt(entry.getValue());
    }

    return buffer.array();
  }

  @Override
  public String serializeToString() {
    List<Map.Entry<String, Integer>> topK = getTopK();
    StringBuilder sb = new StringBuilder();
    sb.append("Top-").append(topCount).append(" items:\n");
    for (int i = 0; i < topK.size(); i++) {
      Map.Entry<String, Integer> entry = topK.get(i);
      sb.append((i + 1))
          .append(". ")
          .append(entry.getKey())
          .append(": ")
          .append(entry.getValue())
          .append("\n");
    }
    return sb.toString();
  }

  @Override
  public JsonNode serializeToJson() {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode topkObject = objectMapper.createObjectNode();

    List<Map.Entry<String, Integer>> topK = getTopK();
    for (Map.Entry<String, Integer> entry : topK) {
      topkObject.put(entry.getKey(), entry.getValue());
    }

    ObjectNode jsonNode = objectMapper.createObjectNode();
    jsonNode.set("topk", topkObject);
    return jsonNode;
  }

  /**
   * Execute query operations on the top-K data. Returns the top-K items as a map of key to
   * frequency.
   *
   * @param params JsonNode containing query parameters (optional)
   */
  @Override
  public JsonNode query(JsonNode params) {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode topkObject = objectMapper.createObjectNode();

    List<Map.Entry<String, Integer>> topK = getTopK();
    for (Map.Entry<String, Integer> entry : topK) {
      topkObject.put(entry.getKey(), entry.getValue());
    }

    ObjectNode queryResult = objectMapper.createObjectNode();
    queryResult.set("topk", topkObject);
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
}
