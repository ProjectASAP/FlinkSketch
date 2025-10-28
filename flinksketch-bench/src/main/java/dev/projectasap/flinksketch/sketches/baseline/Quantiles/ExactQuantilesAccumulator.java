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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Accumulator for exact quantile computation. Stores all values to compute exact quantiles at any
 * rank.
 */
public class ExactQuantilesAccumulator implements Summary<ExactQuantilesAccumulator> {
  public List<Float> values;
  private long count;

  public ExactQuantilesAccumulator() {
    this.values = new ArrayList<>();
  }

  @Override
  public void add(String key, Integer value) {
    values.add(value.floatValue());
    count += 1;
  }

  @Override
  public ExactQuantilesAccumulator merge(ExactQuantilesAccumulator other) {
    ExactQuantilesAccumulator merged = new ExactQuantilesAccumulator();
    merged.values.addAll(this.values);
    merged.values.addAll(other.values);
    merged.count = this.count + other.count;
    return merged;
  }

  /**
   * Computes the quantile value at the specified rank.
   *
   * @param rank the rank (0.0 to 1.0) at which to compute the quantile
   * @return the quantile value at the specified rank
   */
  public float getQuantile(double rank) {
    if (values.isEmpty()) {
      return Float.NaN;
    }

    List<Float> sortedValues = new ArrayList<>(values);
    Collections.sort(sortedValues);

    if (rank <= 0.0) {
      return sortedValues.get(0);
    }
    if (rank >= 1.0) {
      return sortedValues.get(sortedValues.size() - 1);
    }

    double index = rank * (sortedValues.size() - 1);
    int lowerIndex = (int) Math.floor(index);
    int upperIndex = (int) Math.ceil(index);

    if (lowerIndex == upperIndex) {
      return sortedValues.get(lowerIndex);
    }

    double weight = index - lowerIndex;
    return (float)
        (sortedValues.get(lowerIndex) * (1 - weight) + sortedValues.get(upperIndex) * weight);
  }

  /**
   * Computes quantile values at multiple ranks.
   *
   * @param ranks array of ranks (0.0 to 1.0) at which to compute quantiles
   * @return array of quantile values corresponding to the input ranks
   */
  public float[] getQuantiles(double[] ranks) {
    float[] results = new float[ranks.length];
    for (int i = 0; i < ranks.length; i++) {
      results[i] = getQuantile(ranks[i]);
    }
    return results;
  }

  @Override
  public byte[] serializeToBytes() {
    int totalSize = Integer.BYTES + values.size() * Float.BYTES; // size + float values

    ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(values.size());

    for (Float value : values) {
      buffer.putFloat(value);
    }

    return buffer.array();
  }

  @Override
  public JsonNode serializeToJson() {
    ObjectMapper objectMapper = new ObjectMapper();

    // Baseline: return only the raw list of values
    ArrayNode valuesArray = objectMapper.createArrayNode();
    for (Float value : values) {
      valuesArray.add(value);
    }

    return valuesArray;
  }

  /**
   * Execute query operations on the quantiles data. Returns quantile value for the specified rank.
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
        float quantile = getQuantile(rank);
        queryResult.put("rank_" + rank, quantile);
      }
    }

    return queryResult;
  }

  @Override
  public long get_memory() {
    // Manual calculation: ArrayList<Float>
    // return count;
    return values.size() * Float.BYTES;
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
