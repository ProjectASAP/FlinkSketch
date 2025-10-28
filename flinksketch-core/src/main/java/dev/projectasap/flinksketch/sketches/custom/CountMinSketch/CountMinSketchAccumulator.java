/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.sketches.custom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.projectasap.flinksketch.datamodel.Summary;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.codec.digest.XXHash32;

/**
 * Accumulator for Count-Min Sketch algorithm. Maintains a 2D array of counters and hash functions
 * for approximate frequency counting.
 */
public class CountMinSketchAccumulator implements Summary<CountMinSketchAccumulator> {
  private int rows;
  public int cols;
  private final String subType;
  private XXHash32[] hashFunctions;
  public int[][] sketch;

  /**
   * Constructs a CountMinSketchAccumulator with specified parameters.
   *
   * @param aggregationSubType the aggregation subtype (e.g., "count" or "sum")
   * @param parameters configuration parameters including "rows" and "cols"
   */
  public CountMinSketchAccumulator(String aggregationSubType, Map<String, String> parameters) {
    // TODO: Initalize sketch matrix based on subType
    if (!parameters.containsKey("rows") || !parameters.containsKey("cols")) {
      throw new IllegalArgumentException("Missing required parameters 'rows' and/or 'cols'");
    }
    this.rows = Integer.parseInt(parameters.get("rows"));
    this.cols = Integer.parseInt(parameters.get("cols"));
    this.subType = aggregationSubType;
    this.hashFunctions = initHashFunctions();
    this.sketch = initSketchMatrix(subType);
  }

  @Override
  public void add(String key, Integer value) {
    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
    for (int row = 0; row < rows; row++) {
      hashFunctions[row].reset();
      hashFunctions[row].update(keyBytes, 0, keyBytes.length);
      int hash = Integer.remainderUnsigned((int) hashFunctions[row].getValue(), cols);
      if (subType.equals("count")) {
        sketch[row][hash] += 1;
      } else if (subType.equals("sum")) {
        sketch[row][hash] += value;
      }
    }
  }

  @Override
  public CountMinSketchAccumulator merge(CountMinSketchAccumulator other) {
    if (rows != other.rows || cols != other.cols) {
      throw new IllegalArgumentException("Cannot merge: dimension mismatch!");
    }
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        sketch[i][j] += other.sketch[i][j];
      }
    }
    return this;
  }

  @Override
  public byte[] serializeToBytes() {
    ByteBuffer buffer =
        ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + Integer.BYTES * rows * cols)
            .order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(rows);
    buffer.putInt(cols);
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        buffer.putInt(sketch[i][j]);
      }
    }
    return buffer.array();
  }

  @Override
  public JsonNode serializeToJson() {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode jsonNode = objectMapper.createObjectNode();

    jsonNode.put("rows", rows);
    jsonNode.put("cols", cols);

    ArrayNode sketchArray = objectMapper.createArrayNode();
    for (int i = 0; i < rows; i++) {
      ArrayNode rowArray = objectMapper.createArrayNode();
      for (int j = 0; j < cols; j++) {
        rowArray.add(sketch[i][j]);
      }
      sketchArray.add(rowArray);
    }
    jsonNode.set("sketch", sketchArray);
    return jsonNode;
  }

  private XXHash32[] initHashFunctions() {
    XXHash32[] hashFunctions = new XXHash32[rows];
    for (int i = 0; i < rows; i++) {
      // Each hash function initialized with row number as seed
      hashFunctions[i] = new XXHash32(i);
    }
    return hashFunctions;
  }

  private int[][] initSketchMatrix(String subType) {
    int[][] sketch = new int[rows][cols];
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        sketch[i][j] = 0;
      }
    }
    return sketch;
  }

  /** Execute query on the CountMinSketch. Returns the estimated frequency for the given key */
  @Override
  public JsonNode query(JsonNode params) {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode queryResult = objectMapper.createObjectNode();

    if (params != null && params.has("key")) {
      String key = params.get("key").asText();
      byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

      int minFrequency = Integer.MAX_VALUE;
      for (int row = 0; row < rows; row++) {
        hashFunctions[row].reset();
        hashFunctions[row].update(keyBytes, 0, keyBytes.length);
        int hash = Integer.remainderUnsigned((int) hashFunctions[row].getValue(), cols);
        int curFrequency = sketch[row][hash];
        minFrequency = Math.min(minFrequency, curFrequency);
      }
      queryResult.put(key, minFrequency);
    }

    return queryResult;
  }

  @Override
  public long get_memory() {
    // Manual calculation: int[][] sketch array
    return (long) rows * cols * Integer.BYTES;
  }
}
