/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.sketches.custom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.primitives.Longs;
import dev.projectasap.flinksketch.datamodel.Summary;
import dev.projectasap.flinksketch.sketches.datasketches.KllFloatsSketchAccumulator;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Map;
import org.apache.commons.codec.digest.XXHash32;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;

/**
 * Accumulator for HydraKLL algorithm. Maintains a CountSketch matrix, where each cell is a KLL
 * sketch.
 */
public class HydraKLLAccumulator implements Summary<HydraKLLAccumulator> {
  public KllFloatsSketchAccumulator[][] sketch;
  public int rowNum;
  public int colNum;
  public int kllSize;
  private transient XXHash32[] hashFunctions;
  private long count;

  /**
   * Constructs a HydraKLLAccumulator with specified parameters.
   *
   * @param aggregationSubType the aggregation subtype
   * @param parameters configuration parameters including "rows", "cols", and "k"
   */
  public HydraKLLAccumulator(Map<String, String> parameters) {
    // Initialize CountSketch with parameters
    this.rowNum = Integer.parseInt(parameters.get("rows"));
    this.colNum = Integer.parseInt(parameters.get("cols"));
    this.kllSize = Integer.parseInt(parameters.get("k"));
    init_hash();

    // Initialize the sketch matrix
    this.sketch = new KllFloatsSketchAccumulator[rowNum][colNum];
    for (int i = 0; i < rowNum; i++) {
      for (int j = 0; j < colNum; j++) {
        sketch[i][j] = new KllFloatsSketchAccumulator(kllSize);
      }
    }
  }

  @Override
  public byte[] serializeToBytes() {
    // Step 1: Calculate total buffer size
    int bufferSize = Integer.BYTES + Integer.BYTES + Integer.BYTES; // rowNum, colNum, kllSize

    // Store serialized sketches to avoid serializing twice
    byte[][][] serializedSketches = new byte[rowNum][colNum][];

    for (int i = 0; i < rowNum; i++) {
      for (int j = 0; j < colNum; j++) {
        serializedSketches[i][j] = sketch[i][j].serializeToBytes();
        bufferSize +=
            Integer.BYTES + serializedSketches[i][j].length; // length header + actual bytes
      }
    }

    // Step 2: Allocate buffer
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);

    // Step 3: Write metadata
    buffer.putInt(rowNum);
    buffer.putInt(colNum);
    buffer.putInt(kllSize);

    // Step 4: Write each KLL sketch with its length header
    for (int i = 0; i < rowNum; i++) {
      for (int j = 0; j < colNum; j++) {
        buffer.putInt(serializedSketches[i][j].length); // Length header
        buffer.put(serializedSketches[i][j]); // Actual sketch bytes
      }
    }

    // Step 5: Return serialized bytes
    return buffer.array();
  }

  @Override
  public JsonNode serializeToJson() {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode jsonNode = objectMapper.createObjectNode();

    jsonNode.put("row_num", this.rowNum);
    jsonNode.put("col_num", this.colNum);
    jsonNode.put("kll_size", this.kllSize);

    ArrayNode sketchArray = objectMapper.createArrayNode();
    for (int i = 0; i < rowNum; i++) {
      ArrayNode rowArray = objectMapper.createArrayNode();
      for (int j = 0; j < colNum; j++) {
        rowArray.add(sketch[i][j].serializeToJson());
      }
      sketchArray.add(rowArray);
    }
    jsonNode.set("sketch", sketchArray);

    return jsonNode;
  }

  /**
   * Initializes hash functions for each row. We only need a single hash function for Hydra per row,
   * as opposed to CountSketch which needs two.
   */
  public void init_hash() {
    if (this.hashFunctions == null) {
      this.hashFunctions = new XXHash32[rowNum];
      for (int i = 0; i < rowNum; i++) {
        hashFunctions[i] = new XXHash32(i);
      }
    }
  }

  // Add a value to the sketch and update heavy hitters
  @Override
  public void add(String key, Integer value) {
    init_hash();

    int hashValue = key.hashCode();
    byte[] valueBytes = Longs.toByteArray((long) hashValue);

    for (int row = 0; row < rowNum; row++) {
      hashFunctions[row].reset();
      hashFunctions[row].update(valueBytes, 0, valueBytes.length);
      int hash = Integer.remainderUnsigned((int) hashFunctions[row].getValue(), colNum);
      sketch[row][hash].add(key, value);
    }

    count += 1L;
  }

  // Merge two accumulators
  @Override
  public HydraKLLAccumulator merge(HydraKLLAccumulator b) {
    if (rowNum != b.rowNum || colNum != b.colNum) {
      throw new IllegalArgumentException("Cannot merge: dimension mismatch!");
    }

    for (int i = 0; i < rowNum; i++) {
      for (int j = 0; j < colNum; j++) {
        sketch[i][j] = sketch[i][j].merge(b.sketch[i][j]);
      }
    }

    return this;
  }

  /** Internal method to estimate the quantile of a key using a Hydra sketch on top of KLLs. */
  public float queryQuantile(String key, float quantile) {
    int hashValue = key.hashCode();
    byte[] valueBytes = Longs.toByteArray((long) hashValue);
    float[] quantiles = new float[rowNum];

    for (int row = 0; row < rowNum; row++) {
      hashFunctions[row].reset();
      hashFunctions[row].update(valueBytes, 0, valueBytes.length);
      int hash = Integer.remainderUnsigned((int) hashFunctions[row].getValue(), colNum);

      quantiles[row] = sketch[row][hash].queryQuantile(quantile, QuantileSearchCriteria.INCLUSIVE);
    }
    Arrays.sort(quantiles);

    return (rowNum % 2 == 1)
        ? quantiles[rowNum / 2]
        : (quantiles[rowNum / 2 - 1] + quantiles[rowNum / 2]) / 2;
  }

  @Override
  public JsonNode query(JsonNode params) {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode queryResult = objectMapper.createObjectNode();

    // Handle single key quantile query
    if (params != null && params.has("key") && params.has("rank")) {
      String key = params.get("key").asText();
      float rank = (float) params.get("rank").asDouble();
      queryResult.put(key, queryQuantile(key, rank));
      return queryResult;
    }

    // Error: invalid query parameters
    ObjectNode error = objectMapper.createObjectNode();
    error.put("error", "Query must contain 'key' and 'rank' parameters");
    return error;
  }

  @Override
  public long get_memory() {
    long totalMemory = 0;

    // Sum up memory from all KLL sketches in the matrix
    for (int i = 0; i < rowNum; i++) {
      for (int j = 0; j < colNum; j++) {
        totalMemory += sketch[i][j].get_memory();
      }
    }

    return totalMemory;
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
