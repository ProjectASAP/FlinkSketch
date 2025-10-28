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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.commons.codec.digest.XXHash32;

/** Accumulator for Count Sketch algorithm. Maintains sketch matrix and tracks L2-heavy hitters. */
public class CountSketchAccumulator implements Summary<CountSketchAccumulator> {

  public int[][] sketch; // Main sketch table for frequencies
  public int rowNum;
  public int colNum;
  public int topCount;
  private final String subType;
  private final boolean trackHeavyHitters;
  private transient XXHash32[][] hashFunctions;
  public PriorityQueue<HeavyHitter> heavyHitters; // Top-k L2 heavy hitters

  /**
   * Constructs a CountSketchAccumulator with specified parameters.
   *
   * @param aggregationSubType the aggregation subtype
   * @param parameters configuration parameters including "rows", "cols", "trackHH", and "k"
   */
  public CountSketchAccumulator(String aggregationSubType, Map<String, String> parameters) {
    // Initialize CountSketch with parameters
    this.rowNum = Integer.parseInt(parameters.get("rows"));
    this.colNum = Integer.parseInt(parameters.get("cols"));
    this.subType = aggregationSubType;
    init_hash();

    if (!parameters.containsKey("trackHH")) {
      throw new IllegalArgumentException("Missing required parameter 'trackHH'");
    }
    this.trackHeavyHitters = Boolean.parseBoolean(parameters.get("trackHH"));
    if (this.trackHeavyHitters) {
      if (!parameters.containsKey("k")) {
        throw new IllegalArgumentException("Missing required parameter 'k' when trackHH=true");
      }
      this.topCount = Integer.parseInt(parameters.get("k"));
      // Initialize empty priority queue for heavy hitters only if trackHeavyHitters flag is set
      this.heavyHitters = new PriorityQueue<>();
    } else {
      this.topCount = 0;
    }

    // Initialize the sketch matrix to 0
    this.sketch = new int[rowNum][colNum];
    for (int i = 0; i < rowNum; i++) {
      for (int j = 0; j < colNum; j++) {
        sketch[i][j] = 0;
      }
    }
  }

  @Override
  public byte[] serializeToBytes() {
    int bufferSize =
        Integer.BYTES + Integer.BYTES + Integer.BYTES + Integer.BYTES * rowNum * colNum;
    List<HeavyHitter> sortedHeavyHitters = getHeavyHitters();

    if (trackHeavyHitters && sortedHeavyHitters != null) {
      for (HeavyHitter hh : sortedHeavyHitters) {
        bufferSize +=
            Integer.BYTES
                + hh.key.getBytes(StandardCharsets.UTF_8).length
                + Integer.BYTES; // key_length + key + frequency
      }
    }

    ByteBuffer buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(rowNum);
    buffer.putInt(colNum);
    buffer.putInt(sortedHeavyHitters.size());

    for (int i = 0; i < rowNum; i++) {
      for (int j = 0; j < colNum; j++) {
        buffer.putInt(sketch[i][j]);
      }
    }

    if (trackHeavyHitters && sortedHeavyHitters != null) {
      for (HeavyHitter hh : sortedHeavyHitters) {
        byte[] keyBytes = hh.key.getBytes(StandardCharsets.UTF_8);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(hh.frequency);
      }
    }

    return buffer.array();
  }

  @Override
  public JsonNode serializeToJson() {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode jsonNode = objectMapper.createObjectNode();

    jsonNode.put("row_num", this.rowNum);
    jsonNode.put("col_num", this.colNum);

    if (trackHeavyHitters && heavyHitters != null) {
      jsonNode.put("heavy_hitters_size", heavyHitters.size());
    }

    ArrayNode sketchArray = objectMapper.createArrayNode();
    for (int i = 0; i < rowNum; i++) {
      ArrayNode rowArray = objectMapper.createArrayNode();
      for (int j = 0; j < colNum; j++) {
        rowArray.add(sketch[i][j]);
      }
      sketchArray.add(rowArray);
    }
    jsonNode.set("sketch", sketchArray);

    // Only include heavy hitters array if tracking is enabled
    if (trackHeavyHitters && heavyHitters != null) {
      ArrayNode heavyHittersArray = objectMapper.createArrayNode();
      List<HeavyHitter> sortedHeavyHitters = getHeavyHitters();

      for (HeavyHitter hh : sortedHeavyHitters) {
        ObjectNode hhNode = objectMapper.createObjectNode();
        hhNode.put("key", hh.key);
        hhNode.put("frequency", hh.frequency);
        heavyHittersArray.add(hhNode);
      }
      jsonNode.set("heavyHitters", heavyHittersArray);
    }

    return jsonNode;
  }

  /**
   * Initializes hash functions for each row. Creates two hash functions per row: one for position,
   * one for sign.
   */
  public void init_hash() {
    if (this.hashFunctions == null) {
      this.hashFunctions = new XXHash32[rowNum][2];
      for (int i = 0; i < rowNum; i++) {
        hashFunctions[i][0] = new XXHash32(i);
        hashFunctions[i][1] = new XXHash32(rowNum + i);
      }
    }
  }

  /**
   * Updates the top-k heavy hitters with a new key-frequency pair.
   *
   * @param key the item key
   * @param frequency the estimated frequency
   */
  public void updateHeavyHitters(String key, int frequency) {
    if (!trackHeavyHitters || heavyHitters == null || frequency <= 0) {
      return;
    }

    boolean exists = false;
    HeavyHitter existinghh = null;
    for (HeavyHitter hh : heavyHitters) {
      if (hh.key.equals(key)) {
        existinghh = hh;
        exists = true;
        break;
      }
    }

    if (exists) {
      // Update frequency if improved
      heavyHitters.remove(existinghh);
      existinghh.frequency = frequency;
      heavyHitters.add(existinghh);
    } else {
      // If the heap is not full, insert new heavy hitter
      if (heavyHitters.size() < topCount) {
        heavyHitters.add(new HeavyHitter(key, frequency));
      } else if (heavyHitters.peek() != null && frequency > heavyHitters.peek().frequency) {
        // If full, only insert if frequency is higher than the minimum and remove minimum
        heavyHitters.poll();
        heavyHitters.add(new HeavyHitter(key, frequency));
      }
    }
  }

  /**
   * Returns the list of current heavy hitters sorted by frequency.
   *
   * @return list of heavy hitters in decreasing frequency order
   */
  public List<HeavyHitter> getHeavyHitters() {
    if (!trackHeavyHitters || heavyHitters == null) {
      return new ArrayList<>();
    }
    List<HeavyHitter> result = new ArrayList<>(heavyHitters);
    result.sort(Comparator.reverseOrder());
    return result;
  }

  // Add a value to the sketch and update heavy hitters
  @Override
  public void add(String key, Integer value) {
    init_hash();

    int hashValue = key.hashCode();
    byte[] valueBytes = Longs.toByteArray((long) hashValue);

    for (int row = 0; row < rowNum; row++) {
      hashFunctions[row][0].reset();
      hashFunctions[row][1].reset();
      hashFunctions[row][0].update(valueBytes, 0, valueBytes.length);
      hashFunctions[row][1].update(valueBytes, 0, valueBytes.length);
      int hash = Integer.remainderUnsigned((int) hashFunctions[row][0].getValue(), colNum);

      // 1 --> 1, 0 --> -1
      int incOrDec = ((int) (hashFunctions[row][1].getValue() % 2) * 2) - 1;
      if (this.subType.equals("count")) {
        sketch[row][hash] += incOrDec;
      } else if (this.subType.equals("sum")) {
        sketch[row][hash] += incOrDec * value;
      }
    }

    // After inserting, maintain a heap of top-k heavy hitters
    if (trackHeavyHitters) {
      int frequency = queryFrequency(key);
      updateHeavyHitters(key, frequency);
    }
  }

  // Merge two accumulators (element-wise addition and merge heavy hitters)
  @Override
  public CountSketchAccumulator merge(CountSketchAccumulator b) {
    if (rowNum != b.rowNum || colNum != b.colNum) {
      throw new IllegalArgumentException("Cannot merge: dimension mismatch!");
    }

    for (int i = 0; i < rowNum; i++) {
      for (int j = 0; j < colNum; j++) {
        sketch[i][j] += b.sketch[i][j];
      }
    }

    // Merge heavy hitter lists (combine all unique keys)
    if (trackHeavyHitters && b.heavyHitters != null && heavyHitters != null) {
      List<String> allKeys = new ArrayList<>();

      for (HeavyHitter hh : heavyHitters) {
        if (!allKeys.contains(hh.key)) {
          allKeys.add(hh.key);
        }
      }

      for (HeavyHitter hh : b.heavyHitters) {
        if (!allKeys.contains(hh.key)) {
          allKeys.add(hh.key);
        }
      }

      heavyHitters.clear();

      for (String key : allKeys) {
        int frequency = queryFrequency(key);
        updateHeavyHitters(key, frequency);
      }
    }

    return this;
  }

  /**
   * Internal method to estimate the frequency of a key using the CountSketch. Implements the L2
   * heavy hitter "median-of-means" approach: Looks up the indexed and signed sum for each row and
   * returns the median.
   */
  public int queryFrequency(String key) {
    init_hash();
    int hashValue = key.hashCode();
    byte[] valueBytes = Longs.toByteArray((long) hashValue);

    int[] hashes = new int[rowNum];
    for (int row = 0; row < rowNum; row++) {
      hashFunctions[row][0].reset();
      hashFunctions[row][1].reset();
      hashFunctions[row][0].update(valueBytes, 0, valueBytes.length);
      hashFunctions[row][1].update(valueBytes, 0, valueBytes.length);
      int hash = Integer.remainderUnsigned((int) hashFunctions[row][0].getValue(), colNum);

      // 1 --> 1, 0 --> -1
      int incOrDec = ((int) (hashFunctions[row][1].getValue() % 2) * 2) - 1;
      hashes[row] = sketch[row][hash] * incOrDec;
    }
    Arrays.sort(hashes);

    return (rowNum % 2 == 1)
        ? hashes[rowNum / 2]
        : (hashes[rowNum / 2 - 1] + hashes[rowNum / 2]) / 2;
  }

  @Override
  public JsonNode query(JsonNode params) {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode queryResult = objectMapper.createObjectNode();

    if (params.has("statistic")) {
      String statType = params.get("statistic").asText();
      if (statType.equals("freq")) {
        String key = params.get("key").asText();
        int frequency = queryFrequency(key);
        queryResult.put(key, frequency);
        return queryResult;
      } else if (statType.equals("topk")) {
        int requestedK = params.has("topk") ? params.get("topk").asInt() : topCount;

        List<HeavyHitter> heavyHittersList = getHeavyHitters();
        int actualK = Math.min(requestedK, heavyHittersList.size());

        ObjectNode topkObject = objectMapper.createObjectNode();
        for (int i = 0; i < actualK; i++) {
          HeavyHitter hh = heavyHittersList.get(i);
          topkObject.put(hh.key, hh.frequency);
        }

        queryResult.set("topk", topkObject);
        return queryResult;
      }
    }

    // Error: invalid query parameters
    ObjectNode error = objectMapper.createObjectNode();
    error.put(
        "error",
        "Query must contain either 'key' for frequency query or 'topk' for heavy hitter query");
    return error;
  }

  @Override
  public long get_memory() {
    long approxSize = 0;

    approxSize += (long) rowNum * colNum * Integer.BYTES;

    if (trackHeavyHitters && heavyHitters != null) {
      for (HeavyHitter hh : heavyHitters) {
        approxSize += (long) hh.key.length() * Character.BYTES;
        approxSize += Integer.BYTES;
      }
    }

    return approxSize;
  }
}
