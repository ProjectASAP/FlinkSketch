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
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.digest.XXHash32;

/**
 * Accumulator for UnivMon - Holds all parallel CountSketches for different recursion levels
 *
 * <p>Each UnivmonAccumulator instance represents the full pipe of recursive/parallel sketches, see
 * Figure 2 and Algorithms 1 & 2 in the paper.
 */
public class UnivmonAccumulator implements Summary<UnivmonAccumulator> {
  // Array of (log(n)) CountSketches, one per level (§4.2)
  public CountSketchAccumulator[] univSketch;
  public int rowNum;
  public int colNum;
  public int levelNum;
  // public int numHeavyHitters; // Top-k L2 heavy hitters
  private transient XXHash32 levelHash;
  private final String subType;
  public long packetCount; // Total packets seen (needed for entropy estimation, Algorithm 2, §4.3)

  /**
   * Constructor initializes all CountSketch accumulators for each recursive level See §4.2,
   * Algorithm 1: maintains log(n) L2-HH sketches.
   */
  public UnivmonAccumulator(String aggregationSubType, Map<String, String> parameters) {
    this.levelNum = Integer.parseInt(parameters.get("levels"));
    this.rowNum = Integer.parseInt(parameters.get("rows"));
    this.colNum = Integer.parseInt(parameters.get("cols"));
    // this.numHeavyHitters = Integer.parseInt(parameters.get("k"));
    this.subType = aggregationSubType;
    this.packetCount = 0;

    level_init_hash(); // Initialize hash for level selection (§4.2)

    // Initialize the CountSketch arrays to 0
    this.univSketch = new CountSketchAccumulator[levelNum];
    for (int i = 0; i < levelNum; i++) {
      Map<String, String> csParameters =
          Map.of(
              "rows", parameters.get("rows"),
              "cols", parameters.get("cols"),
              "trackHH", String.valueOf(true),
              "k", parameters.get("k"));
      this.univSketch[i] = new CountSketchAccumulator(aggregationSubType, csParameters);
    }
  }

  @Override
  public byte[] serializeToBytes() {
    // Calculate actual buffer size
    int bufferSize = Integer.BYTES + Integer.BYTES + Integer.BYTES + Long.BYTES;
    for (int i = 0; i < levelNum; i++) {
      bufferSize += Integer.BYTES + univSketch[i].serializeToBytes().length;
    }

    ByteBuffer buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(levelNum);
    buffer.putInt(rowNum);
    buffer.putInt(colNum);
    buffer.putLong(packetCount);
    for (int i = 0; i < levelNum; i++) {
      byte[] sketchBytes = univSketch[i].serializeToBytes();
      buffer.putInt(sketchBytes.length);
      buffer.put(sketchBytes);
    }
    return buffer.array();
  }

  @Override
  public JsonNode serializeToJson() {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode jsonNode = objectMapper.createObjectNode();

    jsonNode.put("levelNum", this.levelNum);
    jsonNode.put("rowNum", this.rowNum);
    jsonNode.put("colNum", this.colNum);
    jsonNode.put("packetCount", this.packetCount);

    ArrayNode levelsArray = objectMapper.createArrayNode();
    for (int i = 0; i < levelNum; i++) {
      levelsArray.add(this.univSketch[i].serializeToJson()); // Serialize each CountSketch
    }
    jsonNode.set("levels", levelsArray);

    return jsonNode;
  }

  /**
   * Initialize a hash instance for recursive sampling of levels. See §4.2: hash functions per
   * level.
   */
  public void level_init_hash() {
    levelHash = new XXHash32(rowNum * colNum);
  }

  /**
   * Given the "sampling hash", identify the last level this flow/item will be included in (§4.2 -
   * recursive sampling). Algorithm matches Alg. 1 & the "parallel substreams" intuition.
   */
  public int get_last_level(int samplingHash, int numLevels) {
    int lastLevel = 0;
    for (int i = numLevels - 1; i >= 0; i--) {
      // >>> used for unsigned arithmetic right shift
      if (((samplingHash >>> i) & 1) == 0) {
        return lastLevel;
      }
      lastLevel++;
    }
    return lastLevel;
  }

  /**
   * Compute the sampling hash for this element (§4.2, Algorithm 1 using hash for sampling at each
   * level).
   */
  public int sampling_hash_computation(int hashValue) {
    level_init_hash();
    byte[] valueBytes = Longs.toByteArray((long) hashValue);
    levelHash.reset();
    levelHash.update(valueBytes, 0, valueBytes.length);

    return (int) (levelHash.getValue());
  }

  /** Determine the final level this item will be included in, based on its hash value. */
  public int final_level_computed(String key) {
    int hashValue = key.hashCode();
    int samplingHash = sampling_hash_computation(hashValue);
    int lastLevel = get_last_level(samplingHash, levelNum - 1);
    return lastLevel;
  }

  /**
   * Main aggregation update function. Each observed item is added to all recursive sketches from [0
   * .. last_level], per Alg. 1. This is the streaming (data plane) part of UnivMon (see Algorithm
   * 1).
   */
  @Override
  public void add(String key, Integer value) {
    packetCount++; // Incrementing total packets in accumulator

    int lastLevel = final_level_computed(key);

    for (int i = 0; i <= lastLevel; i++) {
      univSketch[i].add(key, value);
    }
  }

  /**
   * Parallel aggregation merge: combines two accumulators by merging sketches at each level. This
   * follows the property that UnivMon sketches are linear/additive (see §5).
   */
  @Override
  public UnivmonAccumulator merge(UnivmonAccumulator b) {
    if (rowNum != b.rowNum || colNum != b.colNum || levelNum != b.levelNum) {
      throw new IllegalArgumentException("Cannot merge: dimension mismatch!");
    }

    packetCount += b.packetCount;

    for (int i = 0; i < levelNum; i++) {
      univSketch[i].merge(b.univSketch[i]);
    }

    return this;
  }

  /**
   * Returns the estimated frequency of a flow by querying the CountSketch at the deepest level
   * where this flow is included (i.e., the last updated CountSketch array).
   */
  public int queryFrequency(String key) {
    if (levelNum == 0) {
      throw new IllegalStateException("Univmon has no levels initialized.");
    }

    int lastLevel = final_level_computed(key);

    CountSketchAccumulator lastLevelSketch = univSketch[lastLevel];
    return lastLevelSketch.queryFrequency(key);
  }

  /** Utility: x*log(x), used for entropy calculation, as in §4.3 "Entropy Estimation". */
  private double gxlogx(double x) {
    if (x <= 0) {
      return 0.0;
    }
    return x * (Math.log(x) / Math.log(2));
  }

  /**
   * Core estimation method: computes cardinality, entropy, etc. by recursively combining sketches.
   * Implements Algorithm 2 (offline/control plane), see §4.2, 4.3.
   */
  public double inference(String desiredStat) {
    if (levelNum == 0) {
      throw new IllegalStateException("Univmon has no levels initialized.");
    }

    int recLevel = levelNum;

    double ybottom = 0.0;
    // Algorithm 2: At the deepest level, compute g() over each HH (see §4.3, entropy/cardinality
    // examples)
    List<HeavyHitter> lastLevelSketch = univSketch[recLevel - 1].getHeavyHitters();

    for (HeavyHitter hh : lastLevelSketch) {
      double w = hh.frequency;
      if (w > 0) {
        if (desiredStat.equals("entropy")) {
          ybottom += gxlogx(w);
        } else if (desiredStat.equals("cardinality")) {
          ybottom += 1.0;
        } else {
          throw new IllegalArgumentException("Invalid desiredStat: " + desiredStat);
        }
      }
    }

    double y1 = 0.0;
    double y2 = ybottom;
    // Recursively process up the sketch levels, see Algorithm 2
    for (int level = recLevel - 2; level >= 0; level--) {
      double indSum = 0.0;
      List<HeavyHitter> currentLevelSketch = univSketch[level].getHeavyHitters();

      for (HeavyHitter hh : currentLevelSketch) {
        double wtmp = hh.frequency;
        if (wtmp > 0) {
          int hashValue = hh.key.hashCode();

          int samplingHash = sampling_hash_computation(hashValue);
          // To understand the hashEval logic, please check out this link for an explanation
          // w/example: https://tinyurl.com/InferenceCalculations
          double hashEval = 1 - 2 * ((samplingHash >>> (recLevel - level - 2)) & 1);

          if (desiredStat.equals("entropy")) {
            indSum += hashEval * gxlogx(wtmp);
          } else if (desiredStat.equals("cardinality")) {
            indSum += hashEval;
          } else {
            throw new IllegalArgumentException("Invalid desiredStat: " + desiredStat);
          }
        }
      }

      y1 = 2.0 * y2 + indSum;
      y2 = y1;
    }

    // Final estimate for entropy and cardinality, as in §4.3
    if (desiredStat.equals("entropy")) {
      return Math.log(packetCount) / Math.log(2) - (y1 / packetCount);
    } else if (desiredStat.equals("cardinality")) {
      return y1;
    } else {
      throw new IllegalArgumentException("Invalid desiredStat: " + desiredStat);
    }
  }

  /** --- API entry points to query entropy or cardinality, matching Section 4.3 applications --- */
  public double query_entropy() {
    return inference("entropy");
  }

  public double query_cardinality() {
    return inference("cardinality");
  }

  private JsonNode queryForKey(JsonNode params) {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode queryResult = objectMapper.createObjectNode();
    String key = params.get("key").asText();

    // Use the internal String version to get the frequency
    int frequency = queryFrequency(key);
    queryResult.put(key, frequency);
    return queryResult;
  }

  @Override
  public JsonNode query(JsonNode params) {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode queryResult = objectMapper.createObjectNode();

    if (params.has("statistic")) {
      // Handle entropy/cardinality/topk queries
      String statType = params.get("statistic").asText();

      if (statType.equals("freq")) {
        return queryForKey(params);
      } else if (statType.equals("entropy")) {
        queryResult.put("total_entropy", query_entropy());
        return queryResult;
      } else if (statType.equals("cardinality")) {
        queryResult.put("distinct_count", query_cardinality());
        return queryResult;
      } else if (statType.equals("topk")) {
        if (levelNum == 0 || univSketch == null || univSketch[0] == null) {
          ObjectNode error = objectMapper.createObjectNode();
          error.put("error", "UnivMon sketch not properly initialized");
          return error;
        }

        JsonNode countSketchResult = univSketch[0].query(params);
        queryResult.setAll((ObjectNode) countSketchResult);
        return queryResult;
      } else throw new IllegalArgumentException("Invalid stat type: " + statType);
    } else {
      throw new IllegalArgumentException("Query must contain 'statistic' parameter");
    }
  }

  @Override
  public long get_memory() {
    long approxSize = 0;
    // Sum up memory from all CountSketch levels
    for (int i = 0; i < levelNum; i++) {
      approxSize += univSketch[i].get_memory();
    }
    // Add packet count
    approxSize += Long.BYTES;
    return approxSize;
  }
}
