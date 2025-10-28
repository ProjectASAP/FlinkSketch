/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.datamodel;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.projectasap.flinksketch.utils.AggregationConfig;
import dev.projectasap.flinksketch.utils.OutputStrategy;
import java.nio.ByteBuffer;

/**
 * Represents the output of a precomputation window. Contains the aggregated sketch/summary with
 * metadata about the time window.
 */
public class PrecomputedOutput {
  @JsonProperty("start_timestamp")
  public Long startTimestamp;

  @JsonProperty("end_timestamp")
  public Long endTimestamp;

  public SerializableToSink precompute;
  public AggregationConfig config;
  public String key;
  private OutputStrategy outputStrategy; // Encapsulates output logic (pipeline, mode, verbose)
  private ObjectNode cachedQueryResults; // Cache query results from window function execution

  /**
   * Constructs a PrecomputedOutput.
   *
   * @param startTimestamp the window start timestamp
   * @param endTimestamp the window end timestamp
   * @param precompute the aggregated sketch or summary
   * @param config the aggregation configuration
   * @param key the grouping key (null for global aggregation)
   * @param pipeline the pipeline mode (e.g., "insertion", "insertion_querying")
   * @param outputMode the output mode (e.g., "sketch", "query", "memory")
   * @param verbose whether to enable verbose output
   */
  public PrecomputedOutput(
      Long startTimestamp,
      Long endTimestamp,
      SerializableToSink precompute,
      AggregationConfig config,
      String key,
      String pipeline,
      String outputMode,
      boolean verbose) {
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
    this.precompute = precompute;
    this.config = config;
    this.key = key;
    this.outputStrategy = new OutputStrategy(pipeline, outputMode, verbose);
    this.cachedQueryResults = null;
  }

  /**
   * Sets cached query results from eager execution in window function.
   *
   * @param queryResults the query results to cache
   */
  public void setCachedQueryResults(ObjectNode queryResults) {
    this.cachedQueryResults = queryResults;
  }

  /**
   * Returns a debug string showing the freshness of this precomputed output.
   *
   * @return debug string with end timestamp, current time, and freshness
   */
  public String getFreshnessDebugString() {
    Long currentTime = System.currentTimeMillis();
    return "end_timestamp: "
        + this.endTimestamp
        + ", current_time: "
        + currentTime
        + ", freshness: "
        + (currentTime - this.endTimestamp);
  }

  /**
   * Serializes the precomputed output to a byte array.
   *
   * @return serialized byte array containing config, timestamps, key, and precompute data
   */
  public byte[] serializeToBytes() {
    byte[] precomputeBytes = this.precompute.serializeToBytes();
    byte[] configBytes = this.config.serializeToBytes();
    byte[] keyBytes;
    if (this.key == null) {
      keyBytes = new byte[0];
    } else {
      keyBytes = this.key.getBytes();
    }

    ByteBuffer buffer =
        ByteBuffer.allocate(
            Byte.BYTES
                + configBytes.length
                + Long.BYTES
                + Long.BYTES
                + Byte.BYTES
                + keyBytes.length
                + Byte.BYTES
                + precomputeBytes.length);
    buffer.put((byte) configBytes.length);
    buffer.put(configBytes);
    buffer.putLong(this.startTimestamp);
    buffer.putLong(this.endTimestamp);
    buffer.put((byte) keyBytes.length);
    buffer.put(keyBytes);
    buffer.put((byte) precomputeBytes.length);
    buffer.put(precomputeBytes);
    return buffer.array();
  }

  /**
   * Serializes the precomputed output to JSON.
   *
   * @return JsonNode containing the serialized output based on output mode flags
   * @throws JsonProcessingException if JSON serialization fails
   */
  public JsonNode serializeToJson() throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode jsonNode = objectMapper.createObjectNode();

    // Add metadata
    jsonNode.set("config", this.config.serializeToJson());
    jsonNode.put("start_timestamp", this.startTimestamp);
    jsonNode.put("end_timestamp", this.endTimestamp);
    jsonNode.put("key", this.key);
    jsonNode.put("pipeline", this.outputStrategy.getPipeline());
    jsonNode.put("output_mode", this.outputStrategy.getOutputMode());

    // Delegate all output logic to OutputStrategy
    try {
      ObjectNode outputContent =
          this.outputStrategy.buildOutput(this.precompute, this.cachedQueryResults, objectMapper);
      jsonNode.setAll(outputContent);
    } catch (Exception e) {
      throw new RuntimeException("Failed to build output: " + e.getMessage(), e);
    }

    return jsonNode;
  }

  /** Gets the output strategy for this output. */
  public OutputStrategy getOutputStrategy() {
    return this.outputStrategy;
  }
}
