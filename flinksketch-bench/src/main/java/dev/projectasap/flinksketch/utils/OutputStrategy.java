/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.projectasap.flinksketch.datamodel.SerializableToSink;
import java.util.List;

/**
 * Centralized output strategy handler for determining what to output and executing queries. Handles
 * the logic for different pipeline modes and output modes.
 */
public class OutputStrategy {
  private final String pipeline;
  private final String outputMode;
  private final boolean verbose;

  public OutputStrategy(String pipeline, String outputMode, boolean verbose) {
    this.pipeline = pipeline;
    this.outputMode = outputMode;
    this.verbose = verbose;
  }

  public String getPipeline() {
    return pipeline;
  }

  public String getOutputMode() {
    return outputMode;
  }

  public boolean isVerbose() {
    return verbose;
  }

  /** Parse output mode flags (e.g., "query_memory" -> ["query", "memory"]). */
  public OutputModeFlags parseOutputMode() {
    String[] flags = this.outputMode.split("_");
    boolean includeSketch = false;
    boolean includeQuery = false;
    boolean includeMemory = false;

    for (String flag : flags) {
      switch (flag) {
        case "sketch":
          includeSketch = true;
          break;
        case "query":
          includeQuery = true;
          break;
        case "memory":
          includeMemory = true;
          break;
      }
    }

    return new OutputModeFlags(includeSketch, includeQuery, includeMemory);
  }

  /**
   * Determines if queries should be executed in the window function (before serialization). For
   * insertion_querying pipeline, queries are executed in window function and cached.
   *
   * @return true if queries should be executed in window function, false otherwise
   */
  public boolean shouldExecuteQueries() {
    // Execute queries in window function for insertion_querying pipeline
    return "insertion_querying".equals(pipeline);
  }

  /**
   * Execute queries for all keys and/or ranks in the configuration.
   *
   * @param precompute the sketch/summary to query
   * @param keys the list of keys to query (can be null/empty)
   * @param ranks the list of ranks to query (can be null/empty)
   * @param statistic the statistic type for Univmon queries (can be null)
   * @return ObjectNode containing all query results
   * @throws Exception if query execution fails
   */
  public static ObjectNode executeQueries(
      SerializableToSink precompute, List<String> keys, List<String> ranks, String statistic)
      throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    java.lang.reflect.Method queryMethod = precompute.getClass().getMethod("query", JsonNode.class);
    ObjectNode combinedResults = objectMapper.createObjectNode();

    // Case 1: No keys or ranks specified - query with statistic only
    if ((keys == null || keys.isEmpty()) && (ranks == null || ranks.isEmpty())) {
      ObjectNode params = objectMapper.createObjectNode();
      if (statistic != null && !statistic.isEmpty()) {
        params.put("statistic", statistic);
      }
      JsonNode result = (JsonNode) queryMethod.invoke(precompute, params);
      combinedResults.setAll((ObjectNode) result);
      return combinedResults;
    }

    // Case 2: Only ranks specified (quantile queries)
    if ((keys == null || keys.isEmpty()) && ranks != null && !ranks.isEmpty()) {
      for (String rank : ranks) {
        ObjectNode params = objectMapper.createObjectNode();
        params.put("rank", rank.toString());
        if (statistic != null && !statistic.isEmpty()) {
          params.put("statistic", statistic);
        }
        JsonNode result = (JsonNode) queryMethod.invoke(precompute, params);
        combinedResults.setAll((ObjectNode) result);
      }
      return combinedResults;
    }

    // Case 3: Only keys specified (frequency/item queries)
    if (keys != null && !keys.isEmpty() && (ranks == null || ranks.isEmpty())) {
      for (String key : keys) {
        ObjectNode params = objectMapper.createObjectNode();
        params.put("key", key.toString());
        if (statistic != null && !statistic.isEmpty()) {
          params.put("statistic", statistic);
        }
        JsonNode result = (JsonNode) queryMethod.invoke(precompute, params);
        combinedResults.setAll((ObjectNode) result);
      }
      return combinedResults;
    }

    // Case 4: BOTH keys and ranks specified (per-key quantile queries)
    // Build nested array structure: {"key1": [val1, val2, ...], "key2": [val1, val2, ...]}
    if (keys != null && !keys.isEmpty() && ranks != null && !ranks.isEmpty()) {
      for (String key : keys) {
        ArrayNode keyResults = objectMapper.createArrayNode();
        for (String rank : ranks) {
          ObjectNode params = objectMapper.createObjectNode();
          params.put("key", key.toString());
          params.put("rank", rank.toString());
          if (statistic != null && !statistic.isEmpty()) {
            params.put("statistic", statistic);
          }
          JsonNode result = (JsonNode) queryMethod.invoke(precompute, params);
          // Extract the value for this key from the result (format: {"key": value})
          JsonNode valueNode = result.get(key);
          if (valueNode != null) {
            keyResults.add(valueNode);
          }
        }
        combinedResults.set(key, keyResults);
      }
      return combinedResults;
    }

    return combinedResults;
  }

  /**
   * Build the JSON output based on the output strategy configuration.
   *
   * @param precompute the sketch/summary to serialize
   * @param cachedQueryResults query results from window function (can be null)
   * @param objectMapper Jackson ObjectMapper for JSON construction
   * @return ObjectNode with the appropriate output fields
   * @throws Exception if serialization or memory retrieval fails
   */
  public ObjectNode buildOutput(
      SerializableToSink precompute, ObjectNode cachedQueryResults, ObjectMapper objectMapper)
      throws Exception {
    ObjectNode outputNode = objectMapper.createObjectNode();
    OutputModeFlags flags = parseOutputMode();

    // Add sketch if requested
    if (flags.includeSketch && this.verbose) {
      outputNode.set("sketch", precompute.serializeToJson());
    }

    // Add query results if requested and verbose mode is enabled
    if (flags.includeQuery && this.verbose) {
      if (cachedQueryResults != null) {
        outputNode.set("query", cachedQueryResults);
      } else {
        outputNode.put(
            "query_error",
            "Query results not available - queries should be executed in window function");
      }
    }

    // Add memory usage if requested and verbose mode is enabled
    if (flags.includeMemory && this.verbose) {
      java.lang.reflect.Method getMemoryMethod = precompute.getClass().getMethod("get_memory");
      long memoryUsage = (Long) getMemoryMethod.invoke(precompute);
      outputNode.put("memory_bytes", memoryUsage);

      // Also add count when memory is requested
      try {
        java.lang.reflect.Method getCountMethod = precompute.getClass().getMethod("get_count");
        long count = (Long) getCountMethod.invoke(precompute);
        outputNode.put("count", count);
      } catch (NoSuchMethodException e) {
        // get_count method not available for this accumulator type
      }
    }

    return outputNode;
  }

  /** Flags indicating which output components to include. */
  public static class OutputModeFlags {
    public final boolean includeSketch;
    public final boolean includeQuery;
    public final boolean includeMemory;

    public OutputModeFlags(boolean includeSketch, boolean includeQuery, boolean includeMemory) {
      this.includeSketch = includeSketch;
      this.includeQuery = includeQuery;
      this.includeMemory = includeMemory;
    }
  }
}
