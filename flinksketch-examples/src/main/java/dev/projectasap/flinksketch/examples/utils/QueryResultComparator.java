/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.examples.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.projectasap.flinksketch.datamodel.PrecomputedOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class QueryResultComparator {

  // Enum to specify type of statistic comparison
  public enum StatisticType {
    KEY_AGNOSTIC,
    PER_KEY
  }

  private PrecomputedOutput baselineOutput;
  private PrecomputedOutput sketchOutput;
  private StatisticType statisticType;
  private ObjectMapper objectMapper;
  private List<Double> quantileRanks;

  public QueryResultComparator(
      PrecomputedOutput sketch, PrecomputedOutput baseline, StatisticType statisticType)
      throws Exception {
    this.baselineOutput = baseline;
    this.sketchOutput = sketch;
    this.statisticType = statisticType;
    this.objectMapper = new ObjectMapper();
    this.quantileRanks = null;
  }

  public QueryResultComparator(
      PrecomputedOutput sketch,
      PrecomputedOutput baseline,
      StatisticType statisticType,
      List<Double> quantileRanks)
      throws Exception {
    this.baselineOutput = baseline;
    this.sketchOutput = sketch;
    this.statisticType = statisticType;
    this.objectMapper = new ObjectMapper();
    this.quantileRanks = quantileRanks;
  }

  /**
   * Run comparison by querying both baseline and sketch.
   *
   * @param keys List of keys to query. Only used if statisticType is PER_KEY.
   * @return List of comparison results
   */
  public List<QueryComparisonResult> runComparison(Set<String> keys) {
    List<QueryComparisonResult> results = new ArrayList<>();

    try {
      java.lang.reflect.Method baselineQueryMethod =
          baselineOutput.precompute.getClass().getMethod("query", JsonNode.class);
      java.lang.reflect.Method sketchQueryMethod =
          sketchOutput.precompute.getClass().getMethod("query", JsonNode.class);

      // Per-sketch statistic (cardinality, quantiles, etc.)
      if (statisticType == StatisticType.KEY_AGNOSTIC) {
        // If quantile ranks are specified, query each rank
        if (quantileRanks != null && !quantileRanks.isEmpty()) {
          for (Double rank : quantileRanks) {
            ObjectNode queryParams = objectMapper.createObjectNode();
            queryParams.put("rank", rank);

            JsonNode baselineResult =
                (JsonNode) baselineQueryMethod.invoke(baselineOutput.precompute, queryParams);
            JsonNode sketchResult =
                (JsonNode) sketchQueryMethod.invoke(sketchOutput.precompute, queryParams);

            results.add(
                new QueryComparisonResult(
                    "p" + (int) (rank * 100),
                    baselineResult,
                    sketchResult,
                    baselineOutput.endTimestamp,
                    "rank"));
          }
        } else {
          // No ranks specified, query without parameters
          JsonNode baselineResult =
              (JsonNode) baselineQueryMethod.invoke(baselineOutput.precompute, (JsonNode) null);
          JsonNode sketchResult =
              (JsonNode) sketchQueryMethod.invoke(sketchOutput.precompute, (JsonNode) null);

          results.add(
              new QueryComparisonResult(
                  "global", baselineResult, sketchResult, baselineOutput.endTimestamp));
        }
      } else if (statisticType == StatisticType.PER_KEY && (keys == null || keys.isEmpty())) {
        throw new IllegalArgumentException("Keys must be provided for PER_KEY statistic type");
      } else {
        // Per-key statistic (frequency, etc.)
        for (String key : keys) {
          ObjectNode queryParams = objectMapper.createObjectNode();
          queryParams.put("key", key);

          JsonNode baselineResult =
              (JsonNode) baselineQueryMethod.invoke(baselineOutput.precompute, queryParams);
          JsonNode sketchResult =
              (JsonNode) sketchQueryMethod.invoke(sketchOutput.precompute, queryParams);

          results.add(
              new QueryComparisonResult(
                  key, baselineResult, sketchResult, baselineOutput.endTimestamp, "key"));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to query precompute objects: " + e.getMessage(), e);
    }

    return results;
  }

  /**
   * Run comparison for all keys in the accumulator (PER_KEY) or agnostic of key (KEY_AGNOSTIC).
   * Automatically detects keys from the baseline accumulator for PER_KEY statistics.
   *
   * @return List of comparison results
   */
  public List<QueryComparisonResult> allComparisons() throws Exception {
    if (statisticType == StatisticType.KEY_AGNOSTIC) {
      return runComparison(null);
    } else {
      java.lang.reflect.Method getKeySetMethod =
          baselineOutput.precompute.getClass().getMethod("getKeySet");
      Set<String> keys = (Set<String>) getKeySetMethod.invoke(baselineOutput.precompute);
      if (keys == null || keys.isEmpty()) {
        throw new IllegalStateException(
            "Baseline precompute does not provide key set for PER_KEY comparison");
      }
      return runComparison(keys);
    }
  }
}
