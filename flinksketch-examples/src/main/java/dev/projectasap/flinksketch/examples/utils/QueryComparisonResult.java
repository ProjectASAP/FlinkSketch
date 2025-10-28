/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.examples.utils;

import com.fasterxml.jackson.databind.JsonNode;

public class QueryComparisonResult {
  public final String key;
  public final JsonNode baselineResult;
  public final JsonNode sketchResult;
  public final Long timestamp;
  public final String keyLabel; // "key" or "rank" or other label

  // Default constructor - uses "key" as the label
  public QueryComparisonResult(String key, JsonNode baseline, JsonNode sketch, Long timestamp) {
    this(key, baseline, sketch, timestamp, "key");
  }

  // Constructor with custom label
  public QueryComparisonResult(
      String key, JsonNode baseline, JsonNode sketch, Long timestamp, String keyLabel) {
    this.key = key;
    this.baselineResult = baseline;
    this.sketchResult = sketch;
    this.timestamp = timestamp;
    this.keyLabel = keyLabel;
  }

  @Override
  public String toString() {
    return String.format(
        "QueryResult{%s='%s', baseline=%s, sketch=%s, timestamp=%d}",
        keyLabel, key, baselineResult, sketchResult, timestamp);
  }
}
