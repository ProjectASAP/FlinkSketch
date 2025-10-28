/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.sketches.custom;

import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.Summary;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Aggregate function for Count Sketch algorithm. Provides frequency estimation and L2-heavy hitter
 * tracking.
 */
public class CountSketch implements AggregateFunction<DataPoint, CountSketchAccumulator, Summary> {

  private final int rowNum;
  private final int colNum;
  private final int topCount;
  private final String subType;
  private final boolean trackHeavyHitters;

  /**
   * Constructs a CountSketch aggregate function.
   *
   * @param aggregationSubType the aggregation subtype
   * @param parameters configuration parameters including "rows", "cols", "trackHH", and "k"
   */
  public CountSketch(String aggregationSubType, Map<String, String> parameters) {
    this.rowNum = Integer.parseInt(parameters.get("rows"));
    this.colNum = Integer.parseInt(parameters.get("cols"));

    // Positive # of rows and columns
    if (this.rowNum <= 0 || this.colNum <= 0) {
      throw new IllegalArgumentException(
          "Number of rows ("
              + this.rowNum
              + ") and columns ("
              + this.colNum
              + ") must be positive");
    }

    // # of rows must be less than # of columns
    if (this.rowNum > this.colNum) {
      throw new IllegalArgumentException(
          "Number of rows ("
              + this.rowNum
              + ") cannot exceed number of columns ("
              + this.colNum
              + ")");
    }

    // # of columns must be a positive power of 2
    if ((this.colNum & (this.colNum - 1)) != 0) {
      throw new IllegalArgumentException(
          "Number of columns (" + this.colNum + ") must be a positive power of 2");
    }

    if (!parameters.containsKey("trackHH")) {
      throw new IllegalArgumentException("Missing required parameter 'trackHH'");
    }
    this.trackHeavyHitters = Boolean.parseBoolean(parameters.get("trackHH"));
    if (this.trackHeavyHitters) {
      if (!parameters.containsKey("k")) {
        throw new IllegalArgumentException("Missing required parameter 'k' when trackHH=true");
      }
      this.topCount = Integer.parseInt(parameters.get("k"));
    } else {
      this.topCount = 0;
    }

    this.subType = aggregationSubType;
  }

  @Override
  public CountSketchAccumulator createAccumulator() {
    Map<String, String> params = new HashMap<>();
    params.put("rows", String.valueOf(rowNum));
    params.put("cols", String.valueOf(colNum));
    params.put("k", String.valueOf(topCount));
    params.put("trackHH", String.valueOf(trackHeavyHitters));
    return new CountSketchAccumulator(subType, params);
  }

  @Override
  public CountSketchAccumulator add(DataPoint value, CountSketchAccumulator acc) {
    acc.add(value.key, value.value);
    return acc;
  }

  @Override
  public CountSketchAccumulator merge(CountSketchAccumulator a, CountSketchAccumulator b) {
    return a.merge(b);
  }

  @Override
  public Summary getResult(CountSketchAccumulator acc) {
    return acc;
  }
}
