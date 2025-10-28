/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.sketches.custom;

import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.Summary;
import java.util.Map;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Aggregate function for Count-Min Sketch probabilistic data structure. Provides approximate
 * frequency counts with configurable accuracy-space tradeoff.
 */
public class CountMinSketch
    implements AggregateFunction<DataPoint, CountMinSketchAccumulator, Summary> {

  private final String subType;
  private final int rows;
  private final int cols;

  /**
   * Constructs a CountMinSketch aggregate function.
   *
   * @param subType the aggregation subtype (e.g., "count" or "sum")
   * @param parameters configuration parameters including "rows" and "cols"
   */
  public CountMinSketch(String subType, Map<String, String> parameters) {
    this.subType = subType;
    if (!parameters.containsKey("rows") || !parameters.containsKey("cols")) {
      throw new IllegalArgumentException("Missing required parameters 'rows' and/or 'cols'");
    }
    this.rows = Integer.parseInt(parameters.get("rows"));
    this.cols = Integer.parseInt(parameters.get("cols"));

    // Positive # of rows and columns
    if (this.rows <= 0 || this.cols <= 0) {
      throw new IllegalArgumentException(
          "Number of rows (" + this.rows + ") and columns (" + this.cols + ") must be positive");
    }

    // # of rows must be less than # of columns
    if (this.rows > this.cols) {
      throw new IllegalArgumentException(
          "Number of rows (" + this.rows + ") cannot exceed number of columns (" + this.cols + ")");
    }

    // # of columns must be a positive power of 2
    if ((this.cols & (this.cols - 1)) != 0) {
      throw new IllegalArgumentException(
          "Number of columns (" + this.cols + ") must be a positive power of 2");
    }
  }

  @Override
  public CountMinSketchAccumulator createAccumulator() {
    Map<String, String> params = new java.util.HashMap<>();
    params.put("rows", String.valueOf(rows));
    params.put("cols", String.valueOf(cols));
    return new CountMinSketchAccumulator(subType, params);
  }

  @Override
  public CountMinSketchAccumulator add(DataPoint value, CountMinSketchAccumulator acc) {
    acc.add(value.key, value.value);
    return acc;
  }

  @Override
  public CountMinSketchAccumulator merge(CountMinSketchAccumulator a, CountMinSketchAccumulator b) {
    return a.merge(b);
  }

  @Override
  public Summary getResult(CountMinSketchAccumulator acc) {
    return acc;
  }
}
