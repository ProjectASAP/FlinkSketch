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
 * UnivMon AggregateFunction core logic.
 *
 * <p>Implements the UnivMon sketch described in the paper: "One Sketch to Rule Them All: Rethinking
 * Network Flow Monitoring with UnivMon" (SIGCOMM '16), which can be found here:
 * dl.acm.org/doi/pdf/10.1145/2934872.2934906.
 */
public class Univmon implements AggregateFunction<DataPoint, UnivmonAccumulator, Summary> {
  private final int rowNum;
  private final int colNum;
  // # of levels --> log(n) levels for Algorithm 1 (Univmon Online Sketching Step)
  private final int levelNum;
  private final int numHeavyHitters; // Top-k L2 heavy hitters
  private final String subType;

  /**
   * Sets up parameters for each instantiation. Each aggregate instance models one UnivMon
   * "pipeline" as described in Fig. 1 & 6.
   */
  public Univmon(String aggregationSubType, Map<String, String> parameters) {
    this.levelNum = Integer.parseInt(parameters.get("levels")); // log(n), see Algorithm 1
    this.rowNum = Integer.parseInt(parameters.get("rows")); // Row: t in CountSketch
    this.colNum = Integer.parseInt(parameters.get("cols")); // Col: w in CountSketch

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

    this.numHeavyHitters = Integer.parseInt(parameters.get("k")); // # heavy hitters to track
    this.subType = aggregationSubType;
  }

  /**
   * Create the accumulator for this aggregate. Each accumulator contains count sketches for each
   * recursion level.
   */
  @Override
  public UnivmonAccumulator createAccumulator() {
    Map<String, String> params = new HashMap<>();
    params.put("rows", String.valueOf(rowNum));
    params.put("cols", String.valueOf(colNum));
    params.put("levels", String.valueOf(levelNum));
    params.put("k", String.valueOf(numHeavyHitters));
    return new UnivmonAccumulator(subType, params); // See §4: parallel L2-HH sketches
  }

  @Override
  public UnivmonAccumulator add(DataPoint value, UnivmonAccumulator acc) {
    acc.add(value.key, value.value);
    return acc;
  }

  @Override
  public UnivmonAccumulator merge(UnivmonAccumulator a, UnivmonAccumulator b) {
    return a.merge(b);
  }

  @Override
  public Summary getResult(UnivmonAccumulator acc) {
    return acc;
  }
}
