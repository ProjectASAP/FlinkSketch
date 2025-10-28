/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.sketches.baseline;

import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.Summary;
import java.util.Map;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Aggregate function for computing exact per-key quantiles. Stores all values per key to provide
 * exact quantile calculations for each key independently.
 */
public class PerKeyQuantiles
    implements AggregateFunction<DataPoint, PerKeyQuantilesAccumulator, Summary> {

  public PerKeyQuantiles(String aggregationSubType, Map<String, String> parameters) {
    // No parameters needed for exact baseline
  }

  @Override
  public PerKeyQuantilesAccumulator createAccumulator() {
    return new PerKeyQuantilesAccumulator();
  }

  @Override
  public PerKeyQuantilesAccumulator add(DataPoint value, PerKeyQuantilesAccumulator acc) {
    acc.add(value.key, value.value);
    return acc;
  }

  @Override
  public PerKeyQuantilesAccumulator merge(
      PerKeyQuantilesAccumulator a, PerKeyQuantilesAccumulator b) {
    return a.merge(b);
  }

  @Override
  public Summary getResult(PerKeyQuantilesAccumulator acc) {
    return acc;
  }
}
