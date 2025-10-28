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
 * Aggregate function for computing exact quantiles. Stores all values to provide exact quantile
 * calculations.
 */
public class ExactQuantiles
    implements AggregateFunction<DataPoint, ExactQuantilesAccumulator, Summary> {
  private final String subType;
  private final Map<String, String> parameters;

  public ExactQuantiles(String subType, Map<String, String> parameters) {
    this.subType = subType;
    this.parameters = parameters;
  }

  @Override
  public ExactQuantilesAccumulator createAccumulator() {
    return new ExactQuantilesAccumulator();
  }

  @Override
  public ExactQuantilesAccumulator add(DataPoint value, ExactQuantilesAccumulator acc) {
    acc.add(value.key, value.value);
    return acc;
  }

  @Override
  public ExactQuantilesAccumulator merge(ExactQuantilesAccumulator a, ExactQuantilesAccumulator b) {
    return a.merge(b);
  }

  @Override
  public Summary getResult(ExactQuantilesAccumulator acc) {
    return acc;
  }
}
