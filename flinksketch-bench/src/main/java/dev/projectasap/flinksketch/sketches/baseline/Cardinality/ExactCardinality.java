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
 * Aggregate function for computing exact cardinality (distinct count). Uses a HashSet-based
 * accumulator to track distinct keys.
 */
public class ExactCardinality
    implements AggregateFunction<DataPoint, ExactCardinalityAccumulator, Summary> {
  private final String aggregationSubType;
  private final Map<String, String> parameters;

  public ExactCardinality(String aggregationSubType, Map<String, String> parameters) {
    this.aggregationSubType = aggregationSubType;
    this.parameters = parameters;
  }

  @Override
  public ExactCardinalityAccumulator createAccumulator() {
    return new ExactCardinalityAccumulator();
  }

  @Override
  public ExactCardinalityAccumulator add(DataPoint value, ExactCardinalityAccumulator acc) {
    acc.add(value.key, value.value);
    return acc;
  }

  @Override
  public ExactCardinalityAccumulator merge(
      ExactCardinalityAccumulator a, ExactCardinalityAccumulator b) {
    return a.merge(b);
  }

  @Override
  public Summary getResult(ExactCardinalityAccumulator acc) {
    return acc;
  }
}
