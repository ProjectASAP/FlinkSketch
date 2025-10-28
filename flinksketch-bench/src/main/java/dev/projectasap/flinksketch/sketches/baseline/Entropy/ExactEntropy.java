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
 * Aggregate function for computing exact Shannon entropy. Tracks counts of each key in an
 * accumulator, and computes entropy at the end.
 */
public class ExactEntropy
    implements AggregateFunction<DataPoint, ExactEntropyAccumulator, Summary> {
  private final String aggregationSubType;
  private final Map<String, String> parameters;

  public ExactEntropy(String aggregationSubType, Map<String, String> parameters) {
    this.aggregationSubType = aggregationSubType;
    this.parameters = parameters;
  }

  @Override
  public ExactEntropyAccumulator createAccumulator() {
    return new ExactEntropyAccumulator();
  }

  @Override
  public ExactEntropyAccumulator add(DataPoint value, ExactEntropyAccumulator acc) {
    acc.add(value.key, value.value);
    return acc;
  }

  @Override
  public ExactEntropyAccumulator merge(ExactEntropyAccumulator a, ExactEntropyAccumulator b) {
    return a.merge(b);
  }

  @Override
  public Summary getResult(ExactEntropyAccumulator acc) {
    return acc;
  }
}
