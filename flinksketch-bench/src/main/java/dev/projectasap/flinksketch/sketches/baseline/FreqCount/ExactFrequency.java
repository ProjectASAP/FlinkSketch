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
 * Aggregate function for computing exact frequency counts. Maintains exact counts for all items in
 * a HashMap.
 */
public class ExactFrequency
    implements AggregateFunction<DataPoint, ExactFrequencyAccumulator, Summary> {
  private final String subType;
  private final Map<String, String> parameters;

  public ExactFrequency(String subType, Map<String, String> parameters) {
    this.subType = subType;
    this.parameters = parameters;
  }

  @Override
  public ExactFrequencyAccumulator createAccumulator() {
    return new ExactFrequencyAccumulator();
  }

  @Override
  public ExactFrequencyAccumulator add(DataPoint value, ExactFrequencyAccumulator acc) {
    String key = value.key;
    if ("sum".equals(subType)) {
      acc.add(key, value.value);
    } else {
      acc.add(key, 1);
    }

    return acc;
  }

  @Override
  public ExactFrequencyAccumulator merge(ExactFrequencyAccumulator a, ExactFrequencyAccumulator b) {
    return a.merge(b);
  }

  @Override
  public Summary getResult(ExactFrequencyAccumulator acc) {
    return acc;
  }
}
