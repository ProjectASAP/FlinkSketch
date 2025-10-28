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
 * Aggregate function for computing exact top-K items by frequency or sum. Maintains all items and
 * their counts to provide exact top-K results.
 */
public class ExactTopK implements AggregateFunction<DataPoint, ExactTopkAccumulator, Summary> {
  private final String aggregationSubType;
  private final int topCount;

  public ExactTopK(String aggregationSubType, Map<String, String> parameters) {
    this.aggregationSubType = aggregationSubType;
    this.topCount = Integer.parseInt(parameters.get("k"));
  }

  @Override
  public ExactTopkAccumulator createAccumulator() {
    return new ExactTopkAccumulator(topCount);
  }

  @Override
  public ExactTopkAccumulator add(DataPoint value, ExactTopkAccumulator acc) {
    String key = value.key;
    if ("sum".equals(aggregationSubType)) {
      acc.add(key, value.value);
    } else {
      acc.add(key, 1);
    }

    return acc;
  }

  @Override
  public ExactTopkAccumulator merge(ExactTopkAccumulator a, ExactTopkAccumulator b) {
    return a.merge(b);
  }

  @Override
  public Summary getResult(ExactTopkAccumulator acc) {
    return acc;
  }
}
