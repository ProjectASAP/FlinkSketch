/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.sketches.datasketches;

import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.Summary;
import java.util.Map;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Aggregate function for Apache DataSketches HyperLogLog (HLL) sketch. Provides approximate
 * distinct count with tunable accuracy.
 */
public class DataSketchHllSketch
    implements AggregateFunction<DataPoint, HllSketchAccumulator, Summary> {
  private final int lgK;
  private final String aggregationSubType;

  public DataSketchHllSketch(String aggregationSubType, Map<String, String> parameters) {
    this.aggregationSubType = aggregationSubType;
    if (!parameters.containsKey("lgK")) {
      throw new IllegalArgumentException("Missing required parameter 'lgK'");
    }
    this.lgK = Integer.parseInt(parameters.get("lgK"));
  }

  @Override
  public HllSketchAccumulator createAccumulator() {
    return new HllSketchAccumulator(lgK);
  }

  @Override
  public HllSketchAccumulator add(DataPoint value, HllSketchAccumulator acc) {
    acc.add(value.key, value.value);
    return acc;
  }

  @Override
  public HllSketchAccumulator merge(HllSketchAccumulator a, HllSketchAccumulator b) {
    return a.merge(b);
  }

  @Override
  public Summary getResult(HllSketchAccumulator acc) {
    return acc;
  }
}
