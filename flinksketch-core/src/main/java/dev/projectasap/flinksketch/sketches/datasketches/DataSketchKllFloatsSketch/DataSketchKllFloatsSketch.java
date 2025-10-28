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
 * Aggregate function for Apache DataSketches KLL quantiles sketch. Provides approximate quantile
 * estimation for floating-point values.
 */
public class DataSketchKllFloatsSketch
    implements AggregateFunction<DataPoint, KllFloatsSketchAccumulator, Summary> {
  private final int sketchSize;
  private final String aggregationSubType;

  public DataSketchKllFloatsSketch(String aggregationSubType, Map<String, String> parameters) {
    this.aggregationSubType = aggregationSubType;
    if (!parameters.containsKey("k")) {
      throw new IllegalArgumentException("Missing required parameter 'k'");
    }
    this.sketchSize = Integer.parseInt(parameters.get("k"));
  }

  @Override
  public KllFloatsSketchAccumulator createAccumulator() {
    return new KllFloatsSketchAccumulator(sketchSize);
  }

  @Override
  public KllFloatsSketchAccumulator add(DataPoint value, KllFloatsSketchAccumulator acc) {
    acc.add(value.key, value.value);
    return acc;
  }

  @Override
  public KllFloatsSketchAccumulator merge(
      KllFloatsSketchAccumulator a, KllFloatsSketchAccumulator b) {
    return a.merge(b);
  }

  @Override
  public Summary getResult(KllFloatsSketchAccumulator acc) {
    return acc;
  }
}
