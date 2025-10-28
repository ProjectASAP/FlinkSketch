/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.sketches.ddsketch;

import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.Summary;
import java.util.Map;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Aggregate function for Datadog DDSketch. Provides approximate quantile estimation for
 * floating-point values with relative-error guarantees. Supports both bounded (collapsing) and
 * unbounded variants.
 */
public class DDSketchQuantile
    implements AggregateFunction<DataPoint, DDSketchAccumulator, Summary> {
  private final double relativeAccuracy;
  private final int maxNumBins;
  private final boolean unbounded;
  private final String aggregationSubType;

  public DDSketchQuantile(String aggregationSubType, Map<String, String> parameters) {
    this.aggregationSubType = aggregationSubType;

    // Require explicit configuration - no defaults
    if (!parameters.containsKey("relativeAccuracy")) {
      throw new IllegalArgumentException("relativeAccuracy parameter is required for DDSketch");
    }

    this.relativeAccuracy = Double.parseDouble(parameters.get("relativeAccuracy"));

    // unbounded parameter is optional (defaults to false)
    this.unbounded =
        parameters.containsKey("unbounded") && Boolean.parseBoolean(parameters.get("unbounded"));

    // maxNumBins is only required if unbounded is false
    if (!unbounded && !parameters.containsKey("maxNumBins")) {
      throw new IllegalArgumentException("maxNumBins parameter is required for bounded DDSketch");
    }

    this.maxNumBins = unbounded ? 0 : Integer.parseInt(parameters.get("maxNumBins"));
  }

  @Override
  public DDSketchAccumulator createAccumulator() {
    return new DDSketchAccumulator(relativeAccuracy, maxNumBins, unbounded);
  }

  @Override
  public DDSketchAccumulator add(DataPoint value, DDSketchAccumulator acc) {
    acc.add(value.key, value.value);
    return acc;
  }

  @Override
  public DDSketchAccumulator merge(DDSketchAccumulator a, DDSketchAccumulator b) {
    return a.merge(b);
  }

  @Override
  public Summary getResult(DDSketchAccumulator acc) {
    return acc;
  }
}
