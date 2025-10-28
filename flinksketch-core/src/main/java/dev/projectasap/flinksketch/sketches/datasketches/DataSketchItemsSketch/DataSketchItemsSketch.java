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
 * Aggregate function for Apache DataSketches Frequent Items sketch. Provides approximate frequency
 * counts for items.
 */
public class DataSketchItemsSketch
    implements AggregateFunction<DataPoint, ItemsSketchAccumulator, Summary> {
  private final int maxMapSize;
  private final String aggregationSubType;

  /**
   * Constructs a DataSketchItemsSketch aggregate function.
   *
   * @param aggregationSubType the aggregation subtype
   * @param parameters configuration parameters including "maxMapSize"
   */
  public DataSketchItemsSketch(String aggregationSubType, Map<String, String> parameters) {
    this.aggregationSubType = aggregationSubType;
    if (!parameters.containsKey("maxMapSize")) {
      throw new IllegalArgumentException("Missing required parameter 'maxMapSize'");
    }
    this.maxMapSize = Integer.parseInt(parameters.get("maxMapSize"));
  }

  @Override
  public ItemsSketchAccumulator createAccumulator() {
    return new ItemsSketchAccumulator(maxMapSize);
  }

  @Override
  public ItemsSketchAccumulator add(DataPoint value, ItemsSketchAccumulator acc) {
    acc.add(value.key, value.value);
    return acc;
  }

  @Override
  public ItemsSketchAccumulator merge(ItemsSketchAccumulator a, ItemsSketchAccumulator b) {
    return a.merge(b);
  }

  @Override
  public Summary getResult(ItemsSketchAccumulator acc) {
    return acc;
  }
}
