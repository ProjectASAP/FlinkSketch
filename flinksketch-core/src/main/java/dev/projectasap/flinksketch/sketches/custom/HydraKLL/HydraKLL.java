/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.sketches.custom;

import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.Summary;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Aggregate function for HydraKLL algorithm. Provides quantile estimation for different
 * subpopulations of data.
 */
public class HydraKLL implements AggregateFunction<DataPoint, HydraKLLAccumulator, Summary> {

  private final int rowNum;
  private final int colNum;
  private final int kllSize;

  /**
   * Constructs a HydraKLL aggregate function.
   *
   * @param parameters configuration parameters including "rows", "cols", "k"
   */
  public HydraKLL(String aggregationSubType, Map<String, String> parameters) {
    this.rowNum = Integer.parseInt(parameters.get("rows"));
    this.colNum = Integer.parseInt(parameters.get("cols"));
    this.kllSize = Integer.parseInt(parameters.get("k"));
  }

  @Override
  public HydraKLLAccumulator createAccumulator() {
    Map<String, String> params = new HashMap<>();
    params.put("rows", String.valueOf(rowNum));
    params.put("cols", String.valueOf(colNum));
    params.put("k", String.valueOf(kllSize));
    return new HydraKLLAccumulator(params);
  }

  @Override
  public HydraKLLAccumulator add(DataPoint value, HydraKLLAccumulator acc) {
    acc.add(value.key, value.value);
    return acc;
  }

  @Override
  public HydraKLLAccumulator merge(HydraKLLAccumulator a, HydraKLLAccumulator b) {
    return a.merge(b);
  }

  @Override
  public Summary getResult(HydraKLLAccumulator acc) {
    return acc;
  }
}
