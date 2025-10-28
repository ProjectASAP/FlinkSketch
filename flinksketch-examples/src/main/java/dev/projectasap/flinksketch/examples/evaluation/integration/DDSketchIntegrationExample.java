/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.examples.evaluation.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.PrecomputedOutput;
import dev.projectasap.flinksketch.sketches.baseline.ExactQuantiles;
import dev.projectasap.flinksketch.sketches.ddsketch.DDSketchQuantile;
import dev.projectasap.flinksketch.utils.AggregationConfig;
import dev.projectasap.flinksketch.windowfunctions.KeyedWindowProcessor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Minimal example demonstrating how to integrate DDSketch into an existing Flink pipeline. Shows
 * that replacing exact quantile computation with DDSketch requires only changing 2 lines of code in
 * your aggregation pipeline.
 */
public class DDSketchIntegrationExample {
  public static void main(String[] args) throws Exception {
    // Toggle this flag to switch between baseline and DDSketch pipeline
    boolean useDDSketch = true;

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<DataPoint> inputStream = buildExampleInputStream(env);

    if (useDDSketch) {
      runDDSketchPipeline(env, inputStream);
    } else {
      runBaselinePipeline(env, inputStream);
    }
  }

  /** BASELINE PIPELINE: Exact quantile computation */
  private static void runBaselinePipeline(
      StreamExecutionEnvironment env, DataStream<DataPoint> inputStream) throws Exception {
    ExactQuantiles quantiles = createExactQuantiles();
    AggregationConfig config = new AggregationConfig();

    DataStream<PrecomputedOutput> outputStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .aggregate(quantiles, new KeyedWindowProcessor(config, "latency", "exact", false));

    runQueries(outputStream);
    env.execute("Baseline ExactQuantiles Pipeline");
  }

  /**
   * DDSketch PIPELINE: Approximate quantile computation
   *
   * <p>Only 2 lines change from baseline: 1. Replace createExactQuantiles() with
   * createDDSketchQuantile() 2. Replace quantiles with ddSketch in .aggregate()
   */
  private static void runDDSketchPipeline(
      StreamExecutionEnvironment env, DataStream<DataPoint> inputStream) throws Exception {
    // CHANGE 1: DDSketch aggregator
    DDSketchQuantile ddSketch = createDDSketchQuantile();
    AggregationConfig config = new AggregationConfig();

    DataStream<PrecomputedOutput> outputStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            // CHANGE 2: Pass ddSketch instead of quantiles
            .aggregate(ddSketch, new KeyedWindowProcessor(config, "latency", "ddsketch", false));

    runQueries(outputStream);
    env.execute("DDSketch Pipeline");
  }

  private static DDSketchQuantile createDDSketchQuantile() {
    Map<String, String> sketchParams = new HashMap<>();
    sketchParams.put("relativeAccuracy", "0.01");
    sketchParams.put("maxNumBins", "2048");
    return new DDSketchQuantile("ddsketch", sketchParams);
  }

  private static ExactQuantiles createExactQuantiles() {
    Map<String, String> baselineParams = new HashMap<>();
    return new ExactQuantiles("exact", baselineParams);
  }

  private static DataStream<DataPoint> buildExampleInputStream(StreamExecutionEnvironment env) {
    // Simulate latency measurements: key is the latency value (as string), value is 1
    DataStream<DataPoint> inputStream =
        env.fromElements(
            new DataPoint(1L, "10.5", 1),
            new DataPoint(2L, "15.2", 1),
            new DataPoint(3L, "12.8", 1),
            new DataPoint(4L, "150.0", 1),
            new DataPoint(5L, "11.3", 1),
            new DataPoint(6L, "13.7", 1),
            new DataPoint(7L, "9.8", 1),
            new DataPoint(8L, "14.2", 1),
            new DataPoint(9L, "1000.0", 1),
            new DataPoint(10L, "11.9", 1));

    inputStream =
        inputStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.<DataPoint>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.timestamp));

    return inputStream;
  }

  /** Query quantiles at predefined ranks: p50, p90, p95, p99 and format output */
  private static void runQueries(DataStream<PrecomputedOutput> outputStream) {
    List<Double> quantileRanks = Arrays.asList(0.5, 0.9, 0.95, 0.99);
    ObjectMapper objectMapper = new ObjectMapper();

    outputStream
        .map(
            result -> {
              StringBuilder sb = new StringBuilder();
              for (Double rank : quantileRanks) {
                ObjectNode queryParams = objectMapper.createObjectNode();
                queryParams.put("rank", rank);

                java.lang.reflect.Method queryMethod =
                    result.precompute.getClass().getMethod("query", JsonNode.class);
                JsonNode value = (JsonNode) queryMethod.invoke(result.precompute, queryParams);
                sb.append(String.format("p%.0f=%s ", rank * 100, value));
              }
              return sb.toString();
            })
        .print();
  }
}
