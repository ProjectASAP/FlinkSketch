/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.examples.sketch_integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.PrecomputedOutput;
import dev.projectasap.flinksketch.sketches.baseline.ExactCardinality;
import dev.projectasap.flinksketch.sketches.custom.Univmon;
import dev.projectasap.flinksketch.utils.AggregationConfig;
import dev.projectasap.flinksketch.windowfunctions.KeyedWindowProcessor;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Minimal example demonstrating how to integrate Univmon for cardinality estimation. Shows that
 * replacing exact cardinality counting with Univmon requires only changing 2 lines.
 *
 * @deprecated Use {@link
 *     dev.projectasap.flinksketch.examples.basic.multiple_statistics_per_sketch.UnivmonExample}
 *     instead, which demonstrates all Univmon query types in a unified example.
 */
@Deprecated
public class UnivmonCardinalityIntegrationExample {
  public static void main(String[] args) throws Exception {
    // Toggle this flag to switch between baseline and Univmon pipeline
    boolean useUnivmon = false;

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<DataPoint> inputStream = buildExampleInputStream(env);

    if (useUnivmon) {
      runUnivmonPipeline(env, inputStream);
    } else {
      runBaselinePipeline(env, inputStream);
    }
  }

  /** BASELINE PIPELINE: Exact cardinality counting */
  private static void runBaselinePipeline(
      StreamExecutionEnvironment env, DataStream<DataPoint> inputStream) throws Exception {
    ExactCardinality cardinality = createCardinality();
    AggregationConfig config = new AggregationConfig();

    DataStream<PrecomputedOutput> outputStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .aggregate(cardinality, new KeyedWindowProcessor(config, "insertion", "exact", false));

    runQueries(outputStream, "cardinality");
    env.execute("Baseline Cardinality Pipeline");
  }

  /**
   * UNIVMON PIPELINE: Approximate cardinality estimation
   *
   * <p>Only 2 lines change from baseline: 1. Replace createCardinality() with createUnivmon() 2.
   * Replace cardinality with univmon in .aggregate()
   */
  private static void runUnivmonPipeline(
      StreamExecutionEnvironment env, DataStream<DataPoint> inputStream) throws Exception {
    // CHANGE 1: Univmon aggregator
    Univmon univmon = createUnivmon();
    AggregationConfig config = new AggregationConfig();

    DataStream<PrecomputedOutput> outputStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            // CHANGE 2: Pass univmon instead of cardinality
            .aggregate(univmon, new KeyedWindowProcessor(config, "insertion", "univmon", false));

    runQueries(outputStream, "cardinality");
    env.execute("Univmon Cardinality Pipeline");
  }

  /** Query cardinality and format output */
  private static void runQueries(DataStream<PrecomputedOutput> outputStream, String statistic) {
    ObjectMapper objectMapper = new ObjectMapper();

    outputStream
        .map(
            result -> {
              ObjectNode queryParams = objectMapper.createObjectNode();
              queryParams.put("statistic", statistic);

              java.lang.reflect.Method queryMethod =
                  result.precompute.getClass().getMethod("query", JsonNode.class);
              JsonNode queryResult = (JsonNode) queryMethod.invoke(result.precompute, queryParams);

              // Extract distinct_count from result: {"distinct_count": value}
              double distinctCount = queryResult.get("distinct_count").asDouble();
              return String.format("Cardinality (distinct count): %.2f", distinctCount);
            })
        .print();
  }

  private static Univmon createUnivmon() {
    Map<String, String> sketchParams = new HashMap<>();
    sketchParams.put("levels", "5");
    sketchParams.put("rows", "3");
    sketchParams.put("cols", "2048");
    sketchParams.put("k", "10");
    return new Univmon("count", sketchParams);
  }

  private static ExactCardinality createCardinality() {
    Map<String, String> baselineParams = new HashMap<>();
    return new ExactCardinality("exact", baselineParams);
  }

  private static DataStream<DataPoint> buildExampleInputStream(StreamExecutionEnvironment env) {
    // Simulate item insertions with many unique keys
    DataStream<DataPoint> inputStream =
        env.fromElements(
            new DataPoint(1L, "item1", 1),
            new DataPoint(2L, "item2", 1),
            new DataPoint(3L, "item3", 1),
            new DataPoint(4L, "item4", 1),
            new DataPoint(5L, "item5", 1),
            new DataPoint(6L, "item6", 1),
            new DataPoint(7L, "item7", 1),
            new DataPoint(8L, "item8", 1),
            new DataPoint(9L, "item9", 1),
            new DataPoint(10L, "item10", 1));

    inputStream =
        inputStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.<DataPoint>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.timestamp));

    return inputStream;
  }
}
