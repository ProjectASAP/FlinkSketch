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
import dev.projectasap.flinksketch.sketches.baseline.ExactTopK;
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
 * Minimal example demonstrating how to integrate Univmon for top-K queries. Shows that replacing
 * exact top-K computation with Univmon requires only changing 2 lines.
 *
 * @deprecated Use {@link
 *     dev.projectasap.flinksketch.examples.basic.multiple_statistics_per_sketch.UnivmonExample}
 *     instead, which demonstrates all Univmon query types in a unified example.
 */
@Deprecated
public class UnivmonTopKIntegrationExample {
  public static void main(String[] args) throws Exception {
    // Toggle this flag to switch between baseline and Univmon pipeline
    boolean useUnivmon = true;

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<DataPoint> inputStream = buildExampleInputStream(env);

    if (useUnivmon) {
      runUnivmonPipeline(env, inputStream);
    } else {
      runBaselinePipeline(env, inputStream);
    }
  }

  /** BASELINE PIPELINE: Exact top-K computation */
  private static void runBaselinePipeline(
      StreamExecutionEnvironment env, DataStream<DataPoint> inputStream) throws Exception {
    ExactTopK topK = createTopK();
    AggregationConfig config = new AggregationConfig();

    DataStream<PrecomputedOutput> outputStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .aggregate(topK, new KeyedWindowProcessor(config, "insertion", "exact", false));

    runQueries(outputStream, "topk");
    env.execute("Baseline TopK Pipeline");
  }

  /**
   * UNIVMON PIPELINE: Approximate top-K estimation
   *
   * <p>Only 2 lines change from baseline: 1. Replace createTopK() with createUnivmon() 2. Replace
   * topK with univmon in .aggregate()
   */
  private static void runUnivmonPipeline(
      StreamExecutionEnvironment env, DataStream<DataPoint> inputStream) throws Exception {
    // CHANGE 1: Univmon aggregator
    Univmon univmon = createUnivmon();
    AggregationConfig config = new AggregationConfig();

    DataStream<PrecomputedOutput> outputStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            // CHANGE 2: Pass univmon instead of topK
            .aggregate(univmon, new KeyedWindowProcessor(config, "insertion", "count", false));

    runQueries(outputStream, "topk");
    env.execute("Univmon TopK Pipeline");
  }

  /** Query top-K and format output */
  private static void runQueries(DataStream<PrecomputedOutput> outputStream, String statistic) {
    ObjectMapper objectMapper = new ObjectMapper();

    outputStream
        .map(
            result -> {
              ObjectNode queryParams = objectMapper.createObjectNode();
              queryParams.put("statistic", "topk");

              java.lang.reflect.Method queryMethod =
                  result.precompute.getClass().getMethod("query", JsonNode.class);
              JsonNode value = (JsonNode) queryMethod.invoke(result.precompute, queryParams);
              return String.format("Top-K: %s", value);
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

  private static ExactTopK createTopK() {
    Map<String, String> baselineParams = new HashMap<>();
    baselineParams.put("k", "10");
    return new ExactTopK("count", baselineParams);
  }

  private static DataStream<DataPoint> buildExampleInputStream(StreamExecutionEnvironment env) {
    // Simulate item insertions with varying frequencies
    DataStream<DataPoint> inputStream =
        env.fromElements(
            new DataPoint(1L, "apple", 1),
            new DataPoint(2L, "banana", 1),
            new DataPoint(3L, "apple", 1),
            new DataPoint(4L, "cherry", 1),
            new DataPoint(5L, "banana", 1),
            new DataPoint(6L, "apple", 1),
            new DataPoint(7L, "banana", 1),
            new DataPoint(8L, "apple", 1),
            new DataPoint(9L, "cherry", 1),
            new DataPoint(10L, "banana", 1));

    inputStream =
        inputStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.<DataPoint>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.timestamp));

    return inputStream;
  }
}
