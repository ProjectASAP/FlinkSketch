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
import dev.projectasap.flinksketch.sketches.baseline.ExactFrequency;
import dev.projectasap.flinksketch.sketches.custom.CountMinSketch;
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
 * Minimal example demonstrating how to integrate CountMinSketch into an existing Flink pipeline.
 * Shows that replacing exact frequency counting with CountMinSketch requires only changing 2 lines
 * of code in your aggregation pipeline.
 */
public class CountMinSketchIntegrationExample {
  public static void main(String[] args) throws Exception {
    // Toggle this flag to switch between baseline and CountMinSketch pipeline
    boolean useCountMinSketch = true;

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<DataPoint> inputStream = buildExampleInputStream(env);

    if (useCountMinSketch) {
      runCountMinSketchPipeline(env, inputStream);
    } else {
      runBaselinePipeline(env, inputStream);
    }
  }

  /** BASELINE PIPELINE: Exact frequency counting */
  private static void runBaselinePipeline(
      StreamExecutionEnvironment env, DataStream<DataPoint> inputStream) throws Exception {
    ExactFrequency exactFrequency = createExactFrequency();
    AggregationConfig config = new AggregationConfig();

    DataStream<PrecomputedOutput> outputStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .aggregate(
                exactFrequency, new KeyedWindowProcessor(config, "insertion", "exact", false));

    runQueries(outputStream);
    env.execute("Baseline ExactFrequency Pipeline");
  }

  /**
   * CountMinSketch PIPELINE: Approximate frequency counting
   *
   * <p>Only 2 lines change from baseline: 1. Replace createExactFrequency() with
   * createCountMinSketch() 2. Replace exactFrequency with countMinSketch in .aggregate()
   */
  private static void runCountMinSketchPipeline(
      StreamExecutionEnvironment env, DataStream<DataPoint> inputStream) throws Exception {
    // CHANGE 1: CountMinSketch aggregator
    CountMinSketch countMinSketch = createCountMinSketch();
    AggregationConfig config = new AggregationConfig();

    DataStream<PrecomputedOutput> outputStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            // CHANGE 2: Pass countMinSketch instead of exactFrequency
            .aggregate(
                countMinSketch,
                new KeyedWindowProcessor(config, "insertion", "countminsketch", false));

    runQueries(outputStream);
    env.execute("CountMinSketch Pipeline");
  }

  private static CountMinSketch createCountMinSketch() {
    Map<String, String> sketchParams = new HashMap<>();
    sketchParams.put("rows", "3");
    sketchParams.put("cols", "2048");
    return new CountMinSketch("count", sketchParams);
  }

  private static ExactFrequency createExactFrequency() {
    Map<String, String> baselineParams = new HashMap<>();
    return new ExactFrequency("count", baselineParams);
  }

  private static DataStream<DataPoint> buildExampleInputStream(StreamExecutionEnvironment env) {
    // Simulate item insertions: key is the item name, value is 1
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

  /** Query frequencies for predefined keys and format output */
  private static void runQueries(DataStream<PrecomputedOutput> outputStream) {
    List<String> queryKeys = Arrays.asList("apple", "banana", "cherry");
    ObjectMapper objectMapper = new ObjectMapper();

    outputStream
        .map(
            result -> {
              StringBuilder sb = new StringBuilder();
              for (String key : queryKeys) {
                ObjectNode queryParams = objectMapper.createObjectNode();
                queryParams.put("key", key);

                java.lang.reflect.Method queryMethod =
                    result.precompute.getClass().getMethod("query", JsonNode.class);
                JsonNode value = (JsonNode) queryMethod.invoke(result.precompute, queryParams);
                sb.append(String.format("%s=%s ", key, value));
              }
              return sb.toString();
            })
        .print();
  }
}
