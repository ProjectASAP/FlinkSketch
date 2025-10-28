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
import dev.projectasap.flinksketch.sketches.baseline.ExactFrequency;
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
 * Minimal example demonstrating how to integrate Univmon for frequency queries. Shows that
 * replacing exact frequency counting with Univmon requires only changing 2 lines.
 *
 * @deprecated Use {@link
 *     dev.projectasap.flinksketch.examples.basic.multiple_statistics_per_sketch.UnivmonExample}
 *     instead, which demonstrates all Univmon query types in a unified example.
 */
@Deprecated
public class UnivmonFrequencyIntegrationExample {
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

  /** BASELINE PIPELINE: Exact frequency counting */
  private static void runBaselinePipeline(
      StreamExecutionEnvironment env, DataStream<DataPoint> inputStream) throws Exception {
    ExactFrequency exactFrequency = createExactFrequency();
    AggregationConfig config = new AggregationConfig();

    DataStream<PrecomputedOutput> outputStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .aggregate(
                exactFrequency, new KeyedWindowProcessor(config, "insertion", "exact", false));

    runQueries(outputStream);
    env.execute("Baseline Frequency Pipeline");
  }

  /**
   * UNIVMON PIPELINE: Approximate frequency estimation
   *
   * <p>Only 2 lines change from baseline: 1. Replace createExactFrequency() with createUnivmon() 2.
   * Replace exactFrequency with univmon in .aggregate()
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
            // CHANGE 2: Pass univmon instead of exactFrequency
            .aggregate(univmon, new KeyedWindowProcessor(config, "insertion", "univmon", false));

    runQueries(outputStream);
    env.execute("Univmon Frequency Pipeline");
  }

  /** Query frequencies for specific keys */
  private static void runQueries(DataStream<PrecomputedOutput> outputStream) {
    ObjectMapper objectMapper = new ObjectMapper();
    String[] keysToQuery = {"apple", "banana", "cherry"};

    outputStream
        .map(
            result -> {
              StringBuilder output = new StringBuilder("Frequency Queries:\n");

              for (String key : keysToQuery) {
                ObjectNode queryParams = objectMapper.createObjectNode();
                queryParams.put("statistic", "freq");
                queryParams.put("key", key);

                java.lang.reflect.Method queryMethod =
                    result.precompute.getClass().getMethod("query", JsonNode.class);
                JsonNode value = (JsonNode) queryMethod.invoke(result.precompute, queryParams);
                output.append(String.format("  %s: %s\n", key, value));
              }

              return output.toString();
            })
        .print();
  }

  private static Univmon createUnivmon() {
    Map<String, String> sketchParams = new HashMap<>();
    sketchParams.put("levels", "16");
    sketchParams.put("rows", "3");
    sketchParams.put("cols", "2048");
    sketchParams.put("k", "3");
    return new Univmon("count", sketchParams);
  }

  private static ExactFrequency createExactFrequency() {
    Map<String, String> baselineParams = new HashMap<>();
    return new ExactFrequency("exact", baselineParams);
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
