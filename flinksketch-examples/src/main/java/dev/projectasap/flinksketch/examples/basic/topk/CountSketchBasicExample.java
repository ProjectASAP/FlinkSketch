/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.examples.basic.topk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.PrecomputedOutput;
import dev.projectasap.flinksketch.sketches.custom.CountSketch;
import dev.projectasap.flinksketch.utils.AggregationConfig;
import dev.projectasap.flinksketch.windowfunctions.KeyedWindowProcessor;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/** Basic example demonstrating CountSketch with heap for top-K heavy hitters tracking. */
public class CountSketchBasicExample {
  public static void main(String[] args) throws Exception {
    // Initialize Flink execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Create input stream with varying item frequencies
    DataStream<DataPoint> inputStream = buildExampleInputStream(env);
    AggregationConfig config = new AggregationConfig();

    // Apply CountSketch aggregation in tumbling windows
    // CountSketch uses a heap to track the most frequent items (L2-heavy hitters)
    DataStream<PrecomputedOutput> outputStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .aggregate(
                createCountSketch(),
                new KeyedWindowProcessor(config, "insertion", "countsketch", false));

    // Query for top-K items and print results
    runQueries(outputStream);

    env.execute("CountSketch Top-K Basic Example");
  }

  /** Query top-K items using reflection */
  private static void runQueries(DataStream<PrecomputedOutput> outputStream) {
    ObjectMapper objectMapper = new ObjectMapper();

    outputStream
        .map(
            result -> {
              StringBuilder sb = new StringBuilder("\n=== Top-K Frequent Items ===\n");

              // Query for top-K items
              ObjectNode queryParams = objectMapper.createObjectNode();
              queryParams.put("statistic", "topk");

              java.lang.reflect.Method queryMethod =
                  result.precompute.getClass().getMethod("query", JsonNode.class);
              JsonNode topKResult = (JsonNode) queryMethod.invoke(result.precompute, queryParams);

              sb.append(String.format("Top items:\n%s\n", topKResult));

              return sb.toString();
            })
        .print();
  }

  private static CountSketch createCountSketch() {
    Map<String, String> sketchParams = new HashMap<>();
    sketchParams.put("rows", "3"); // Number of hash functions
    sketchParams.put("cols", "2048"); // Width of each counter array (must be power of 2)
    sketchParams.put("trackHH", "true"); // Enable heavy hitter tracking
    sketchParams.put("k", "10"); // Track top-10 items in heap
    return new CountSketch("count", sketchParams);
  }

  private static DataStream<DataPoint> buildExampleInputStream(StreamExecutionEnvironment env) {
    // Simulate item insertions with varying frequencies
    // "apple" appears most frequently, followed by "banana", then "cherry"
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
            new DataPoint(10L, "banana", 1),
            new DataPoint(11L, "apple", 1),
            new DataPoint(12L, "date", 1),
            new DataPoint(13L, "apple", 1),
            new DataPoint(14L, "elderberry", 1),
            new DataPoint(15L, "apple", 1));

    inputStream =
        inputStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.<DataPoint>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.timestamp));

    return inputStream;
  }
}
