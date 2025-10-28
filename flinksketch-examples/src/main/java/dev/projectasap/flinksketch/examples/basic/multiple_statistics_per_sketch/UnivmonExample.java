/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.examples.basic.multiple_statistics_per_sketch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.PrecomputedOutput;
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
 * Unified example demonstrating Univmon's multiple query capabilities (frequency, cardinality,
 * top-K).
 */
public class UnivmonExample {
  public static void main(String[] args) throws Exception {
    // Initialize Flink execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Create input stream with sample data
    DataStream<DataPoint> inputStream = buildExampleInputStream(env);

    // Apply Univmon aggregation in tumbling windows
    // Univmon is a universal monitoring framework supporting multiple query types from one sketch
    Univmon univmon = createUnivmon();
    AggregationConfig config = new AggregationConfig();

    DataStream<PrecomputedOutput> outputStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .aggregate(univmon, new KeyedWindowProcessor(config, "insertion", "univmon", false));

    // Demonstrate all three query types on the same sketch
    runAllQueries(outputStream);

    env.execute("Univmon Multi-Query Example");
  }

  /** Query all three statistics (frequency, cardinality, top-K) from the same Univmon sketch */
  private static void runAllQueries(DataStream<PrecomputedOutput> outputStream) {
    ObjectMapper objectMapper = new ObjectMapper();

    outputStream
        .map(
            result -> {
              StringBuilder output = new StringBuilder("\n=== Univmon Multi-Query Results ===\n\n");

              // 1. FREQUENCY QUERIES - How often do specific items occur?
              output.append("1. Frequency Estimation:\n");
              String[] freqKeys = {"apple", "banana", "cherry"};
              for (String key : freqKeys) {
                ObjectNode freqParams = objectMapper.createObjectNode();
                freqParams.put("statistic", "freq");
                freqParams.put("key", key);

                java.lang.reflect.Method queryMethod =
                    result.precompute.getClass().getMethod("query", JsonNode.class);
                JsonNode freqResult = (JsonNode) queryMethod.invoke(result.precompute, freqParams);
                output.append(String.format("   %s: %s\n", key, freqResult));
              }

              // 2. CARDINALITY QUERY - How many distinct items are there?
              output.append("\n2. Cardinality Estimation:\n");
              ObjectNode cardParams = objectMapper.createObjectNode();
              cardParams.put("statistic", "cardinality");

              java.lang.reflect.Method queryMethod =
                  result.precompute.getClass().getMethod("query", JsonNode.class);
              JsonNode cardResult = (JsonNode) queryMethod.invoke(result.precompute, cardParams);
              double distinctCount = cardResult.get("distinct_count").asDouble();
              output.append(String.format("   Distinct items: %.2f\n", distinctCount));

              // 3. TOP-K QUERY - What are the most frequent items?
              output.append("\n3. Top-K Heavy Hitters:\n");
              ObjectNode topkParams = objectMapper.createObjectNode();
              topkParams.put("statistic", "topk");

              JsonNode topkResult = (JsonNode) queryMethod.invoke(result.precompute, topkParams);
              output.append(String.format("   %s\n", topkResult));

              return output.toString();
            })
        .print();
  }

  private static Univmon createUnivmon() {
    Map<String, String> sketchParams = new HashMap<>();
    sketchParams.put("levels", "16"); // Number of levels in the hierarchical structure
    sketchParams.put("rows", "3"); // Number of hash functions
    sketchParams.put("cols", "2048"); // Width of each counter array
    sketchParams.put("k", "10"); // Track top-10 items
    return new Univmon("count", sketchParams);
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
