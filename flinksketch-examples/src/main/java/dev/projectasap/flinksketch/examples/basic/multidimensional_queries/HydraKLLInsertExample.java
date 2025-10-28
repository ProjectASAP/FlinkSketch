/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.examples.basic.multidimensional_queries;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.PrecomputedOutput;
import dev.projectasap.flinksketch.sketches.baseline.PerKeyQuantiles;
import dev.projectasap.flinksketch.sketches.custom.HydraKLL;
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

/** Basic example demonstrating HydraKLL for multidimensional per-key quantile estimation. */
public class HydraKLLInsertExample {
  public static void main(String[] args) throws Exception {
    // Initialize Flink execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Create input stream with multi-key latency data (simulating multiple services)
    DataStream<DataPoint> inputStream = buildExampleInputStream(env);
    AggregationConfig config = new AggregationConfig();

    // Apply HydraKLL aggregation in tumbling windows
    // HydraKLL efficiently tracks quantiles across multiple keys simultaneously
    DataStream<PrecomputedOutput> outputStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            // BEFORE FLINKSKETCH - Exact per-key quantiles
            // .aggregate(
            //     createPerKeyQuantiles(),
            //     new KeyedWindowProcessor(config, "latency", "exact", false));
            // AFTER FLINKSKETCH - Approximate per-key quantiles with HydraKLL
            .aggregate(
                createHydraKLL(), new KeyedWindowProcessor(config, "latency", "hydrakll", false));

    // Query for specific keys and ranks
    runQueries(outputStream);

    env.execute("HydraKLL Insert Example");
  }

  /** Query quantiles for specific keys at predefined ranks using reflection */
  private static void runQueries(DataStream<PrecomputedOutput> outputStream) {
    // Define which keys to query
    List<String> queryKeys = Arrays.asList("service-a", "service-b", "service-c");
    // Define which quantile ranks to query (0.0 to 1.0)
    List<Double> quantileRanks = Arrays.asList(0.5, 0.9, 0.95, 0.99);
    ObjectMapper objectMapper = new ObjectMapper();

    outputStream
        .map(
            result -> {
              StringBuilder sb = new StringBuilder("\n=== Per-Key Quantile Results ===\n");

              for (String key : queryKeys) {
                sb.append(String.format("\nKey: %s\n", key));

                for (Double rank : quantileRanks) {
                  ObjectNode queryParams = objectMapper.createObjectNode();
                  queryParams.put("key", key);
                  queryParams.put("rank", rank);

                  try {
                    java.lang.reflect.Method queryMethod =
                        result.precompute.getClass().getMethod("query", JsonNode.class);
                    JsonNode value = (JsonNode) queryMethod.invoke(result.precompute, queryParams);

                    if (value.has(key)) {
                      sb.append(
                          String.format("  p%.0f = %.2f\n", rank * 100, value.get(key).asDouble()));
                    }
                  } catch (Exception e) {
                    sb.append(String.format("  p%.0f = Error: %s\n", rank * 100, e.getMessage()));
                  }
                }
              }

              return sb.toString();
            })
        .print();
  }

  private static HydraKLL createHydraKLL() {
    Map<String, String> sketchParams = new HashMap<>();
    sketchParams.put("rows", "3"); // Number of hash functions
    sketchParams.put("cols", "1024"); // Number of columns in sketch matrix
    sketchParams.put("k", "200"); // Size of each KLL sketch
    return new HydraKLL("hydrakll", sketchParams);
  }

  private static PerKeyQuantiles createPerKeyQuantiles() {
    Map<String, String> baselineParams = new HashMap<>();
    return new PerKeyQuantiles("exact", baselineParams);
  }

  /**
   * Creates a sample input stream with multiple keys and multiple values per key. Simulates latency
   * measurements from different services.
   */
  private static DataStream<DataPoint> buildExampleInputStream(StreamExecutionEnvironment env) {
    DataStream<DataPoint> inputStream =
        env.fromElements(
            // Service A - low latency values (10-30ms)
            new DataPoint(1L, "service-a", 15),
            new DataPoint(2L, "service-a", 18),
            new DataPoint(3L, "service-a", 12),
            new DataPoint(4L, "service-a", 22),
            new DataPoint(5L, "service-a", 17),
            new DataPoint(6L, "service-a", 25),
            new DataPoint(7L, "service-a", 13),
            new DataPoint(8L, "service-a", 20),
            new DataPoint(9L, "service-a", 16),
            new DataPoint(10L, "service-a", 28),

            // Service B - medium latency values (50-100ms)
            new DataPoint(11L, "service-b", 65),
            new DataPoint(12L, "service-b", 72),
            new DataPoint(13L, "service-b", 58),
            new DataPoint(14L, "service-b", 88),
            new DataPoint(15L, "service-b", 75),
            new DataPoint(16L, "service-b", 92),
            new DataPoint(17L, "service-b", 61),
            new DataPoint(18L, "service-b", 78),
            new DataPoint(19L, "service-b", 69),
            new DataPoint(20L, "service-b", 95),

            // Service C - high latency values with outliers (100-500ms)
            new DataPoint(21L, "service-c", 120),
            new DataPoint(22L, "service-c", 145),
            new DataPoint(23L, "service-c", 108),
            new DataPoint(24L, "service-c", 350),
            new DataPoint(25L, "service-c", 135),
            new DataPoint(26L, "service-c", 165),
            new DataPoint(27L, "service-c", 115),
            new DataPoint(28L, "service-c", 180),
            new DataPoint(29L, "service-c", 125),
            new DataPoint(30L, "service-c", 450));

    inputStream =
        inputStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.<DataPoint>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.timestamp));

    return inputStream;
  }
}
