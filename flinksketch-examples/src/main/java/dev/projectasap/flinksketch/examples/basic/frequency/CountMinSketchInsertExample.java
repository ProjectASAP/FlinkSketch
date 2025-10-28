/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.examples.basic.frequency;

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

/** Basic example demonstrating CountMinSketch for frequency estimation queries. */
public class CountMinSketchInsertExample {
  public static void main(String[] args) throws Exception {
    // Initialize Flink execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Create input stream with sample data
    DataStream<DataPoint> inputStream = buildExampleInputStream(env);
    AggregationConfig config = new AggregationConfig();

    // Apply CountMinSketch aggregation in tumbling windows
    // The sketch provides approximate frequency counts with bounded memory
    DataStream<PrecomputedOutput> outputStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            // BEFORE FLINKSKETCH
            // .aggregate(
            //     createFreqCount(), new KeyedWindowProcessor(config, "insertion", "exact",
            // false));
            // AFTER FLINKSKETCH
            .aggregate(
                createCountMinSketch(),
                new KeyedWindowProcessor(config, "insertion", "countminsketch", false));

    // Query for specific keys and print results
    runQueries(outputStream);

    env.execute("CountMinSketch Insert Example");
  }

  /** Query frequencies for predefined keys using reflection */
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

  private static CountMinSketch createCountMinSketch() {
    Map<String, String> sketchParams = new HashMap<>();
    sketchParams.put("rows", "3");
    sketchParams.put("cols", "2048");
    return new CountMinSketch("count", sketchParams);
  }

  private static ExactFrequency createFreqCount() {
    Map<String, String> baselineParams = new HashMap<>();
    return new ExactFrequency("count", baselineParams);
  }

  private static DataStream<DataPoint> buildExampleInputStream(StreamExecutionEnvironment env) {
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
