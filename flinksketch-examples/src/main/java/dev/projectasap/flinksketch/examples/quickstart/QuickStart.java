/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.examples.quickstart;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.PrecomputedOutput;
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

/** Quick start example demonstrating CountMinSketch for frequency estimation with querying. */
public class QuickStart {
  public static void main(String[] args) throws Exception {
    // Set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Create a data stream with sample data
    DataStream<DataPoint> dataStream =
        env.fromElements(
            new DataPoint(1L, "apple", 1),
            new DataPoint(2L, "banana", 1),
            new DataPoint(3L, "apple", 1),
            new DataPoint(4L, "orange", 1),
            new DataPoint(5L, "banana", 1),
            new DataPoint(6L, "apple", 1),
            new DataPoint(7L, "banana", 1),
            new DataPoint(8L, "apple", 1));

    // Assign timestamps for windowing
    dataStream =
        dataStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.<DataPoint>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.timestamp));

    // Configure CountMinSketch parameters
    Map<String, String> sketchParams = new HashMap<>();
    sketchParams.put("rows", "3"); // Number of hash functions
    sketchParams.put("cols", "2048"); // Number of counters per hash function

    // Apply CountMinSketch aggregation with bounded memory
    AggregationConfig config = new AggregationConfig();
    DataStream<PrecomputedOutput> outputStream =
        dataStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .aggregate(
                new CountMinSketch("count", sketchParams),
                new KeyedWindowProcessor(config, "insertion", "countminsketch", false));

    // Query for specific keys and print their frequencies
    List<String> queryKeys = Arrays.asList("apple", "banana", "orange");
    ObjectMapper objectMapper = new ObjectMapper();

    outputStream
        .map(
            result -> {
              StringBuilder sb = new StringBuilder("Frequencies: ");
              for (String key : queryKeys) {
                ObjectNode queryParams = objectMapper.createObjectNode();
                queryParams.put("key", key);

                java.lang.reflect.Method queryMethod =
                    result.precompute.getClass().getMethod("query", JsonNode.class);
                JsonNode frequency = (JsonNode) queryMethod.invoke(result.precompute, queryParams);
                sb.append(String.format("%s=%s ", key, frequency));
              }
              return sb.toString();
            })
        .print();

    // Execute the job
    env.execute("Quick Start - Frequency Estimation with CountMinSketch");
  }
}
