/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.examples.basic.quantile;

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

/** Basic example demonstrating DDSketch for quantile estimation queries. */
public class DDSketchInsertExample {
  public static void main(String[] args) throws Exception {
    // Initialize Flink execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Create input stream with sample latency data
    DataStream<DataPoint> inputStream = buildExampleInputStream(env);
    AggregationConfig config = new AggregationConfig();

    // Apply DDSketch aggregation in tumbling windows
    // DDSketch provides quantile estimates with relative-error guarantees
    DataStream<PrecomputedOutput> outputStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            // BEFORE FLINKSKETCH
            // .aggregate(
            //     createQuantiles(), new KeyedWindowProcessor(config, "latency", "exact", false));
            // AFTER FLINKSKETCH
            .aggregate(
                createDDSketch(), new KeyedWindowProcessor(config, "latency", "ddsketch", false));

    // Query for quantile ranks and print results
    runQueries(outputStream);

    env.execute("DDSketch Insert Example");
  }

  /** Query quantiles at predefined ranks using reflection */
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

  private static DDSketchQuantile createDDSketch() {
    Map<String, String> sketchParams = new HashMap<>();
    sketchParams.put("relativeAccuracy", "0.01");
    sketchParams.put("maxNumBins", "2048");
    return new DDSketchQuantile("ddsketch", sketchParams);
  }

  private static ExactQuantiles createQuantiles() {
    Map<String, String> baselineParams = new HashMap<>();
    return new ExactQuantiles("exact", baselineParams);
  }

  private static DataStream<DataPoint> buildExampleInputStream(StreamExecutionEnvironment env) {
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
}
