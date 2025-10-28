/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.examples.evaluation.accuracy;

import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.PrecomputedOutput;
import dev.projectasap.flinksketch.examples.utils.QueryComparisonResult;
import dev.projectasap.flinksketch.examples.utils.QueryResultComparator;
import dev.projectasap.flinksketch.sketches.baseline.ExactFrequency;
import dev.projectasap.flinksketch.sketches.custom.CountMinSketch;
import dev.projectasap.flinksketch.utils.AggregationConfig;
import dev.projectasap.flinksketch.windowfunctions.KeyedWindowProcessor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Simple program that runs CountMinSketch vs ExactFrequency (exact) on a stream of frequency data
 */
public class CountMinSketchAccuracyExample {
  public static void main(String[] args) throws Exception {
    // Initialize the Flink streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Build the input data stream with item insertions
    DataStream<DataPoint> inputStream = buildExampleInputStream(env);

    // Instantiate the approximate frequency sketch (CountMinSketch)
    CountMinSketch countMinSketch = createCountMinSketch();

    // Instantiate the exact frequency counter (baseline for comparison)
    ExactFrequency exactFrequency = createExactFrequency();

    // Create configuration objects for each aggregator to track metadata
    AggregationConfig baselineConfig = new AggregationConfig();
    AggregationConfig sketchConfig = new AggregationConfig();

    /**
     * BASELINE PATH: Count exact frequencies 1. keyBy(item -> 0): Route all items to the same
     * partition (constant key) 2. window(): Create 5-second tumbling windows 3. aggregate(): Apply
     * the exact ExactFrequency aggregator function
     */
    DataStream<PrecomputedOutput> baselineStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .aggregate(
                exactFrequency,
                new KeyedWindowProcessor(baselineConfig, "insertion", "exact", false));

    /**
     * SKETCH PATH: Count approximate frequencies using CountMinSketch 1. keyBy(item -> 0): Route
     * all items to the same partition (constant key) 2. window(): Create 5-second tumbling windows
     * (matches baseline window) 3. aggregate(): Apply the CountMinSketch aggregator function
     */
    DataStream<PrecomputedOutput> sketchStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .aggregate(
                countMinSketch,
                new KeyedWindowProcessor(sketchConfig, "insertion", "countminsketch", false));

    /**
     * Re-key both streams for joining: Convert output streams to keyed streams Ensures that
     * baseline and sketch outputs have a common key for the interval join The constant key "key"
     * routes all window results to the same join processor
     */
    KeyedStream<PrecomputedOutput, String> baselineKeyedStream =
        baselineStream.keyBy(item -> "key");

    KeyedStream<PrecomputedOutput, String> sketchKeyedStream = sketchStream.keyBy(item -> "key");

    /**
     * INTERVAL JOIN: Match baseline and sketch results from the same window between(0, 0): Join
     * elements that have timestamps within 0ms of each other This effectively joins results from
     * the same 5-second window process(): Apply custom logic to compare the two frequency counters
     */
    DataStream<List<QueryComparisonResult>> comparisonStream =
        baselineKeyedStream
            .intervalJoin(sketchKeyedStream)
            .between(Time.milliseconds(0), Time.milliseconds(0))
            .process(
                new ProcessJoinFunction<
                    PrecomputedOutput, PrecomputedOutput, List<QueryComparisonResult>>() {
                  @Override
                  public void processElement(
                      PrecomputedOutput baseline,
                      PrecomputedOutput sketch,
                      Context ctx,
                      Collector<List<QueryComparisonResult>> out)
                      throws Exception {
                    /**
                     * Compare baseline (exact) vs sketch (approximate) frequency counts
                     * QueryResultComparator: Utility class that computes statistics about
                     * differences StatisticType.PER_KEY: Compares frequency counts for each unique
                     * key allComparisons(): Returns all comparison metrics (error, accuracy, etc.)
                     */
                    QueryResultComparator comparator =
                        new QueryResultComparator(
                            sketch, baseline, QueryResultComparator.StatisticType.PER_KEY);
                    List<QueryComparisonResult> results = comparator.allComparisons();
                    out.collect(results);
                  }
                });

    // Print all comparison results to stdout
    comparisonStream.print();

    // Execute the Flink job
    env.execute("CountMinSketch Example");
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
}
