/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.examples.evaluation.accuracy;

import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.PrecomputedOutput;
import dev.projectasap.flinksketch.examples.utils.QueryComparisonResult;
import dev.projectasap.flinksketch.examples.utils.QueryResultComparator;
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
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Simple program that runs DDSketchQuantile vs ExactQuantiles (exact) on a stream of latency data
 */
public class DDSketchAccuracyExample {
  public static void main(String[] args) throws Exception {
    // Initialize the Flink streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Build the input data stream with latency measurements
    DataStream<DataPoint> inputStream = buildExampleInputStream(env);

    // Instantiate the approximate quantile sketch (DDSketch)
    DDSketchQuantile ddSketchQuantile = createDDSketchQuantile();

    // Instantiate the exact quantile aggregator (baseline for comparison)
    ExactQuantiles quantiles = createQuantiles();

    // Create configuration objects for each aggregator to track metadata
    AggregationConfig baselineConfig = new AggregationConfig();
    AggregationConfig sketchConfig = new AggregationConfig();

    /**
     * BASELINE PATH: Compute exact quantiles 1. keyBy(item -> 0): Route all items to the same
     * partition (constant key) 2. window(): Create 5-second tumbling windows 3. aggregate(): Apply
     * the exact ExactQuantiles aggregator function
     */
    DataStream<PrecomputedOutput> baselineStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .aggregate(
                quantiles, new KeyedWindowProcessor(baselineConfig, "latency", "exact", false));

    /**
     * SKETCH PATH: Compute approximate quantiles using DDSketch 1. keyBy(item -> 0): Route all
     * items to the same partition (constant key) 2. window(): Create 5-second tumbling windows
     * (matches baseline window) 3. aggregate(): Apply the DDSketchQuantile aggregator function
     */
    DataStream<PrecomputedOutput> sketchStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .aggregate(
                ddSketchQuantile,
                new KeyedWindowProcessor(sketchConfig, "latency", "ddsketch", false));

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
     * the same 5-second window process(): Apply custom logic to compare the two quantile
     * computations
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
                     * Compare baseline (exact) vs sketch (approximate) quantile results
                     * QueryResultComparator: Utility class that computes statistics about
                     * differences StatisticType.KEY_AGNOSTIC: Compares overall quantile estimates
                     * (not per-key) quantileRanks: Predefined list of quantile ranks to query (p50,
                     * p90, p95, p99) allComparisons(): Returns all comparison metrics (error,
                     * accuracy, etc.)
                     */
                    List<Double> quantileRanks = Arrays.asList(0.5, 0.9, 0.95, 0.99);
                    QueryResultComparator comparator =
                        new QueryResultComparator(
                            sketch,
                            baseline,
                            QueryResultComparator.StatisticType.KEY_AGNOSTIC,
                            quantileRanks);
                    List<QueryComparisonResult> results = comparator.allComparisons();
                    out.collect(results);
                  }
                });

    // Print all comparison results to stdout
    comparisonStream.print();

    // Execute the Flink job
    env.execute("DDSketchQuantile Example");
  }

  private static DDSketchQuantile createDDSketchQuantile() {
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
    // Simulate latency measurements: key is the latency value (as string), value is 1
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
