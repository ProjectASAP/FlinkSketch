/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.examples.evaluation.memory;

import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.PrecomputedOutput;
import dev.projectasap.flinksketch.examples.utils.MemoryComparator;
import dev.projectasap.flinksketch.examples.utils.MemoryComparator.MemoryComparisonResult;
import dev.projectasap.flinksketch.sketches.baseline.ExactFrequency;
import dev.projectasap.flinksketch.sketches.custom.CountMinSketch;
import dev.projectasap.flinksketch.utils.AggregationConfig;
import dev.projectasap.flinksketch.windowfunctions.KeyedWindowProcessor;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Memory comparison example demonstrating the space efficiency of CountMinSketch vs exact
 * ExactFrequency. Uses DataGen to generate high-cardinality streams and compares memory usage.
 */
public class CountMinSketchMemoryExample {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Configuration for data generation
    long rowsPerSecond = 10000; // High throughput
    long durationSeconds = 10; // 10 seconds of data
    int keyCardinality = 100000; // 100K unique keys - high cardinality to show memory difference

    // Build high-cardinality input stream using DataGen
    DataStream<DataPoint> inputStream =
        createDataGenStream(env, rowsPerSecond, durationSeconds, keyCardinality);

    // Instantiate the approximate frequency sketch (CountMinSketch)
    CountMinSketch countMinSketch = createCountMinSketch();

    // Instantiate the exact frequency counter (baseline for comparison)
    ExactFrequency exactFrequency = createExactFrequency();

    // Create configuration objects for each aggregator to track metadata
    AggregationConfig baselineConfig = new AggregationConfig();
    AggregationConfig sketchConfig = new AggregationConfig();

    /**
     * BASELINE PATH: Count exact frequencies ExactFrequency stores every unique key in a HashMap,
     * so memory grows linearly with cardinality With 100K keys, this will use approximately: 100K *
     * (avg_key_size + 4 bytes) ≈ 1-2 MB
     */
    DataStream<PrecomputedOutput> baselineStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .aggregate(
                exactFrequency,
                new KeyedWindowProcessor(baselineConfig, "insertion", "exact", false));

    /**
     * SKETCH PATH: Count approximate frequencies using CountMinSketch CountMinSketch uses fixed
     * memory regardless of key cardinality With rows=3, cols=2048: 3 * 2048 * 4 bytes = 24 KB
     * (constant!)
     */
    DataStream<PrecomputedOutput> sketchStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .aggregate(
                countMinSketch,
                new KeyedWindowProcessor(sketchConfig, "insertion", "countminsketch", false));

    /** Re-key both streams for joining to compare memory usage side-by-side */
    KeyedStream<PrecomputedOutput, String> baselineKeyedStream =
        baselineStream.keyBy(item -> "key");

    KeyedStream<PrecomputedOutput, String> sketchKeyedStream = sketchStream.keyBy(item -> "key");

    /**
     * INTERVAL JOIN: Match baseline and sketch results from the same window Memory comparison is
     * performed once per window aggregation (not per key) This measures the total memory footprint
     * of each data structure
     */
    DataStream<MemoryComparisonResult> comparisonStream =
        baselineKeyedStream
            .intervalJoin(sketchKeyedStream)
            .between(Time.milliseconds(0), Time.milliseconds(0))
            .process(
                new ProcessJoinFunction<
                    PrecomputedOutput, PrecomputedOutput, MemoryComparisonResult>() {
                  private int windowCounter = 0;

                  @Override
                  public void processElement(
                      PrecomputedOutput baseline,
                      PrecomputedOutput sketch,
                      Context ctx,
                      Collector<MemoryComparisonResult> out)
                      throws Exception {
                    /**
                     * Use MemoryComparator to compare memory usage Memory is compared once per
                     * aggregation window, regardless of key cardinality MemoryComparator uses
                     * reflection to invoke get_memory() on both accumulators Increment window
                     * counter for each tumbling window
                     */
                    windowCounter++;
                    MemoryComparator comparator = new MemoryComparator(sketch, baseline);
                    MemoryComparisonResult result = comparator.compareMemory(windowCounter);
                    out.collect(result);
                  }
                });

    // Print memory comparison results
    comparisonStream.print();

    // Execute the Flink job
    env.execute("CountMinSketch Memory Comparison Example");
  }

  /**
   * Creates a DataGen source that generates high-cardinality item insertions. This simulates a
   * realistic high-throughput scenario with many unique keys.
   */
  private static DataStream<DataPoint> createDataGenStream(
      StreamExecutionEnvironment env,
      long rowsPerSecond,
      long durationSeconds,
      int keyCardinality) {
    long numberOfRecords = rowsPerSecond * durationSeconds;
    long baseTimestamp = 1000000000000L;
    Random random = new Random(42L);

    GeneratorFunction<Long, DataPoint> generatorFunction =
        index -> {
          long timestamp = baseTimestamp + (index * 1000 / rowsPerSecond); // Space out timestamps
          String key = "item" + ((index % keyCardinality) + 1); // Cycle through keys
          int value = 1; // Each occurrence counts as 1
          return new DataPoint(timestamp, key, value);
        };

    DataGeneratorSource<DataPoint> dataGenSource =
        new DataGeneratorSource<>(
            generatorFunction, numberOfRecords, TypeInformation.of(DataPoint.class));

    return env.fromSource(
        dataGenSource,
        WatermarkStrategy.<DataPoint>forMonotonousTimestamps()
            .withTimestampAssigner((event, timestamp) -> event.timestamp),
        "High-Cardinality Generator");
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
}
