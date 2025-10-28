/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.examples.evaluation.memory;

import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.PrecomputedOutput;
import dev.projectasap.flinksketch.examples.utils.MemoryComparator;
import dev.projectasap.flinksketch.examples.utils.MemoryComparator.MemoryComparisonResult;
import dev.projectasap.flinksketch.sketches.baseline.ExactQuantiles;
import dev.projectasap.flinksketch.sketches.ddsketch.DDSketchQuantile;
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

/** Memory comparison example demonstrating the space efficiency of DDSketch vs exact quantiles. */
public class DDSketchMemoryExample {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    long rowsPerSecond = 5000;
    long durationSeconds = 10;
    int keyCardinality = 50000; // moderate cardinality for latency values

    DataStream<DataPoint> inputStream =
        createDataGenStream(env, rowsPerSecond, durationSeconds, keyCardinality);

    DDSketchQuantile ddSketch = createDDSketchQuantile();
    ExactQuantiles exactQuantiles = createExactQuantiles();

    AggregationConfig baselineConfig = new AggregationConfig();
    AggregationConfig sketchConfig = new AggregationConfig();

    DataStream<PrecomputedOutput> baselineStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .aggregate(
                exactQuantiles,
                new KeyedWindowProcessor(baselineConfig, "latency", "exact", false));

    DataStream<PrecomputedOutput> sketchStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .aggregate(
                ddSketch, new KeyedWindowProcessor(sketchConfig, "latency", "ddsketch", false));

    KeyedStream<PrecomputedOutput, String> baselineKeyedStream =
        baselineStream.keyBy(item -> "key");
    KeyedStream<PrecomputedOutput, String> sketchKeyedStream = sketchStream.keyBy(item -> "key");

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
                    windowCounter++;
                    MemoryComparator comparator = new MemoryComparator(sketch, baseline);
                    MemoryComparisonResult result = comparator.compareMemory(windowCounter);
                    out.collect(result);
                  }
                });

    comparisonStream.print();
    env.execute("DDSketch Memory Comparison Example");
  }

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
          long timestamp = baseTimestamp + (index * 1000 / rowsPerSecond);
          // Generate latency-like values spread across keys
          String key = "flow" + ((index % keyCardinality) + 1);
          int value = random.nextInt(1000); // latency/value
          return new DataPoint(timestamp, key, value);
        };

    DataGeneratorSource<DataPoint> dataGenSource =
        new DataGeneratorSource<>(
            generatorFunction, numberOfRecords, TypeInformation.of(DataPoint.class));

    return env.fromSource(
        dataGenSource,
        WatermarkStrategy.<DataPoint>forMonotonousTimestamps()
            .withTimestampAssigner((event, timestamp) -> event.timestamp),
        "Latency Generator");
  }

  private static DDSketchQuantile createDDSketchQuantile() {
    Map<String, String> sketchParams = new HashMap<>();
    // Match integration example: set relative accuracy and max number of bins
    sketchParams.put("relativeAccuracy", "0.01");
    sketchParams.put("maxNumBins", "2048");
    return new DDSketchQuantile("ddsketch", sketchParams);
  }

  private static ExactQuantiles createExactQuantiles() {
    Map<String, String> baselineParams = new HashMap<>();
    return new ExactQuantiles("quantile", baselineParams);
  }
}
