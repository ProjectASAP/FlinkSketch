/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.examples.evaluation.memory;

import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.PrecomputedOutput;
import dev.projectasap.flinksketch.examples.utils.MemoryComparator;
import dev.projectasap.flinksketch.examples.utils.MemoryComparator.MemoryComparisonResult;
import dev.projectasap.flinksketch.sketches.baseline.ExactTopK;
import dev.projectasap.flinksketch.sketches.custom.Univmon;
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

/** Memory comparison example demonstrating the space efficiency of Univmon vs exact TopK. */
public class UnivmonMemoryExample {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    long rowsPerSecond = 5000;
    long durationSeconds = 10;
    int keyCardinality = 100000; // many unique keys

    DataStream<DataPoint> inputStream =
        createDataGenStream(env, rowsPerSecond, durationSeconds, keyCardinality);

    Univmon univmon = createUnivmon();
    ExactTopK topK = createTopK();

    AggregationConfig baselineConfig = new AggregationConfig();
    AggregationConfig sketchConfig = new AggregationConfig();

    DataStream<PrecomputedOutput> baselineStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .aggregate(topK, new KeyedWindowProcessor(baselineConfig, "insertion", "exact", false));

    DataStream<PrecomputedOutput> sketchStream =
        inputStream
            .keyBy(item -> 0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .aggregate(
                univmon, new KeyedWindowProcessor(sketchConfig, "insertion", "count", false));

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
    env.execute("Univmon Memory Comparison Example");
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
          String key = "item" + ((index % keyCardinality) + 1);
          int value = 1;
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

  private static Univmon createUnivmon() {
    Map<String, String> params = new HashMap<>();
    params.put("levels", "5");
    params.put("rows", "3");
    params.put("cols", "2048");
    params.put("k", "10");
    return new Univmon("count", params);
  }

  private static ExactTopK createTopK() {
    Map<String, String> params = new HashMap<>();
    params.put("k", "10");
    return new ExactTopK("count", params);
  }
}
