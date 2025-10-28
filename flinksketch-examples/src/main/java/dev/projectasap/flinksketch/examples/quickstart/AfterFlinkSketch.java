/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.examples.quickstart;

import dev.projectasap.flinksketch.datamodel.DataPoint;
import java.time.Duration;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/** Example showing memory efficiency of FlinkSketch-based approximate computation. */
public class AfterFlinkSketch {

  private static DataStream<DataPoint> createUnionedStreams(
      StreamExecutionEnvironment env, int numSources) {
    // Watermark strategy
    var watermarkStrategy =
        WatermarkStrategy.<DataPoint>forBoundedOutOfOrderness(Duration.ofSeconds(10))
            .withTimestampAssigner((event, ts) -> event.timestamp);

    DataStream<DataPoint> combinedStream = null;

    // Create sources and streams in a loop
    for (int i = 0; i < numSources; i++) {
      final String constantKey = "key";

      var source =
          new DataGeneratorSource<>(
              index -> new DataPoint(System.currentTimeMillis(), constantKey, index.intValue()),
              Long.MAX_VALUE,
              TypeInformation.of(DataPoint.class));

      var stream = env.fromSource(source, watermarkStrategy, "DataGen Source " + (i + 1));

      combinedStream = (combinedStream == null) ? stream : combinedStream.union(stream);
    }

    return combinedStream;
  }

  public static void main(String[] args) throws Exception {
    // Initialize Flink execution environment
    var env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Create data stream from source
    var combinedStream = createUnionedStreams(env, 1);

    // Apply DDSketch for quantile estimation - uses bounded memory
    // The sketch maintains accuracy while using constant space
    combinedStream
        .keyBy(item -> item.key)
        .window(TumblingEventTimeWindows.of(Time.seconds(300)))
        .aggregate(
            new dev.projectasap.flinksketch.sketches.ddsketch.DDSketchQuantile(
                null, Map.of("relativeAccuracy", "0.01", "maxNumBins", "2048")));

    env.execute("After FlinkSketch Demo");
  }
}
