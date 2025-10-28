/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch;

import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.PrecomputedOutput;
import dev.projectasap.flinksketch.datamodel.Summary;
import dev.projectasap.flinksketch.sinks.SinkBuilder;
import dev.projectasap.flinksketch.utils.AggregationConfig;
import dev.projectasap.flinksketch.utils.ConfigLoader;
import dev.projectasap.flinksketch.utils.StreamingConfig;
import dev.projectasap.flinksketch.windowfunctions.KeyedWindowProcessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main Flink job for streaming data aggregation with sketches. Supports various aggregation
 * functions and sketch algorithms with configurable pipelines.
 */
public class DataStreamJob {
  private static final Logger LOG = LoggerFactory.getLogger(DataStreamJob.class);

  private static Namespace parseArgs(String[] args) {
    ArgumentParser parser =
        ArgumentParsers.newFor("DataStreamJob")
            .build()
            .defaultHelp(true)
            .description("Custom Aggregate Example");

    parser.addArgument("--outputFilePath").type(String.class).help("Output file path");

    parser.addArgument("--configFilePath").type(String.class).help("Configuration file path");

    parser
        .addArgument("--outputFormat")
        .type(String.class)
        .choices("byte", "json")
        .help("Output format: byte or json");

    parser
        .addArgument("--compressJson")
        .type(Boolean.class)
        .setDefault(false)
        .help("Compress JSON output");

    parser
        .addArgument("--logLevel")
        .type(String.class)
        .choices("TRACE", "DEBUG", "INFO", "WARN", "ERROR")
        .setDefault("INFO")
        .help("Sets the logging level (default: INFO)");

    parser
        .addArgument("--datagenKeyCardinality")
        .type(Integer.class)
        .required(true)
        .help("DataGen key cardinality");

    parser
        .addArgument("--datagenItemsPerWindow")
        .type(Long.class)
        .required(true)
        .help("Number of items to fit in each tumbling window");

    parser
        .addArgument("--verbose")
        .type(Boolean.class)
        .setDefault(false)
        .help("Enable verbose output to sink (default: false)");

    parser
        .addArgument("--pipeline")
        .type(String.class)
        .choices("insertion", "insertion_querying")
        .setDefault("insertion")
        .help("Pipeline type: insertion (sketch only) or insertion_querying (sketch + queries)");

    parser
        .addArgument("--outputMode")
        .type(String.class)
        .setDefault("sketch")
        .help(
            "Output mode: sketch, query, memory, or combinations separated by underscore"
                + " (e.g., query_memory, sketch_query_memory). Note: query only available for"
                + " insertion_querying pipeline");

    parser.addArgument("--queryKeys").type(String.class).help("Comma-separated list of query keys");

    parser
        .addArgument("--queryRanks")
        .type(String.class)
        .help("Comma-separated list of query ranks (percentiles 0.0-1.0)");

    parser
        .addArgument("--queryName")
        .type(String.class)
        .help(
            "Query name (also used as statistic for Univmon): freq, quantile, perKeyQuantile, cardinality, topk, entropy");

    parser
        .addArgument("--parallelism")
        .type(Integer.class)
        .setDefault(1)
        .help("Parallelism for the Flink job (default: 1)");

    parser
        .addArgument("--enableMerging")
        .type(Boolean.class)
        .setDefault(true)
        .help("Enable merging of parallel window results (default: true)");

    parser
        .addArgument("--distribution")
        .type(String.class)
        .choices("uniform", "normal")
        .setDefault("uniform")
        .help("Distribution type for generated values: uniform or normal (default: uniform)");

    Namespace parsedArgs = parser.parseArgsOrFail(args);
    return parsedArgs;
  }

  private static void checkArgs(Namespace parsedArgs) {
    boolean verbose = parsedArgs.getBoolean("verbose");
    if (verbose && parsedArgs.getString("outputFilePath") == null) {
      throw new IllegalArgumentException(
          "Output file path is required when verbose mode is enabled");
    }
    if (parsedArgs.getString("configFilePath") == null) {
      throw new IllegalArgumentException("Configuration file path is required");
    }
    if (verbose && parsedArgs.getString("outputFormat") == null) {
      throw new IllegalArgumentException("Output format is required when verbose mode is enabled");
    }
  }

  /**
   * Helper method to generate a DataPoint with timestamp, key, and value.
   *
   * @param index the index of the data point
   * @param itemsPerWindow number of items per tumbling window
   * @param baseTimestamp base timestamp in milliseconds
   * @param millisecondTumblingWindow tumbling window size in milliseconds
   * @param infiniteCardinality whether to use infinite cardinality for keys
   * @param keyCardinality the cardinality of keys (used when infiniteCardinality is false)
   * @param distribution the distribution type ("normal" or "uniform")
   * @param randomSource the Random source for value generation
   * @return a new DataPoint
   */
  private static DataPoint generateDataPoint(
      long index,
      long itemsPerWindow,
      long baseTimestamp,
      long millisecondTumblingWindow,
      boolean infiniteCardinality,
      int keyCardinality,
      String distribution,
      Random randomSource) {
    // Calculate timestamp so that N items fit into each tumbling window
    long timestamp = baseTimestamp + (index / itemsPerWindow) * millisecondTumblingWindow;
    String key = infiniteCardinality ? "key" + index : "key" + ((index % keyCardinality) + 1);

    // Generate value based on distribution type
    int value;
    if ("normal".equals(distribution)) {
      // Normal distribution: mean=500,000, stddev=166,667 (to approximate 1-1,000,000 range)
      double normalValue = randomSource.nextGaussian() * 166667 + 500000;
      // Clamp to 1-1,000,000 range
      value = (int) Math.max(1, Math.min(1000000, normalValue));
    } else {
      // Uniform distribution (default): 1-1,000,000
      value = randomSource.nextInt(1000000) + 1;
    }

    return new DataPoint(timestamp, key, value);
  }

  private static DataStream<DataPoint> createDataGenStream(
      StreamExecutionEnvironment env,
      int keyCardinality,
      int numSources,
      long tumblingWindowSizeSeconds,
      long itemsPerWindow,
      String distribution) {
    long recordsPerSource = Long.MAX_VALUE;
    boolean infiniteCardinality = keyCardinality == -1;

    // Items per window passed from config (e.g., 1e6 or 1e7)
    long baseTimestamp = 1000000000000L; // Base timestamp in milliseconds
    long millisecondTumblingWindow = tumblingWindowSizeSeconds * 1000; // Convert to milliseconds

    // Create first source
    Random random0 = new Random(40L);
    GeneratorFunction<Long, DataPoint> baseGeneratorFunction =
        index ->
            generateDataPoint(
                index,
                itemsPerWindow,
                baseTimestamp,
                millisecondTumblingWindow,
                infiniteCardinality,
                keyCardinality,
                distribution,
                random0);

    DataGeneratorSource<DataPoint> baseDataGenSource =
        new DataGeneratorSource<>(
            baseGeneratorFunction, recordsPerSource, TypeInformation.of(DataPoint.class));

    DataStream<DataPoint> inputStream =
        env.fromSource(baseDataGenSource, WatermarkStrategy.noWatermarks(), "Generator Source 0");

    // Create and union additional sources
    for (int i = 1; i < numSources; i++) {
      final int sourceId = i;
      Random randomSource = new Random(40L + i);
      GeneratorFunction<Long, DataPoint> generatorFunction =
          index ->
              generateDataPoint(
                  index,
                  itemsPerWindow,
                  baseTimestamp,
                  millisecondTumblingWindow,
                  infiniteCardinality,
                  keyCardinality,
                  distribution,
                  randomSource);

      DataGeneratorSource<DataPoint> dataGenSource =
          new DataGeneratorSource<>(
              generatorFunction, recordsPerSource, TypeInformation.of(DataPoint.class));

      DataStream<DataPoint> additionalStream =
          env.fromSource(dataGenSource, WatermarkStrategy.noWatermarks(), "Generator Source " + i);

      inputStream = inputStream.union(additionalStream);
    }

    return inputStream;
  }

  /**
   * Main entry point for the Flink streaming job.
   *
   * @param args command-line arguments for configuration
   * @throws Exception if the job fails to execute
   */
  public static void main(String[] args) throws Exception {
    Namespace parsedArgs = parseArgs(args);

    // Set log level based on command line argument
    if (parsedArgs.getString("logLevel") != null) {
      System.setProperty("log.level", parsedArgs.getString("logLevel"));
    }

    LOG.info("Starting with log level: {}", System.getProperty("log.level", "INFO"));

    checkArgs(parsedArgs);

    StreamingConfig streamingConfig =
        ConfigLoader.loadConfig(parsedArgs.getString("configFilePath"));

    // Parse query keys from command line and set them in all aggregation configs
    String queryKeysStr = parsedArgs.getString("queryKeys");
    List<String> queryKeys = new ArrayList<>();
    if (queryKeysStr != null && !queryKeysStr.trim().isEmpty()) {
      queryKeys = Arrays.asList(queryKeysStr.split(","));
      queryKeys = queryKeys.stream().map(String::trim).collect(Collectors.toList());
    }

    // Parse query ranks from command line
    String queryRanksStr = parsedArgs.getString("queryRanks");
    List<String> queryRanks = new ArrayList<>();
    if (queryRanksStr != null && !queryRanksStr.trim().isEmpty()) {
      queryRanks = Arrays.asList(queryRanksStr.split(","));
      queryRanks = queryRanks.stream().map(String::trim).collect(Collectors.toList());
    }

    // Parse query name from command line (will be used as statistic for Univmon)
    String queryName = parsedArgs.getString("queryName");

    // Set query keys, ranks, and statistic in all aggregation configs
    for (AggregationConfig config : streamingConfig.aggregationConfigs) {
      config.keys = queryKeys;
      config.ranks = queryRanks;
      // Set statistic field to query name (for Univmon: entropy, cardinality, topk)
      if (queryName != null && !queryName.trim().isEmpty()) {
        config.statistic = queryName.trim();
      }
    }

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // Set parallelism from command line argument
    int parallelism = parsedArgs.getInt("parallelism");
    env.setParallelism(parallelism);

    Sink<PrecomputedOutput> sink = null;
    boolean verbose = parsedArgs.getBoolean("verbose");
    String pipeline = parsedArgs.getString("pipeline");
    String outputMode = parsedArgs.getString("outputMode");
    boolean enableMerging = parsedArgs.getBoolean("enableMerging");

    if (verbose) {
      sink = SinkBuilder.buildSink(parsedArgs);
    }

    // Get tumbling window size from first aggregation config
    long tumblingWindowSizeSeconds = 600; // Default 10 minutes
    if (streamingConfig.aggregationConfigs != null
        && !streamingConfig.aggregationConfigs.isEmpty()) {
      tumblingWindowSizeSeconds = streamingConfig.aggregationConfigs.get(0).tumblingWindowSize;
    }

    // DataGen using multiple parallel sources
    int keyCardinality = parsedArgs.getInt("datagenKeyCardinality");
    long itemsPerWindow = parsedArgs.getLong("datagenItemsPerWindow");
    String distribution = parsedArgs.getString("distribution");

    DataStream<DataPoint> inputStream =
        createDataGenStream(
            env,
            keyCardinality,
            parallelism,
            tumblingWindowSizeSeconds,
            itemsPerWindow,
            distribution);

    inputStream =
        inputStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.<DataPoint>forMonotonousTimestamps()
                // .withTimestampAssigner((event, timestamp) -> event.timestamp * 1000L)
                .withTimestampAssigner((event, timestamp) -> event.timestamp));

    for (AggregationConfig config : streamingConfig.aggregationConfigs) {
      // Use keyBy with a simple round-robin partitioning based on timestamp
      // This ensures deterministic and even distribution
      // Use Math.floorMod to handle negative timestamps correctly
      KeyedStream<DataPoint, Integer> keyedStream =
          inputStream.keyBy(item -> Math.floorMod(item.timestamp, parallelism));

      // Support infinite window with tumblingWindowSize = -1 (use very large window)
      long windowSizeSeconds =
          config.tumblingWindowSize == -1 ? Long.MAX_VALUE : config.tumblingWindowSize;

      WindowedStream<DataPoint, Integer, TimeWindow> windowedStream =
          keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(windowSizeSeconds)));

      DataStream<PrecomputedOutput> parallelOutputStream =
          windowedStream.aggregate(
              (AggregateFunction<DataPoint, ?, Summary>) config.getAggregationFunction(),
              new KeyedWindowProcessor(config, pipeline, outputMode, verbose));

      // Conditionally merge parallel window results based on enableMerging flag and parallelism
      // When parallelism = 1, merging is unnecessary since there's only one partition
      DataStream<PrecomputedOutput> outputStream;
      if (enableMerging && parallelism > 1) {
        // Merge parallel window results using a global window and reduce
        outputStream =
            parallelOutputStream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSizeSeconds)))
                .reduce(
                    (output1, output2) -> {
                      // Merge the two accumulator outputs
                      Summary merged =
                          ((Summary) output1.precompute).merge((Summary) output2.precompute);
                      return new PrecomputedOutput(
                          output1.startTimestamp,
                          output1.endTimestamp,
                          merged,
                          config,
                          null,
                          pipeline,
                          outputMode,
                          verbose);
                    });
      } else {
        // No merging - use parallel outputs directly (either disabled or parallelism = 1)
        outputStream = parallelOutputStream;
      }

      if (verbose && sink != null) {
        outputStream.sinkTo(sink);
      }
    }

    env.execute("Sketch Precompute");
  }
}
