/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.windowfunctions;

import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.projectasap.flinksketch.datamodel.PrecomputedOutput;
import dev.projectasap.flinksketch.datamodel.Summary;
import dev.projectasap.flinksketch.utils.AggregationConfig;
import dev.projectasap.flinksketch.utils.OutputStrategy;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process function that adds window metadata to all aggregated summaries. Handles global windows
 * and executes queries for insertion_querying pipeline mode.
 */
public class GlobalWindowProcessor
    extends ProcessAllWindowFunction<Summary, PrecomputedOutput, TimeWindow> {
  private AggregationConfig config;
  private String pipeline;
  private String outputMode;
  private boolean verbose;
  private static final Logger logger = LoggerFactory.getLogger(GlobalWindowProcessor.class);

  /**
   * Constructs a GlobalWindowProcessor.
   *
   * @param config the aggregation configuration
   * @param pipeline the pipeline mode (e.g., "insertion_querying")
   * @param outputMode the output mode (e.g., "query", "sketch_query")
   * @param verbose whether to enable verbose logging
   */
  public GlobalWindowProcessor(
      AggregationConfig config, String pipeline, String outputMode, boolean verbose) {
    this.config = config;
    this.pipeline = pipeline;
    this.outputMode = outputMode;
    this.verbose = verbose;
  }

  @Override
  public void process(
      Context context, Iterable<Summary> elements, Collector<PrecomputedOutput> out) {
    Summary result = elements.iterator().next();
    long start = context.window().getStart();
    long end = context.window().getEnd();

    PrecomputedOutput output =
        new PrecomputedOutput(start, end, result, config, null, pipeline, outputMode, verbose);

    // Execute queries in window function if needed (for insertion_querying pipeline)
    if (output.getOutputStrategy().shouldExecuteQueries()) {
      try {
        // Execute queries and cache results for later serialization
        ObjectNode queryResults =
            OutputStrategy.executeQueries(result, config.keys, config.ranks, config.statistic);
        output.setCachedQueryResults(queryResults);
      } catch (Exception e) {
        logger.error(
            "Failed to execute queries for window [{}, {}]: {}", start, end, e.getMessage());
      }
    }

    out.collect(output);
  }
}
