/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.sinks;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.projectasap.flinksketch.datamodel.PrecomputedOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder for creating Flink sinks from command-line arguments. Supports file sinks with various
 * output formats.
 */
public class SinkBuilder {
  private static final Logger logger = LoggerFactory.getLogger(SinkBuilder.class);

  /**
   * Builds a Flink sink based on parsed command-line arguments.
   *
   * @param parsedArgs the parsed command-line arguments
   * @return the configured Flink sink
   */
  public static Sink<PrecomputedOutput> buildSink(Namespace parsedArgs) {
    String outputFormat = parsedArgs.getString("outputFormat");
    String outputPath = parsedArgs.getString("outputFilePath");

    logger.info("Building sink with output format: {}", outputFormat);
    logger.info("Using file sink with path: {}", outputPath);

    Sink<PrecomputedOutput> sink =
        FileSink.forRowFormat(
                new Path(outputPath),
                new Encoder<PrecomputedOutput>() {
                  @Override
                  public void encode(PrecomputedOutput data, OutputStream stream)
                      throws IOException {
                    try {
                      if (outputFormat.equals("byte")) {
                        stream.write(data.serializeToBytes());
                      } else if (outputFormat.equals("json")) {
                        JsonNode jsonNode = data.serializeToJson();
                        ObjectMapper objectMapper = new ObjectMapper();
                        stream.write(objectMapper.writeValueAsBytes(jsonNode));
                        stream.write("\n".getBytes());
                      } else {
                        throw new IllegalArgumentException(
                            "Invalid output format: " + outputFormat);
                      }
                    } catch (JsonProcessingException e) {
                      logger.info("Error serializing to JSON", e);
                      throw new IOException("Error serializing to JSON", e);
                    }
                  }
                })
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withRolloverInterval(Duration.ofMinutes(15))
                    .withInactivityInterval(Duration.ofMinutes(1))
                    .withMaxPartSize(1024 * 1024 * 1024)
                    .build())
            .build();

    return sink;
  }
}
