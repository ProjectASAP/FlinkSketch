/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for loading streaming configuration from YAML files. Parses aggregation
 * configurations and parameters.
 */
public class ConfigLoader {
  /**
   * Loads streaming configuration from a YAML file.
   *
   * @param configFilePath path to the YAML configuration file
   * @return parsed streaming configuration
   * @throws IOException if file reading or parsing fails
   */
  public static StreamingConfig loadConfig(String configFilePath) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    String yamlContent = new String(Files.readAllBytes(Paths.get(configFilePath)));
    ObjectNode rootNode = mapper.readValue(yamlContent, ObjectNode.class);

    StreamingConfig streamingConfig = new StreamingConfig();

    List<AggregationConfig> aggregationConfigs = new ArrayList<>();
    rootNode
        .get("aggregations")
        .forEach(
            node -> {
              AggregationConfig config = new AggregationConfig();
              config.aggregationId = node.get("aggregationId").asInt();
              config.aggregationType = node.get("aggregationType").asText();
              config.aggregationSubType = node.get("aggregationSubType").asText();
              config.aggregationPackage = node.get("aggregationPackage").asText();

              Map<String, String> parameters = new HashMap<>();
              node.get("parameters")
                  .fields()
                  .forEachRemaining(
                      entry -> {
                        parameters.put(entry.getKey(), entry.getValue().asText());
                      });
              config.parameters = parameters;

              List<String> groupingLabels = new ArrayList<>();
              List<String> aggregatedLabels = new ArrayList<>();
              List<String> rollupLabels = new ArrayList<>();

              config.tumblingWindowSize = node.get("tumblingWindowSize").asInt();

              config.setOriginalYaml(node.toString());
              aggregationConfigs.add(config);
            });
    streamingConfig.aggregationConfigs = aggregationConfigs;

    return streamingConfig;
  }
}
