/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.datamodel.Summary;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Configuration for a single aggregation function. Contains type, parameters, and window settings
 * for the aggregation.
 */
public class AggregationConfig implements Serializable {
  public Integer aggregationId;
  public String aggregationType;
  public String aggregationSubType;
  public String aggregationPackage;
  public Map<String, String> parameters;
  public int tumblingWindowSize;
  public String spatialFilter;
  public String metric;
  public List<String> keys;
  public List<String> ranks;
  public String statistic;

  private String originalYaml;

  public void setOriginalYaml(String originalYaml) {
    this.originalYaml = originalYaml;
  }

  public byte[] serializeToBytes() {
    return originalYaml.getBytes();
  }

  /**
   * Serializes the aggregation configuration to JSON.
   *
   * @return JsonNode containing the configuration details
   */
  public JsonNode serializeToJson() {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode jsonNode = objectMapper.createObjectNode();

    jsonNode.put("aggregationId", this.aggregationId);
    jsonNode.put("aggregationType", this.aggregationType);
    jsonNode.put("aggregationSubType", this.aggregationSubType);
    jsonNode.put("aggregationPackage", this.aggregationPackage);
    jsonNode.putPOJO("parameters", this.parameters);

    // Serialize new fields
    jsonNode.put("tumblingWindowSize", this.tumblingWindowSize);

    return jsonNode;
  }

  /**
   * Instantiates the aggregation function based on configuration.
   *
   * @return the instantiated aggregation function
   * @throws RuntimeException if function instantiation fails
   */
  public AggregateFunction<?, ?, Summary> getAggregationFunction() {
    try {
      String packageName =
          "dev.projectasap.flinksketch.sketches." + aggregationPackage + "." + aggregationType;
      Class<?> clazz = Class.forName(packageName);
      return (AggregateFunction<DataPoint, ?, Summary>)
          clazz.getConstructor(String.class, Map.class).newInstance(aggregationSubType, parameters);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create aggregation function", e);
    }
  }
}
