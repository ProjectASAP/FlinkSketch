/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.sinks;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

/** Serialization schema for 2D integer arrays. Converts int[][] to JSON byte arrays. */
public class IntArraySerializationSchema implements SerializationSchema<int[][]> {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public byte[] serialize(int[][] element) {
    try {
      return objectMapper.writeValueAsBytes(element);
    } catch (Exception e) {
      throw new RuntimeException("Error serializing int[][]", e);
    }
  }
}
