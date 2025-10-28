/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.datamodel;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Interface for objects that can be serialized for output to sinks. Provides methods for byte array
 * and JSON serialization.
 */
public interface SerializableToSink {
  byte[] serializeToBytes();

  JsonNode serializeToJson();

  default String serializeToString() {
    // Default to JSON string
    return serializeToJson().toString();
  }
}
