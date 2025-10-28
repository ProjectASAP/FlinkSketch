/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.datamodel;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Represents a single data point with timestamp, key, and value. Used as the basic unit of data in
 * the streaming pipeline.
 */
@JsonPropertyOrder({"timestamp", "key", "value"})
public class DataPoint {
  public Long timestamp;
  public String key;
  public Integer value;

  /** Default constructor initializing all fields to default values. */
  public DataPoint() {
    this.timestamp = 0L;
    this.key = "";
    this.value = 0;
  }

  /**
   * Constructs a DataPoint with specified values.
   *
   * @param timestamp the event timestamp
   * @param key the data key
   * @param value the data value
   */
  public DataPoint(Long timestamp, String key, Integer value) {
    this.timestamp = timestamp;
    this.key = key;
    this.value = value;
  }

  @Override
  public String toString() {
    return "DataPoint{"
        + "timestamp="
        + timestamp
        + ", key='"
        + key
        + '\''
        + ", value="
        + value
        + '}';
  }
}
