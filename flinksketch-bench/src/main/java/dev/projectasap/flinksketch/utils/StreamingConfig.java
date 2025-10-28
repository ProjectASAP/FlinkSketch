/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.utils;

import java.util.List;

/** Configuration for the streaming job. Contains aggregation configurations. */
public class StreamingConfig {
  public List<AggregationConfig> aggregationConfigs;
}
