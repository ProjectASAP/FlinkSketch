/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.datamodel;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Set;

/**
 * Interface for precompute operator outputs (sketches and baseline accumulators). Uses
 * self-referential generic type T to ensure type-safe merge operations.
 *
 * <p>Inherits serialization methods from SerializableToSink: - byte[] serializeToBytes() - JsonNode
 * serializeToJson() - String serializeToString()
 */
public interface Summary<T extends Summary<T>> extends SerializableToSink {

  /**
   * Execute a query on the accumulated data.
   *
   * @param params Query parameters as JsonNode. Format depends on query type: - For frequency:
   *     {"key": "someKey"} - For top-k: {"topk": k} or null (returns all top-k) - For cardinality:
   *     null (returns distinct count) - For quantile: {"quantile": 0.95}
   * @return Query result as JsonNode
   */
  JsonNode query(JsonNode params);

  /**
   * Get the memory footprint of this accumulator in bytes. Should calculate raw data size without
   * overhead (e.g., length * sizeof for strings/arrays).
   *
   * @return Memory usage in bytes
   */
  long get_memory();

  /**
   * Add a key-value pair to the accumulator.
   *
   * @param key The key to add
   * @param value The value associated with the key, based on subtype
   */
  void add(String key, Integer value);

  /**
   * Merge another accumulator of the same type into a new accumulator. This operation should be
   * commutative and associative for correct parallel processing.
   *
   * @param other The accumulator to merge with
   * @return A new merged accumulator of type T
   */
  T merge(T other);

  /**
   * @return KeySet of the accumulator if applicable, else null
   */
  default Set<String> getKeySet() {
    return null;
  }
}
