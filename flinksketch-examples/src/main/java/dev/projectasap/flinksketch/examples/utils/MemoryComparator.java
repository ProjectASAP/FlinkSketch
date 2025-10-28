/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.examples.utils;

import dev.projectasap.flinksketch.datamodel.PrecomputedOutput;
import java.util.Set;

/**
 * Utility class for comparing memory usage between baseline and sketch implementations. Memory
 * comparison is performed once per aggregation window, regardless of key cardinality, as memory
 * usage is a property of the data structure itself, not individual keys.
 */
public class MemoryComparator {

  private PrecomputedOutput baselineOutput;
  private PrecomputedOutput sketchOutput;

  public MemoryComparator(PrecomputedOutput sketch, PrecomputedOutput baseline) {
    this.baselineOutput = baseline;
    this.sketchOutput = sketch;
  }

  /**
   * Compare memory usage between baseline and sketch implementations. Uses reflection to invoke
   * get_memory() method on both accumulators.
   *
   * @return MemoryComparisonResult containing memory metrics
   * @throws Exception if reflection fails or get_memory() method is not found
   */
  public MemoryComparisonResult compareMemory() throws Exception {
    return compareMemory(-1);
  }

  /**
   * Compare memory usage between baseline and sketch implementations with window number. Uses
   * reflection to invoke get_memory() method on both accumulators.
   *
   * @param windowNumber the window number (1-indexed)
   * @return MemoryComparisonResult containing memory metrics and window number
   * @throws Exception if reflection fails or get_memory() method is not found
   */
  public MemoryComparisonResult compareMemory(int windowNumber) throws Exception {
    // Use reflection to call get_memory() on baseline accumulator
    java.lang.reflect.Method baselineMemoryMethod =
        baselineOutput.precompute.getClass().getMethod("get_memory");
    long baselineMemory = (Long) baselineMemoryMethod.invoke(baselineOutput.precompute);

    // Use reflection to call get_memory() on sketch accumulator
    java.lang.reflect.Method sketchMemoryMethod =
        sketchOutput.precompute.getClass().getMethod("get_memory");
    long sketchMemory = (Long) sketchMemoryMethod.invoke(sketchOutput.precompute);
    // By default, do a key-agnostic memory comparison. If callers need per-key
    // cardinality information they should call `compareMemoryAllKeys(...)` which
    // will attempt to extract the key set and will throw if that information is
    // not available (mirrors QueryResultComparator.allComparisons behavior).
    int keyCardinality = -1; // unknown / not requested

    return new MemoryComparisonResult(baselineMemory, sketchMemory, keyCardinality, windowNumber);
  }

  /**
   * Compare memory usage and return the key-cardinality-aware result. This will attempt to call
   * `getKeySet()` on the baseline accumulator and will throw an IllegalStateException if the method
   * is not provided (same behavior as QueryResultComparator.allComparisons).
   *
   * @param windowNumber the window number (1-indexed)
   * @return MemoryComparisonResult containing memory metrics and key cardinality
   * @throws Exception if reflection fails or the baseline does not provide getKeySet()
   */
  public MemoryComparisonResult compareMemoryAllKeys(int windowNumber) throws Exception {
    java.lang.reflect.Method baselineMemoryMethod =
        baselineOutput.precompute.getClass().getMethod("get_memory");
    long baselineMemory = (Long) baselineMemoryMethod.invoke(baselineOutput.precompute);

    java.lang.reflect.Method sketchMemoryMethod =
        sketchOutput.precompute.getClass().getMethod("get_memory");
    long sketchMemory = (Long) sketchMemoryMethod.invoke(sketchOutput.precompute);

    // Attempt to retrieve the key set; throw if not present to match QueryResultComparator
    java.lang.reflect.Method getKeySetMethod =
        baselineOutput.precompute.getClass().getMethod("getKeySet");
    Set<?> keySet = (Set<?>) getKeySetMethod.invoke(baselineOutput.precompute);
    if (keySet == null || keySet.isEmpty()) {
      throw new IllegalStateException(
          "Baseline precompute does not provide key set for PER_KEY memory comparison");
    }

    int keyCardinality = keySet.size();
    return new MemoryComparisonResult(baselineMemory, sketchMemory, keyCardinality, windowNumber);
  }

  /** Result class containing memory comparison metrics. */
  public static class MemoryComparisonResult {
    public final long baselineMemoryBytes;
    public final long sketchMemoryBytes;
    public final int keyCardinality;
    public final double compressionRatio;
    public final double spaceSavingsPercent;
    public final int windowNumber;

    public MemoryComparisonResult(
        long baselineMemoryBytes, long sketchMemoryBytes, int keyCardinality) {
      this(baselineMemoryBytes, sketchMemoryBytes, keyCardinality, -1);
    }

    public MemoryComparisonResult(
        long baselineMemoryBytes, long sketchMemoryBytes, int keyCardinality, int windowNumber) {
      this.baselineMemoryBytes = baselineMemoryBytes;
      this.sketchMemoryBytes = sketchMemoryBytes;
      this.keyCardinality = keyCardinality;
      this.windowNumber = windowNumber;
      this.compressionRatio =
          sketchMemoryBytes > 0 ? (double) baselineMemoryBytes / sketchMemoryBytes : 0.0;
      this.spaceSavingsPercent =
          baselineMemoryBytes > 0
              ? 100.0 * (1.0 - ((double) sketchMemoryBytes / baselineMemoryBytes))
              : 0.0;
    }

    @Override
    public String toString() {
      double baselineKB = baselineMemoryBytes / 1024.0;
      double sketchKB = sketchMemoryBytes / 1024.0;

      StringBuilder sb = new StringBuilder();
      sb.append("\n========== MEMORY COMPARISON ==========\n");
      if (windowNumber >= 0) {
        sb.append(String.format("Window:                  #%d\n", windowNumber));
      }
      if (keyCardinality >= 0) {
        sb.append(String.format("Key Cardinality:         %,d unique keys\n", keyCardinality));
      }
      sb.append(
          String.format(
              "Memory usage before FlinkSketch: %,d bytes (%.2f KB)\n",
              baselineMemoryBytes, baselineKB));
      sb.append(
          String.format(
              "Memory usage after FlinkSketch:  %,d bytes (%.2f KB)\n",
              sketchMemoryBytes, sketchKB));
      sb.append(String.format("Reduction in memory use:       %.2fx\n", compressionRatio));
      sb.append("=======================================");

      return sb.toString();
    }
  }
}
