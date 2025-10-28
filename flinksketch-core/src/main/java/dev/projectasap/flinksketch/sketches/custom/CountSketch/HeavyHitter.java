/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.flinksketch.sketches.custom;

// Represents a candidate L2 heavy hitter (key + estimated frequency)
class HeavyHitter implements Comparable<HeavyHitter> {
  public String key;
  public int frequency;

  public HeavyHitter(String key, int frequency) {
    this.key = key;
    this.frequency = frequency;
  }

  public int compareTo(HeavyHitter other) {
    // Sort by frequency (ascending)
    return Integer.compare(this.frequency, other.frequency);
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof HeavyHitter)) {
      return false;
    }

    HeavyHitter other = (HeavyHitter) obj;
    return this.key.equals(other.key);
  }
}
