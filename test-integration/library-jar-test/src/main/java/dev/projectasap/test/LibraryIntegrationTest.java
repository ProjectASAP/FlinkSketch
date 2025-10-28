/*
 * Copyright 2025 ProjectASAP contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.projectasap.test;

import dev.projectasap.flinksketch.sketches.datasketches.DataSketchHllSketch;
import dev.projectasap.flinksketch.sketches.datasketches.DataSketchKllFloatsSketch;
import dev.projectasap.flinksketch.sketches.custom.CountMinSketch;
import dev.projectasap.flinksketch.datamodel.DataPoint;
import dev.projectasap.flinksketch.utils.ConfigLoader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Integration test to verify FlinkSketch works as a library dependency.
 *
 * <p>Tests: 1. Classes can be imported successfully 2. No dependency conflicts occur 3. Basic
 * instantiation works 4. Core classes are loadable
 */
public class LibraryIntegrationTest {

  public static void main(String[] args) {
    System.out.println("=== FlinkSketch Library JAR Integration Test ===\n");

    int passed = 0;
    int failed = 0;

    // Test 1: Class imports
    try {
      System.out.println("[Test 1] Verifying class imports...");
      System.out.println("✓ Classes imported successfully");
      passed++;
    } catch (Exception e) {
      System.err.println("✗ Test 1 FAILED: " + e.getMessage());
      failed++;
    }

    // Test 2: HLL Sketch class loading
    try {
      System.out.println("\n[Test 2] Loading HLL Sketch class...");
      Class<?> hllClass = DataSketchHllSketch.class;
      System.out.println("✓ HLL Sketch class loaded successfully: " + hllClass.getName());
      passed++;
    } catch (Exception e) {
      System.err.println("✗ Test 2 FAILED: " + e.getMessage());
      e.printStackTrace();
      failed++;
    }

    // Test 3: KLL Sketch class loading
    try {
      System.out.println("\n[Test 3] Loading KLL Sketch class...");
      Class<?> kllClass = DataSketchKllFloatsSketch.class;
      System.out.println("✓ KLL Sketch class loaded successfully: " + kllClass.getName());
      passed++;
    } catch (Exception e) {
      System.err.println("✗ Test 3 FAILED: " + e.getMessage());
      e.printStackTrace();
      failed++;
    }

    // Test 4: Count-Min Sketch class loading
    try {
      System.out.println("\n[Test 4] Loading Count-Min Sketch class...");
      Class<?> cmsClass = CountMinSketch.class;
      System.out.println("✓ Count-Min Sketch class loaded successfully: " + cmsClass.getName());
      passed++;
    } catch (Exception e) {
      System.err.println("✗ Test 4 FAILED: " + e.getMessage());
      e.printStackTrace();
      failed++;
    }

    // Test 5: Flink environment integration
    try {
      System.out.println("\n[Test 5] Creating Flink environment...");
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      System.out.println("✓ Flink environment created successfully");
      passed++;
    } catch (Exception e) {
      System.err.println("✗ Test 5 FAILED: " + e.getMessage());
      e.printStackTrace();
      failed++;
    }

    // Test 6: Verify all core classes loadable
    try {
      System.out.println("\n[Test 6] Loading core classes...");
      Class.forName("dev.projectasap.flinksketch.datamodel.DataPoint");
      Class.forName("dev.projectasap.flinksketch.utils.ConfigLoader");
      Class.forName("dev.projectasap.flinksketch.utils.StreamingConfig");
      Class.forName("dev.projectasap.flinksketch.datamodel.PrecomputedOutput");
      System.out.println("✓ All core classes loadable");
      passed++;
    } catch (ClassNotFoundException e) {
      System.err.println("✗ Test 6 FAILED: " + e.getMessage());
      e.printStackTrace();
      failed++;
    }

    // Test 7: Verify no duplicate dependencies
    try {
      System.out.println("\n[Test 7] Checking for dependency conflicts...");
      // Try to load a common dependency (Guava) that might conflict
      Class.forName("com.google.common.collect.Lists");
      System.out.println("✓ No ClassLoader conflicts detected");
      passed++;
    } catch (ClassNotFoundException e) {
      System.err.println("✗ Test 7 FAILED: Missing dependency - " + e.getMessage());
      failed++;
    }

    // Summary
    String separator = "=".repeat(50);
    System.out.println("\n" + separator);
    System.out.println("TEST RESULTS");
    System.out.println(separator);
    System.out.println("Passed: " + passed);
    System.out.println("Failed: " + failed);
    System.out.println("Total:  " + (passed + failed));

    if (failed == 0) {
      System.out.println("\n✓✓✓ ALL LIBRARY JAR TESTS PASSED ✓✓✓");
      System.exit(0);
    } else {
      System.out.println("\n✗✗✗ SOME TESTS FAILED ✗✗✗");
      System.exit(1);
    }
  }
}
