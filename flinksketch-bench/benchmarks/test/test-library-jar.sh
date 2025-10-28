#!/bin/bash
#
# Copyright 2025 ProjectASAP contributors
# SPDX-License-Identifier: Apache-2.0
#

# Test script for library JAR integration

set -e

PROJECT_ROOT="/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/FlinkSketch"

echo "============================================================"
echo "FlinkSketch Library JAR Integration Test"
echo "============================================================"

# Step 1: Build FlinkSketch
echo -e "\n[Step 1] Building FlinkSketch..."
cd "$PROJECT_ROOT"
mvn clean install -DskipTests

# Step 2: Verify library JAR exists and is small
echo -e "\n[Step 2] Verifying library JAR..."
LIBRARY_JAR="$PROJECT_ROOT/flinksketch-core/target/flinksketch-core-0.1.jar"
if [ ! -f "$LIBRARY_JAR" ]; then
    echo "✗ FAIL: Library JAR not found at $LIBRARY_JAR"
    exit 1
fi

JAR_SIZE=$(du -k "$LIBRARY_JAR" | cut -f1)
echo "✓ Library JAR found: $LIBRARY_JAR (${JAR_SIZE}KB)"

if [ "$JAR_SIZE" -gt 5000 ]; then
    echo "⚠ WARNING: Library JAR size is ${JAR_SIZE}KB (expected < 5MB for library)"
    echo "  This may indicate dependencies are being bundled"
fi

# Step 3: Verify shaded JAR exists and is large
echo -e "\n[Step 3] Verifying shaded JAR..."
SHADED_JAR="$PROJECT_ROOT/flinksketch-core/target/flinksketch-core-0.1-shaded.jar"
if [ ! -f "$SHADED_JAR" ]; then
    echo "✗ FAIL: Shaded JAR not found at $SHADED_JAR"
    exit 1
fi

SHADED_SIZE=$(du -k "$SHADED_JAR" | cut -f1)
echo "✓ Shaded JAR found: $SHADED_JAR (${SHADED_SIZE}KB)"

if [ "$SHADED_SIZE" -lt 10000 ]; then
    echo "⚠ WARNING: Shaded JAR size is ${SHADED_SIZE}KB (expected > 10MB with dependencies)"
fi

# Step 4: Verify source JAR exists
echo -e "\n[Step 4] Verifying source JAR..."
SOURCE_JAR="$PROJECT_ROOT/flinksketch-core/target/flinksketch-core-0.1-sources.jar"
if [ ! -f "$SOURCE_JAR" ]; then
    echo "✗ FAIL: Source JAR not found at $SOURCE_JAR"
    exit 1
fi
SOURCE_SIZE=$(du -k "$SOURCE_JAR" | cut -f1)
echo "✓ Source JAR found: $SOURCE_JAR (${SOURCE_SIZE}KB)"

# Step 5: Verify javadoc JAR exists
echo -e "\n[Step 5] Verifying Javadoc JAR..."
JAVADOC_JAR="$PROJECT_ROOT/flinksketch-core/target/flinksketch-core-0.1-javadoc.jar"
if [ ! -f "$JAVADOC_JAR" ]; then
    echo "✗ FAIL: Javadoc JAR not found at $JAVADOC_JAR"
    exit 1
fi
JAVADOC_SIZE=$(du -k "$JAVADOC_JAR" | cut -f1)
echo "✓ Javadoc JAR found: $JAVADOC_JAR (${JAVADOC_SIZE}KB)"

# Step 6: Verify installation to Maven local repo
echo -e "\n[Step 6] Verifying Maven local repository installation..."
LOCAL_REPO_PATH="$HOME/.m2/repository/dev/projectasap/flinksketch/0.1"
if [ ! -d "$LOCAL_REPO_PATH" ]; then
    echo "✗ FAIL: FlinkSketch not found in local Maven repository"
    exit 1
fi
echo "✓ FlinkSketch installed to: $LOCAL_REPO_PATH"
ls -lh "$LOCAL_REPO_PATH"/*.jar

# Step 7: Build integration test
echo -e "\n[Step 7] Building integration test project..."
cd "$PROJECT_ROOT/test-integration/library-jar-test"
mvn clean compile

# Step 8: Run integration test
echo -e "\n[Step 8] Running integration test..."
mvn exec:java -Dexec.mainClass="dev.projectasap.test.LibraryIntegrationTest"

echo -e "\n============================================================"
echo "✓✓✓ LIBRARY JAR TEST SUITE PASSED ✓✓✓"
echo "============================================================"
