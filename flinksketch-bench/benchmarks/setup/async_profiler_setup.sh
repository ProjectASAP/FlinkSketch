#!/bin/bash
#
# Copyright 2025 ProjectASAP contributors
# SPDX-License-Identifier: Apache-2.0
#

#
# Async Profiler Setup Script - FlinkSketch
#
# Purpose:
#   Downloads and configures async-profiler for Java application profiling.
#   Installs both x86_64 and arm64 versions for cross-platform compatibility.
#
# Usage:
#   This script can be run from ANY directory using its absolute path:
#
#   /path/to/FlinkSketch/flinksketch-bench/benchmarks/setup/async_profiler_setup.sh
#
#   Or from the FlinkSketch root directory:
#
#   bash flinksketch-bench/benchmarks/setup/async_profiler_setup.sh
#
# Requirements:
#   - Ubuntu/Debian-based Linux system
#   - sudo privileges
#   - Active internet connection
#   - wget installed (usually pre-installed)
#
# What it does:
#   1. Creates tools directory for async-profiler
#   2. Downloads async-profiler for x86_64 architecture
#   3. Downloads async-profiler for arm64 architecture
#   4. Sets executable permissions on binaries
#   5. Configures kernel parameters for profiling
#
# Exit codes:
#   0 - Success
#   1 - Download or extraction failure
#

set -e  # Exit immediately if any command fails

# ============================================================
# Configuration Variables
# ============================================================

ASYNC_PROFILER_VERSION="4.1"

# ============================================================
# Path Resolution
# ============================================================
#
# Calculate absolute paths dynamically so this script works
# regardless of the current working directory when executed.
#
# SCRIPT_DIR: Absolute path to this script's directory (flinksketch-bench/benchmarks/setup/)
# FLINKSKETCH_ROOT: FlinkSketch project root (three levels up from script)
# TOOLS_DIR: Directory where async-profiler will be installed
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLINKSKETCH_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
TOOLS_DIR="${FLINKSKETCH_ROOT}/flinksketch-bench/benchmarks/experiments/tools/async-profiler"

echo "============================================================"
echo "Async Profiler Setup - FlinkSketch"
echo "============================================================"
echo "Script location: ${SCRIPT_DIR}"
echo "FlinkSketch root: ${FLINKSKETCH_ROOT}"
echo "Tools directory: ${TOOLS_DIR}"
echo "Version: ${ASYNC_PROFILER_VERSION}"
echo ""

# ============================================================
# Step 1: Create Tools Directory
# ============================================================

echo "Step 1: Creating tools directory..."
echo "------------------------------------"

# Create directory with full path if it doesn't exist
# Set ownership to current user to avoid permission issues
if [ ! -d "${TOOLS_DIR}" ]; then
    echo "Creating directory: ${TOOLS_DIR}"
    sudo mkdir -p "${TOOLS_DIR}"
    sudo chown -R $USER: "${TOOLS_DIR}"
    echo "✓ Directory created with ownership set to: $USER"
else
    echo "✓ Directory already exists: ${TOOLS_DIR}"
    # Ensure proper ownership even if directory exists
    sudo chown -R $USER: "${TOOLS_DIR}"
fi

# Change to tools directory for downloads
cd "${TOOLS_DIR}"
echo "Working directory: $(pwd)"
echo ""

# ============================================================
# Step 2: Download and Extract x86_64 Version
# ============================================================

echo "Step 2: Setting up x86_64 version..."
echo "-------------------------------------"

# Define variables for x86_64 version
X64_TARBALL="async-profiler-${ASYNC_PROFILER_VERSION}-linux-x64.tar.gz"
X64_URL="https://github.com/async-profiler/async-profiler/releases/download/v${ASYNC_PROFILER_VERSION}/${X64_TARBALL}"
X64_DIR="async-profiler-${ASYNC_PROFILER_VERSION}-linux-x64"

if [ -d "${X64_DIR}" ]; then
    echo "✓ x86_64 version already exists: ${X64_DIR}"
    echo "  Skipping download"
else
    echo "Downloading async-profiler ${ASYNC_PROFILER_VERSION} for x86_64..."
    echo "URL: ${X64_URL}"

    # Download with wget (quiet mode)
    if ! wget -q "${X64_URL}"; then
        echo "ERROR: Failed to download x86_64 version from ${X64_URL}"
        exit 1
    fi

    echo "✓ Download complete"
    echo "Extracting archive..."
    tar -xzf "${X64_TARBALL}"

    echo "Cleaning up tarball..."
    rm -f "${X64_TARBALL}"

    echo "✓ x86_64 version installed to: ${X64_DIR}"
fi

echo ""

# ============================================================
# Step 3: Download and Extract arm64 Version
# ============================================================

echo "Step 3: Setting up arm64 version..."
echo "------------------------------------"

# Define variables for arm64 version
ARM64_TARBALL="async-profiler-${ASYNC_PROFILER_VERSION}-linux-arm64.tar.gz"
ARM64_URL="https://github.com/async-profiler/async-profiler/releases/download/v${ASYNC_PROFILER_VERSION}/${ARM64_TARBALL}"
ARM64_DIR="async-profiler-${ASYNC_PROFILER_VERSION}-linux-arm64"

if [ -d "${ARM64_DIR}" ]; then
    echo "✓ arm64 version already exists: ${ARM64_DIR}"
    echo "  Skipping download"
else
    echo "Downloading async-profiler ${ASYNC_PROFILER_VERSION} for arm64..."
    echo "URL: ${ARM64_URL}"

    # Download with wget (quiet mode)
    if ! wget -q "${ARM64_URL}"; then
        echo "ERROR: Failed to download arm64 version from ${ARM64_URL}"
        exit 1
    fi

    echo "✓ Download complete"
    echo "Extracting archive..."
    tar -xzf "${ARM64_TARBALL}"

    echo "Cleaning up tarball..."
    rm -f "${ARM64_TARBALL}"

    echo "✓ arm64 version installed to: ${ARM64_DIR}"
fi

echo ""

# ============================================================
# Step 4: Set Executable Permissions
# ============================================================

echo "Step 4: Setting executable permissions..."
echo "------------------------------------------"

# Set permissions for x86_64 binaries
if [ -d "${X64_DIR}" ]; then
    echo "Setting permissions for x86_64 binaries..."
    # asprof: Main async-profiler command-line tool
    # jfrconv: Java Flight Recorder converter
    # libasyncProfiler.so: Native profiling library
    chmod +x "${X64_DIR}/bin/asprof" 2>/dev/null || true
    chmod +x "${X64_DIR}/bin/jfrconv" 2>/dev/null || true
    chmod +x "${X64_DIR}/lib/libasyncProfiler.so" 2>/dev/null || true
    echo "✓ x86_64 permissions set"
else
    echo "Warning: x86_64 directory not found, skipping permissions"
fi

# Set permissions for arm64 binaries
if [ -d "${ARM64_DIR}" ]; then
    echo "Setting permissions for arm64 binaries..."
    chmod +x "${ARM64_DIR}/bin/asprof" 2>/dev/null || true
    chmod +x "${ARM64_DIR}/bin/jfrconv" 2>/dev/null || true
    chmod +x "${ARM64_DIR}/lib/libasyncProfiler.so" 2>/dev/null || true
    echo "✓ arm64 permissions set"
else
    echo "Warning: arm64 directory not found, skipping permissions"
fi

echo ""

# ============================================================
# Step 5: Configure Kernel Parameters for Profiling
# ============================================================

echo "Step 5: Configuring kernel parameters..."
echo "-----------------------------------------"

# These kernel parameters are required for async-profiler to function properly:
#
# kernel.perf_event_paranoid
#   Controls access to performance monitoring events.
#   Values:
#     2 (default): Requires CAP_PERFMON or CAP_SYS_ADMIN (most restrictive)
#     1: Allow profiling own processes without special permissions
#     0: Allow kernel profiling (less restrictive)
#    -1: Allow all profiling (least restrictive)
#
# kernel.kptr_restrict
#   Controls visibility of kernel pointers in /proc.
#   Values:
#     0: Kernel pointers visible to all (allows symbol resolution)
#     1: Kernel pointers hidden from non-privileged users (default)
#     2: Kernel pointers hidden from all users (most restrictive)
#

echo "Setting kernel.perf_event_paranoid=1..."
echo "  (Allows users to profile their own processes)"
sudo sysctl kernel.perf_event_paranoid=1

echo "Setting kernel.kptr_restrict=0..."
echo "  (Allows async-profiler to resolve kernel symbols)"
sudo sysctl kernel.kptr_restrict=0

echo "✓ Kernel parameters configured"
echo ""
echo "Note: These settings are temporary and will reset on reboot."
echo ""
echo "To make them persistent across reboots, add these lines to /etc/sysctl.conf:"
echo "  kernel.perf_event_paranoid=1"
echo "  kernel.kptr_restrict=0"
echo ""
echo "Then run: sudo sysctl -p"
echo ""

# ============================================================
# Summary
# ============================================================

echo "============================================================"
echo "Async Profiler Setup Complete!"
echo "============================================================"
echo ""
echo "Installation locations:"

if [ -d "${X64_DIR}" ]; then
    echo "  x86_64 version:"
    echo "    Directory: ${TOOLS_DIR}/${X64_DIR}"
    echo "    Binary:    ${TOOLS_DIR}/${X64_DIR}/bin/asprof"
    echo "    Library:   ${TOOLS_DIR}/${X64_DIR}/lib/libasyncProfiler.so"
else
    echo "  x86_64 version: NOT INSTALLED"
fi

echo ""

if [ -d "${ARM64_DIR}" ]; then
    echo "  arm64 version:"
    echo "    Directory: ${TOOLS_DIR}/${ARM64_DIR}"
    echo "    Binary:    ${TOOLS_DIR}/${ARM64_DIR}/bin/asprof"
    echo "    Library:   ${TOOLS_DIR}/${ARM64_DIR}/lib/libasyncProfiler.so"
else
    echo "  arm64 version: NOT INSTALLED"
fi

echo ""
echo "Current kernel parameters:"
echo "  kernel.perf_event_paranoid = $(sysctl -n kernel.perf_event_paranoid)"
echo "  kernel.kptr_restrict       = $(sysctl -n kernel.kptr_restrict)"
echo ""
echo "Usage example:"
echo "  # Auto-detect architecture and set profiler path"
echo "  ARCH=\$(uname -m)"
echo "  if [ \"\$ARCH\" = \"x86_64\" ]; then"
echo "    ASPROF=\"${TOOLS_DIR}/${X64_DIR}/bin/asprof\""
echo "  elif [ \"\$ARCH\" = \"aarch64\" ]; then"
echo "    ASPROF=\"${TOOLS_DIR}/${ARM64_DIR}/bin/asprof\""
echo "  fi"
echo ""
echo "  # Profile a Java process for 30 seconds"
echo "  \$ASPROF -d 30 -f flamegraph.html <java-pid>"
echo ""
echo "  # Start profiling a Java process"
echo "  \$ASPROF start <java-pid>"
echo ""
echo "  # Stop profiling and generate flamegraph"
echo "  \$ASPROF stop -f flamegraph.html <java-pid>"
echo ""
echo "Setup completed successfully!"
