#!/bin/bash
#
# Copyright 2025 ProjectASAP contributors
# SPDX-License-Identifier: Apache-2.0
#

#
# Environment Setup Script - FlinkSketch
#
# Purpose:
#   Installs all required dependencies for FlinkSketch development and execution:
#   - Python3 tooling and development libraries
#   - OpenJDK 11 and Maven for Java compilation
#   - uv Python package manager
#   - Docker Engine for containerized Flink execution
#   - Apache Flink 1.20.2
#
# Usage:
#   This script can be run from ANY directory using its absolute path:
#
#   /path/to/FlinkSketch/flinksketch-bench/benchmarks/setup/env_setup.sh
#
#   Or from the FlinkSketch root directory:
#
#   bash flinksketch-bench/benchmarks/setup/env_setup.sh
#
# Requirements:
#   - Ubuntu/Debian-based Linux system
#   - sudo privileges
#   - Active internet connection
#
# What it does:
#   1. Installs system packages (Python3, Java, Maven, Docker)
#   2. Installs uv and syncs Python dependencies
#   3. Downloads and installs Apache Flink 1.20.2
#   4. Moves FlinkSketch into the Flink directory
#   5. Compiles the FlinkSketch Maven project
#
# Exit codes:
#   0 - Success
#   1 - Failure (via set -e)
#

set -euo pipefail  # Exit on error, undefined variables, pipe failures

# ============================================================
# Configuration Variables
# ============================================================

FLINK_VERSION="1.20.2"
SCALA_VERSION="2.12"
FLINK_TGZ="flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz"

# ============================================================
# Path Resolution
# ============================================================
#
# Calculate absolute paths dynamically so this script works
# regardless of the current working directory when executed.
#
# SCRIPT_DIR: Absolute path to this script's directory (flinksketch-bench/benchmarks/setup/)
# FLINKSKETCH_ROOT: FlinkSketch project root (three levels up from script)
# PARENT_DIR: Parent directory where Flink will be installed
# FLINK_DIR: Target Flink installation directory
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLINKSKETCH_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
PARENT_DIR="$(cd "${FLINKSKETCH_ROOT}/.." && pwd)"
FLINK_DIR="${PARENT_DIR}/flink-${FLINK_VERSION}"

echo "============================================================"
echo "FlinkSketch Environment Setup"
echo "============================================================"
echo "Script location: ${SCRIPT_DIR}"
echo "FlinkSketch root: ${FLINKSKETCH_ROOT}"
echo "Flink will be installed to: ${FLINK_DIR}"
echo ""

# ============================================================
# Step 1: System Package Updates
# ============================================================

echo "Step 1: Updating system packages..."
echo "------------------------------------"
sudo apt update -y

# ============================================================
# Step 2: Install Python Development Tools
# ============================================================

echo ""
echo "Step 2: Installing Python3 and development tools..."
echo "----------------------------------------------------"
# python3: Python 3 interpreter
# python3-dev: Header files for building Python extensions
# python3-pip: Python package installer
# python3-venv: Virtual environment support
# curl: For downloading files
# ca-certificates: SSL certificate authorities
# gnupg: GNU Privacy Guard for signature verification
sudo apt install -y python3 python3-dev python3-pip python3-venv curl ca-certificates gnupg

# ============================================================
# Step 3: Install Java Development Kit and Maven
# ============================================================

echo ""
echo "Step 3: Installing OpenJDK 11 and Maven..."
echo "--------------------------------------------"
# openjdk-11-jdk: Java Development Kit (required for Flink and Maven)
# maven: Build tool for Java projects
sudo apt install -y openjdk-11-jdk maven

# ============================================================
# Step 4: Install uv Python Package Manager
# ============================================================

echo ""
echo "Step 4: Installing uv Python package manager..."
echo "------------------------------------------------"
# uv is a fast Python package manager (alternative to pip)
# It will be installed to ~/.cargo/bin/uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# ============================================================
# Step 5: Sync Python Dependencies
# ============================================================

echo ""
echo "Step 5: Syncing Python environment in flinksketch-bench/benchmarks/experiments/..."
echo "---------------------------------------------------------------------------------"
# Navigate to experiments directory and sync dependencies using uv
# This reads pyproject.toml and installs all required Python packages
EXPERIMENTS_DIR="${FLINKSKETCH_ROOT}/flinksketch-bench/benchmarks/experiments"

if [ -d "${EXPERIMENTS_DIR}" ]; then
    (cd "${EXPERIMENTS_DIR}" && {
        # Try uv in PATH first, then ~/.cargo/bin/uv as fallback
        if command -v uv &> /dev/null; then
            uv sync
        else
            ~/.local/bin/uv sync
        fi
    })
else
    echo "Warning: Experiments directory not found at ${EXPERIMENTS_DIR}"
fi

# ============================================================
# Step 6: Install Docker Engine
# ============================================================

echo ""
echo "Step 6: Installing Docker Engine..."
echo "------------------------------------"

# 6.1: Install Docker prerequisites
echo "Installing Docker prerequisites..."
sudo apt-get update -y
sudo apt-get install -y ca-certificates curl gnupg lsb-release

# 6.2: Add Docker's official GPG key
echo "Adding Docker GPG key..."
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | sudo gpg --dearmor -o /etc/apt/keyrings/docker-archive-keyring.gpg
sudo chmod a+r /etc/apt/keyrings/docker-archive-keyring.gpg

# 6.3: Add Docker repository to apt sources
echo "Adding Docker repository..."
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker-archive-keyring.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# 6.4: Install Docker packages
echo "Installing Docker Engine and plugins..."
sudo apt-get update -y
# docker-ce: Docker Community Edition engine
# docker-ce-cli: Docker command-line interface
# containerd.io: Container runtime
# docker-buildx-plugin: Extended build capabilities
# docker-compose-plugin: Docker Compose V2
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# 6.5: Enable and start Docker service
echo "Starting Docker daemon..."
sudo systemctl enable docker
sudo systemctl start docker

# 6.6: Verify Docker installation
echo "Verifying Docker installation..."
sudo docker run hello-world || echo "Warning: hello-world test failed"

# 6.7: Add current user to docker group
# This allows running Docker commands without sudo
echo "Adding user '${USER}' to docker group..."
if ! id -nG "$USER" | grep -qw docker; then
  sudo usermod -aG docker "$USER"
  echo "✓ User added to docker group"
  echo "  Note: Log out and back in for group membership to take effect"
  ADDED_TO_GROUP=1
else
  echo "✓ User already in docker group"
  ADDED_TO_GROUP=0
fi

# ============================================================
# Step 7: Download and Install Apache Flink
# ============================================================

echo ""
echo "Step 7: Installing Apache Flink ${FLINK_VERSION}..."
echo "----------------------------------------------------"

FLINK_URL="https://dlcdn.apache.org/flink/flink-${FLINK_VERSION}/${FLINK_TGZ}"

# Navigate to parent directory where Flink will be installed
cd "${PARENT_DIR}"

if [ -d "${FLINK_DIR}" ]; then
  echo "✓ Flink ${FLINK_VERSION} already exists at: ${FLINK_DIR}"
  echo "  Skipping download"
else
  echo "Downloading Flink from: ${FLINK_URL}"
  curl -fLO "${FLINK_URL}"

  echo "Extracting Flink archive..."
  tar -xzf "${FLINK_TGZ}"

  echo "Cleaning up archive..."
  rm -f "${FLINK_TGZ}"

  echo "✓ Flink installed to: ${FLINK_DIR}"
fi

# ============================================================
# Step 8: Move FlinkSketch into Flink Directory
# ============================================================

echo ""
echo "Step 8: Moving FlinkSketch into Flink directory..."
echo "---------------------------------------------------"

# FlinkSketch should reside inside the Flink installation directory
FLINKSKETCH_TARGET="${FLINK_DIR}/FlinkSketch"

if [ -d "${FLINKSKETCH_TARGET}" ]; then
  echo "✓ FlinkSketch already exists at: ${FLINKSKETCH_TARGET}"
else
  echo "Moving FlinkSketch..."
  echo "  From: ${FLINKSKETCH_ROOT}"
  echo "  To:   ${FLINKSKETCH_TARGET}"

  mv "${FLINKSKETCH_ROOT}" "${FLINKSKETCH_TARGET}" || {
    echo "Warning: Could not move FlinkSketch (may already be in place)"
  }
fi

# ============================================================
# Step 9: Set Environment Variables
# ============================================================

echo ""
echo "Step 9: Setting environment variables..."
echo "-----------------------------------------"

# FLINK_HOME: Points to Flink installation directory
# PATH: Add Flink binaries to PATH for easy access
export FLINK_HOME="${FLINK_DIR}"
export PATH="${FLINK_HOME}/bin:${PATH}"

echo "✓ FLINK_HOME=${FLINK_HOME}"
echo "✓ PATH includes: ${FLINK_HOME}/bin"

# ============================================================
# Step 10: Compile FlinkSketch Maven Project
# ============================================================

echo ""
echo "Step 10: Compiling FlinkSketch Maven project..."
echo "------------------------------------------------"

if [ -d "${FLINKSKETCH_TARGET}" ]; then
  echo "Compiling project at: ${FLINKSKETCH_TARGET}"
  cd "${FLINKSKETCH_TARGET}"

  # mvn clean: Remove previous build artifacts
  # mvn install: Compile, test, and package the project
  mvn clean install

  echo "✓ Maven build complete"
  echo "  JAR location: ${FLINKSKETCH_TARGET}/target/flinksketch-0.1.jar"
else
  echo "Warning: FlinkSketch not found at ${FLINKSKETCH_TARGET}"
  echo "         Skipping Maven build"
fi

# ============================================================
# Summary
# ============================================================

echo ""
echo "============================================================"
echo "Environment Setup Complete!"
echo "============================================================"
echo ""
echo "Installed components:"
echo "  ✓ Python3 with development tools"
echo "  ✓ OpenJDK 11"
echo "  ✓ Maven"
echo "  ✓ uv (Python package manager)"
echo "  ✓ Docker Engine"
echo "  ✓ Apache Flink ${FLINK_VERSION}"
echo ""
echo "Project locations:"
echo "  FlinkSketch: ${FLINKSKETCH_TARGET}"
echo "  Flink:       ${FLINK_DIR}"
echo ""
echo "Environment variables (current session only):"
echo "  FLINK_HOME=${FLINK_HOME}"
echo ""
echo "To make environment variables permanent, add to ~/.bashrc:"
echo "  export FLINK_HOME=\"${FLINK_DIR}\""
echo "  export PATH=\"\${FLINK_HOME}/bin:\${PATH}\""
echo ""

if [ "${ADDED_TO_GROUP:-0}" -eq 1 ]; then
  echo "IMPORTANT:"
  echo "  You were added to the 'docker' group."
  echo "  Please log out and back in for this to take effect."
  echo "  Until then, you'll need to use 'sudo' with docker commands."
  echo ""
fi

echo "Setup completed successfully!"
