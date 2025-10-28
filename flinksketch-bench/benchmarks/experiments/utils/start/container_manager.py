#
# Copyright 2025 ProjectASAP contributors
# SPDX-License-Identifier: Apache-2.0
#

"""
Docker-based Flink cluster service management for testing.
"""

import os
import subprocess
import time


class FlinkClusterService:
    """Docker-based Flink cluster service with resource constraints."""

    def __init__(self):
        """
        Initialize Docker Flink cluster service for local execution.
        """
        self.jobmanager_container = "flink-jobmanager"
        self.taskmanager_container = "flink-taskmanager"
        self.network_name = "flink-network"

    def get_container_name(self) -> str:
        """Get the JobManager Docker container name."""
        return self.jobmanager_container

    def get_flink_web_ui_url(self) -> str:
        """Get Flink Web UI URL."""
        return "http://localhost:8081"

    def start(
        self,
        jobmanager_cpu: float,
        jobmanager_memory: str,
        taskmanager_cpu: float,
        taskmanager_memory: str,
        experiment_output_dir: str,
        jar_path: str,
        output_file_path: str,
        config_file_path: str,
        output_format: str,
        verbose: bool,
        pipeline: str,
        output_mode: str,
        datagen_key_cardinality: int,
        datagen_items_per_window: int,
        query_keys: list,
        query_ranks: list,
        flink_root: str,
        query_name: str = None,
        taskmanager_slots: int = None,
        parallelism: int = 1,
        enable_merging: bool = True,
        distribution: str = "uniform",
    ) -> None:
        """
        Start Flink cluster (JobManager + TaskManager) in Docker containers with resource limits,
        then submit the Flink job.

        Args:
            jobmanager_cpu: Number of CPUs for JobManager (e.g., 1.0)
            jobmanager_memory: Memory limit for JobManager (e.g., "1g")
            taskmanager_cpu: Number of CPUs for TaskManager (e.g., 3.0)
            taskmanager_memory: Memory limit for TaskManager (e.g., "7g")
            experiment_output_dir: Directory for Flink logs and data
            jar_path: Path to the Flink job JAR file relative to flink_root
            output_file_path: Path where job output will be written
            config_file_path: Path to the aggregation config YAML file
            output_format: Output format (e.g., "json")
            verbose: Enable verbose output to sink
            pipeline: Pipeline type (insertion or insertion_querying)
            output_mode: Output mode - can be single flag or multiple flags separated by underscore
                        (e.g., sketch, query, memory, query_memory, sketch_query, sketch_query_memory)
                        Note: query flag only available for insertion_querying pipeline
            datagen_key_cardinality: Number of distinct keys to generate
            flink_root: Root directory of Flink installation
            taskmanager_slots: Number of task slots per TaskManager (default: None, uses Flink default)
            parallelism: Parallelism for the Flink job (default: 1)
        """

        # Stop any existing containers to avoid conflicts
        self.stop()

        # Create debug log file for container output
        self.debug_log_path = os.path.join(experiment_output_dir, "debug_logs.log")

        # Initialize debug log with timestamp header
        with open(self.debug_log_path, "w") as debug_log:
            import datetime

            debug_log.write(
                f"=== Experiment Debug Log - {datetime.datetime.now()} ===\n"
            )
            debug_log.write(f"Experiment Output Directory: {experiment_output_dir}\n")
            debug_log.write(f"JAR Path: {jar_path}\n")
            debug_log.write(f"Config Path: {config_file_path}\n")
            debug_log.write(f"Pipeline: {pipeline}\n")
            debug_log.write(f"Output Mode: {output_mode}\n")
            debug_log.write("=" * 60 + "\n\n")

        # Create Flink data directory for logs and checkpoints
        flink_data_dir = os.path.join(experiment_output_dir, "flink_data")
        os.makedirs(flink_data_dir, exist_ok=True)

        # Create output directory and ensure it exists
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

        # Set proper permissions for Flink container user
        # Dynamically detect the Flink user UID from the container
        uid_result = subprocess.run(
            "docker run --rm flink:1.20.2-scala_2.12 id -u",
            shell=True,
            capture_output=True,
            text=True,
            check=True,
        )
        flink_uid = int(uid_result.stdout.strip())

        # Set ownership to Flink user for container access
        subprocess.run(
            f"sudo chown -R {flink_uid}:{flink_uid} {flink_data_dir}",
            shell=True,
            check=True,
        )

        subprocess.run(
            f"sudo chmod -R 777 {experiment_output_dir}", shell=True, check=True
        )
        subprocess.run(f"sudo chmod -R 777 {flink_data_dir}", shell=True, check=True)

        # Also ensure the Flink user can access the directories by setting ownership
        subprocess.run(
            f"sudo chown -R {flink_uid}:{flink_uid} {experiment_output_dir}",
            shell=True,
            check=True,
        )

        # Create Docker network for container communication (ignore if exists)
        network_cmd = f"docker network create {self.network_name} || true"
        with open(self.debug_log_path, "a") as debug_log:
            debug_log.write("=== Creating Docker Network ===\n")
            debug_log.write(f"{network_cmd}\n")
            debug_log.flush()
            result = subprocess.run(
                network_cmd, shell=True, capture_output=True, text=True, check=True
            )
            debug_log.write(f"Network creation stdout: {result.stdout}\n")
            debug_log.write(f"Network creation stderr: {result.stderr}\n")

        # Configure JobManager memory properties
        jobmanager_props = (
            f"jobmanager.rpc.address: {self.jobmanager_container}\n"
            f"jobmanager.memory.process.size: {jobmanager_memory}"
        )

        jobmanager_cmd = (
            f"docker run -d --rm --name {self.jobmanager_container} "
            f"--network {self.network_name} "
            f"--cpus={jobmanager_cpu} --memory={jobmanager_memory} "
            f"--publish 8081:8081 "
            f"-v {flink_data_dir}:/opt/flink/log "
            f"-v {experiment_output_dir}:{experiment_output_dir} "
            f'--env FLINK_PROPERTIES="{jobmanager_props}" '
            f"flink:1.20.2-scala_2.12 jobmanager"
        )

        # Start JobManager container with resource constraints
        with open(self.debug_log_path, "a") as debug_log:
            debug_log.write("=== Starting JobManager Container ===\n")
            debug_log.write(f"{jobmanager_cmd}\n")
            debug_log.flush()
            result = subprocess.run(
                jobmanager_cmd, shell=True, capture_output=True, text=True, check=True
            )
            debug_log.write(f"JobManager stdout: {result.stdout}\n")
            debug_log.write(f"JobManager stderr: {result.stderr}\n")

        # Get async-profiler tools directory to mount
        script_dir = os.path.dirname(os.path.abspath(__file__))
        async_profiler_dir = os.path.join(
            script_dir, "..", "..", "tools", "async-profiler"
        )

        flink_props = f"jobmanager.rpc.address: {self.jobmanager_container}\n"

        # Calculate container memory as multiplier * JVM heap memory
        # This multiplier accounts for JVM overhead, metaspace, and other non-heap memory
        memory_multiplier = 2

        # Parse memory value (e.g., "2g" -> 2) and calculate container memory
        memory_value = taskmanager_memory.lower()
        if memory_value.endswith("g"):
            heap_gb = float(memory_value[:-1])
            container_gb = heap_gb * memory_multiplier
            container_memory = f"{int(container_gb)}g"
        elif memory_value.endswith("m"):
            heap_mb = float(memory_value[:-1])
            container_mb = heap_mb * memory_multiplier
            container_memory = f"{int(container_mb)}m"
        else:
            raise ValueError(f"Invalid memory format: {taskmanager_memory}")

        # Set JVM heap directly with -Xmx and -Xms via environment variable
        # This bypasses all Flink memory management
        jvm_opts = f"-Xmx{taskmanager_memory} -Xms{taskmanager_memory}"

        # Print memory configuration
        print("\n=== TaskManager Memory Configuration ===")
        print(f"JVM Heap (-Xmx/-Xms): {taskmanager_memory}")
        print(f"Docker Container Memory: {container_memory}")
        print(f"Memory Multiplier: {memory_multiplier}x")
        print("=========================================\n")

        # Add task slots configuration if specified
        if taskmanager_slots is not None:
            flink_props += f"taskmanager.numberOfTaskSlots: {taskmanager_slots}\n"

        taskmanager_cmd = (
            f"docker run -d --rm --name {self.taskmanager_container} "
            f"--network {self.network_name} "
            f"--cpus={taskmanager_cpu} --memory={container_memory} "
            f"--cap-add SYS_ADMIN --cap-add SYS_PTRACE "
            f"-v {flink_data_dir}:/opt/flink/log "
            f"-v {experiment_output_dir}:{experiment_output_dir} "
            f"-v {async_profiler_dir}:/opt/async-profiler "
            f'--env FLINK_PROPERTIES="{flink_props}" '
            f'--env FLINK_ENV_JAVA_OPTS="{jvm_opts}" '
            f"flink:1.20.2-scala_2.12 taskmanager"
        )

        # Start TaskManager container with resource constraints
        with open(self.debug_log_path, "a") as debug_log:
            debug_log.write("=== Starting TaskManager Container ===\n")
            debug_log.write(f"{taskmanager_cmd}\n")
            debug_log.flush()
            result = subprocess.run(
                taskmanager_cmd, shell=True, capture_output=True, text=True, check=True
            )
            debug_log.write(f"TaskManager stdout: {result.stdout}\n")
            debug_log.write(f"TaskManager stderr: {result.stderr}\n")

        # Wait for cluster initialization before job submission
        time.sleep(10)

        self._submit_flink_job(
            jar_path=jar_path,
            output_file_path=output_file_path,
            config_file_path=config_file_path,
            output_format=output_format,
            verbose=verbose,
            pipeline=pipeline,
            output_mode=output_mode,
            datagen_key_cardinality=datagen_key_cardinality,
            datagen_items_per_window=datagen_items_per_window,
            query_keys=query_keys,
            query_ranks=query_ranks,
            query_name=query_name,
            flink_root=flink_root,
            parallelism=parallelism,
            enable_merging=enable_merging,
            distribution=distribution,
        )

    def _submit_flink_job(
        self,
        jar_path: str,
        output_file_path: str,
        config_file_path: str,
        output_format: str,
        verbose: bool,
        pipeline: str,
        output_mode: str,
        datagen_key_cardinality: int,
        datagen_items_per_window: int,
        query_keys: list,
        query_ranks: list,
        flink_root: str,
        query_name: str = None,
        parallelism: int = 1,
        enable_merging: bool = True,
        distribution: str = "uniform",
    ) -> None:
        """
        Submit the Flink job to the running cluster.

        Args:
            jar_path: Path to the Flink job JAR file relative to flink_root
            output_file_path: Path where job output will be written
            config_file_path: Path to the aggregation config YAML file
            output_format: Output format (e.g., "json")
            verbose: Enable verbose output to sink
            pipeline: Pipeline type (insertion or insertion_querying)
            output_mode: Output mode - can be single flag or multiple flags separated by underscore
                        (e.g., sketch, query, memory, query_memory, sketch_query, sketch_query_memory)
                        Note: query flag only available for insertion_querying pipeline
            datagen_key_cardinality: Number of distinct keys to generate
            flink_root: Root directory of Flink installation
            parallelism: Parallelism for the Flink job
        """
        # Build Flink job submission command with all required parameters
        keys_param = ",".join(map(str, query_keys)) if query_keys else ""
        ranks_param = ",".join(map(str, query_ranks)) if query_ranks else ""

        flink_run_cmd = (
            f"cd {flink_root} && "
            f"./bin/flink run {jar_path} "
            f"--outputFilePath {output_file_path} "
            f"--configFilePath {config_file_path} "
            f"--outputFormat {output_format} "
            f"--verbose {str(verbose).lower()} "
            f"--pipeline {pipeline} "
            f"--outputMode {output_mode} "
            f"--datagenKeyCardinality {datagen_key_cardinality} "
            f"--datagenItemsPerWindow {datagen_items_per_window} "
            f"--parallelism {parallelism} "
            f"--enableMerging {str(enable_merging).lower()} "
            f"--distribution {distribution}"
        )

        # Only add --queryKeys if keys are provided
        if keys_param:
            flink_run_cmd += f" --queryKeys {keys_param}"

        # Only add --queryRanks if ranks are provided
        if ranks_param:
            flink_run_cmd += f" --queryRanks {ranks_param}"

        # Only add --queryName if query name is provided (also used as statistic for Univmon)
        if query_name:
            flink_run_cmd += f" --queryName {query_name}"

        print(flink_run_cmd)

        # Execute Flink job submission command in background (non-blocking)
        # Redirect output to debug log file
        with open(self.debug_log_path, "a") as debug_log:
            debug_log.write("=== Flink Job Submission Command ===\n")
            debug_log.write(f"{flink_run_cmd}\n")
            debug_log.write("=== Flink Job Output ===\n")
            debug_log.flush()
            subprocess.Popen(
                flink_run_cmd, shell=True, stdout=debug_log, stderr=debug_log
            )

    def is_job_running(
        self, flink_root: str = "/Users/songtingwang/Desktop/Sketch/FlinkSketch/"
    ) -> bool:
        """
        Check if any Flink job is currently running in the cluster.

        Args:
            flink_root: Root directory of Flink installation

        Returns:
            True if at least one job is running, False otherwise
        """
        # Use Flink CLI to list all jobs
        list_jobs_cmd = f"cd {flink_root} && ./bin/flink list"
        result = subprocess.run(
            list_jobs_cmd, shell=True, capture_output=True, text=True, check=True
        )

        # Check if output contains running jobs (excluding the header lines)
        output_lines = result.stdout.strip().split("\n")
        for line in output_lines:
            if "RUNNING" in line:
                return True
        return False

    def get_running_jobs(
        self, flink_root: str = "/Users/songtingwang/Desktop/Sketch/FlinkSketch/"
    ) -> list:
        """
        Get list of running job IDs and names.

        Args:
            flink_root: Root directory of Flink installation

        Returns:
            List of dictionaries containing job information
        """
        # Use Flink CLI to list all jobs
        list_jobs_cmd = f"cd {flink_root} && ./bin/flink list"
        result = subprocess.run(
            list_jobs_cmd, shell=True, capture_output=True, text=True, check=True
        )

        running_jobs = []
        output_lines = result.stdout.strip().split("\n")

        for line in output_lines:
            if "RUNNING" in line:
                # Parse job line format: "job_id : job_name (RUNNING)"
                parts = line.split(" : ")
                job_id = parts[0].strip()
                job_name_status = parts[1].strip()
                job_name = job_name_status.replace(" (RUNNING)", "")
                running_jobs.append(
                    {"job_id": job_id, "job_name": job_name, "status": "RUNNING"}
                )

        return running_jobs

    def stop(self) -> None:
        """
        Stop Flink cluster Docker containers and remove network.
        Containers auto-remove due to --rm flag.
        """
        # Stop TaskManager if running
        self._stop_container_and_wait(self.taskmanager_container)

        # Stop JobManager if running
        self._stop_container_and_wait(self.jobmanager_container)

        # Remove Docker network if it exists
        self._remove_network_and_wait(self.network_name)

    def _exists(self, cmd_args) -> bool:
        """Return True if the docker ps/ls query returns at least one id."""
        res = subprocess.run(cmd_args, capture_output=True, text=True, check=True)
        return bool(res.stdout.strip())

    def _stop_container_and_wait(self, name: str) -> None:
        """Stop a Docker container by name and wait until it's gone from `docker ps`."""
        check_cmd = ["docker", "ps", "-q", "-f", f"name={name}"]
        if self._exists(check_cmd):
            print(f"Stopping {name}")
            subprocess.run(["docker", "stop", name], check=True)
            # wait until container is no longer listed
            while True:
                if not self._exists(check_cmd):
                    break
                time.sleep(0.5)

    def _remove_network_and_wait(self, network: str) -> None:
        """Remove a Docker network by name and wait until it's gone from `docker network ls`."""
        check_cmd = ["docker", "network", "ls", "-q", "-f", f"name={network}"]
        if self._exists(check_cmd):
            print(f"Removing network {network}")
            subprocess.run(["docker", "network", "rm", network], check=True)
            # wait until network is no longer listed
            while True:
                if not self._exists(check_cmd):
                    break
                time.sleep(0.5)
