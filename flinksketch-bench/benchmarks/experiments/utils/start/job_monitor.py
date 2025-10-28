#!/usr/bin/env python3
#
# Copyright 2025 ProjectASAP contributors
# SPDX-License-Identifier: Apache-2.0
#

"""
Flink Job Monitor - Sidecar monitoring script for Flink jobs
Monitors all jobs and vertices and collects numRecordsInPerSecond metrics
"""

import requests
import time
import os
import subprocess
import json
import psutil
from typing import List, Optional, Dict, Tuple
from dataclasses import dataclass
from tqdm import tqdm


@dataclass
class JobInfo:
    job_id: str
    state: str


@dataclass
class MetricData:
    job_id: str
    vertex_id: str
    vertex_name: str
    num_records_in_per_second: float
    num_records_out_per_second: float
    cpu_usage_percent: float
    thread_cpu_usages: Dict[int, float]
    backpressure_ratio: float
    jvm_heap_used_mb: float
    timestamp: float


class FlinkJobMonitor:
    def __init__(
        self,
        host: str,
        port: int,
        interval: int,
        output_dir: str,
        timer: int,
        flink_bin: str,
        log_file_name: str,
        container_service,
        architecture: str,
        parallelism: int = 1,
    ):
        self.host = host
        self.port = port
        self.interval = interval
        self.output_dir = output_dir
        self.timer = timer
        self.flink_bin_path = flink_bin
        self.log_file_name = log_file_name
        self.container_service = container_service
        self.architecture = architecture
        self.parallelism = parallelism

        self.base_url = f"http://{host}:{port}"
        self.session = requests.Session()
        self._prev_cpu_times: Dict[int, dict] = {}

        # Set container paths for async-profiler binaries based on architecture
        if architecture == "arm64":
            self.container_asprof_bin = (
                "/opt/async-profiler/async-profiler-4.1-linux-arm64/bin/asprof"
            )
            self.container_jfrconv_bin = (
                "/opt/async-profiler/async-profiler-4.1-linux-arm64/bin/jfrconv"
            )
        else:  # Default to x86_64
            self.container_asprof_bin = (
                "/opt/async-profiler/async-profiler-4.1-linux-x64/bin/asprof"
            )
            self.container_jfrconv_bin = (
                "/opt/async-profiler/async-profiler-4.1-linux-x64/bin/jfrconv"
            )

        self.profile_output_file = os.path.join(self.output_dir, "cpu_profile.jfr")
        self.flamegraph_output_file = os.path.join(
            self.output_dir, "cpu_flamegraph.html"
        )
        self.profiling_duration = 30  # Duration in seconds

    def get_all_jobs(self) -> List[JobInfo]:
        """Get all Flink jobs"""
        response = self.session.get(f"{self.base_url}/jobs")
        response.raise_for_status()

        jobs_data = response.json()
        jobs = []

        for job in jobs_data["jobs"]:
            jobs.append(JobInfo(job_id=job["id"], state=job["status"]))

        return jobs

    def start_profiling_flink_pids(self, flink_pids: List[int]):
        """Start async-profiler for given Flink PIDs using docker exec"""

        # Build command to start profiling for all PIDs inside container
        cmd_parts = []
        for pid in flink_pids:
            docker_exec_cmd = f"docker exec flink-taskmanager {self.container_asprof_bin} -e cpu -f {self.profile_output_file} start {pid}"
            cmd_parts.append(docker_exec_cmd)

        # Make directory writable by all
        perm_cmd = f"docker exec flink-taskmanager chmod -R 777 {os.path.dirname(self.profile_output_file)}"
        subprocess.run(perm_cmd, shell=True, capture_output=True, text=True)

        cmd = ";".join(cmd_parts)
        print(f"Starting profiling for flink pids with command: {cmd}")

        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            print("ERROR: Failed to start profiling")
            print(f"  Return code: {result.returncode}")
            print(f"  Stderr: {result.stderr}")
            print(f"  Stdout: {result.stdout}")
        else:
            print("Profiling started successfully")
            if result.stdout:
                print(f"  Output: {result.stdout}")

    def stop_and_generate_flamegraph(self, pid: int):
        """Stop profiling and generate JFR output, then convert to flamegraph"""
        # Stop profiling (JFR file already declared at start)
        stop_cmd = (
            f"docker exec flink-taskmanager {self.container_asprof_bin} stop {pid}"
        )

        print(f"Stopping profiling and generating JFR for PID {pid}")
        result = subprocess.run(stop_cmd, shell=True, capture_output=True, text=True)

        # Convert JFR to flamegraph using jfrconv inside container
        convert_cmd = f"docker exec flink-taskmanager {self.container_jfrconv_bin} --cpu -o html {self.profile_output_file} {self.flamegraph_output_file}"

        print("Converting JFR to flamegraph")
        result = subprocess.run(convert_cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"CPU flamegraph saved to: {self.flamegraph_output_file}")

    def get_taskmanager_pid(self, internal: bool = False) -> int:
        """Get TaskManager process PID

        Args:
            internal: If True, get PID from inside container.
                      If False, get host PID.
        """
        if internal:
            # Get PID from inside the TaskManager container for async-profiler
            result = subprocess.run(
                [
                    "docker",
                    "exec",
                    "flink-taskmanager",
                    "pgrep",
                    "-f",
                    "TaskManagerRunner",
                ],
                capture_output=True,
                text=True,
            )
        else:
            # Get host PID for CPU monitoring
            result = subprocess.run(
                ["pgrep", "-f", "TaskManagerRunner"], capture_output=True, text=True
            )

        return int(result.stdout.strip().split("\n")[0])

    def get_jvm_heap_memory(self) -> float:
        """Get JVM heap memory usage from TaskManager

        Returns:
            Current heap usage in MB
        """
        try:
            # Get TaskManager ID from Flink REST API
            response = self.session.get(f"{self.base_url}/taskmanagers")
            response.raise_for_status()
            tm_data = response.json()

            if not tm_data.get("taskmanagers"):
                return 0.0

            tm_id = tm_data["taskmanagers"][0]["id"]

            # Get memory metrics from TaskManager
            metrics_response = self.session.get(
                f"{self.base_url}/taskmanagers/{tm_id}/metrics?get=Status.JVM.Memory.Heap.Used"
            )
            metrics_response.raise_for_status()
            metrics = metrics_response.json()

            heap_used = 0.0

            for metric in metrics:
                if "Heap.Used" in metric.get("id", ""):
                    heap_used = float(metric.get("value", 0)) / (
                        1024 * 1024
                    )  # Convert bytes to MB
                    break

            return heap_used

        except Exception as e:
            print(f"Warning: Failed to get JVM heap memory: {e}")
            return 0.0

    def get_process_and_threads_cpu(
        self, pid: int
    ) -> Optional[Tuple[float, Dict[int, float]]]:
        """
        Calculates CPU usage for a process and its threads.

        On the first call for a given PID, it stores the current times and returns None.
        On subsequent calls, it returns a tuple containing:
        - Process CPU usage percentage (can be > 100% on multi-core systems).
        - A dictionary mapping thread IDs to their CPU usage percentage.

        Returns None if the process is not found or on the first successful call.
        """
        process = psutil.Process(pid)
        now = time.monotonic()

        # Get process and thread times
        process_times = process.cpu_times()
        current_process_time = process_times.user + process_times.system
        current_thread_times = {
            t.id: t.user_time + t.system_time for t in process.threads()
        }

        # Get previous state
        previous_state = self._prev_cpu_times.get(pid)

        # Store current state for next call
        self._prev_cpu_times[pid] = {
            "timestamp": now,
            "process_time": current_process_time,
            "thread_times": current_thread_times,
        }

        if not previous_state:
            # First call for this PID, can't calculate usage yet
            return None

        # Calculate time delta
        time_delta = now - previous_state["timestamp"]
        if time_delta <= 0:
            return None

        # Calculate process CPU usage
        process_time_delta = current_process_time - previous_state["process_time"]
        process_cpu_percent = (process_time_delta / time_delta) * 100

        # Calculate thread CPU usage
        thread_cpu_percent = {}
        prev_thread_times = previous_state["thread_times"]
        for tid, current_thread_time in current_thread_times.items():
            # Use 0 for new threads, so their usage is calculated from 0.
            prev_thread_time = prev_thread_times.get(tid, 0.0)
            thread_time_delta = current_thread_time - prev_thread_time
            thread_cpu_percent[tid] = (thread_time_delta / time_delta) * 100

        return process_cpu_percent, thread_cpu_percent

    def get_job_with_metrics(
        self,
        job_id: str,
        tm_cpu_usage: float,
        thread_usages: Dict[int, float],
        heap_used_mb: float,
    ) -> List[MetricData]:
        """Get job details with vertex metrics, collecting from all parallel subtasks"""
        response = self.session.get(f"{self.base_url}/jobs/{job_id}")
        response.raise_for_status()

        job_data = response.json()
        metrics = []
        timestamp = time.time()

        for vertex in job_data["vertices"]:
            # Sum metrics across all parallel subtasks (0 to parallelism-1)
            total_records_in_per_sec = 0.0
            total_records_out_per_sec = 0.0
            total_backpressure_ratio = 0.0

            for subtask_id in range(self.parallelism):
                # Get real-time metrics from Flink for each subtask
                metric_response = self.session.get(
                    f"{self.base_url}/jobs/{job_id}/vertices/{vertex['id']}/metrics?get={subtask_id}.numRecordsInPerSecond,{subtask_id}.numRecordsOutPerSecond,{subtask_id}.backPressuredTimeMsPerSecond,{subtask_id}.idleTimeMsPerSecond,{subtask_id}.busyTimeMsPerSecond"
                )
                metric_response.raise_for_status()

                metric_data = metric_response.json()

                # Extract metrics for this subtask
                backpressured_time = 0.0
                idle_time = 0.0
                busy_time = 0.0

                for metric in metric_data:
                    metric_id = metric.get("id", "")
                    if "numRecordsInPerSecond" in metric_id:
                        total_records_in_per_sec += float(metric.get("value", 0))
                    elif "numRecordsOutPerSecond" in metric_id:
                        total_records_out_per_sec += float(metric.get("value", 0))
                    elif "backPressuredTimeMsPerSecond" in metric_id:
                        backpressured_time = float(metric.get("value", 0))
                    elif "idleTimeMsPerSecond" in metric_id:
                        idle_time = float(metric.get("value", 0))
                    elif "busyTimeMsPerSecond" in metric_id:
                        busy_time = float(metric.get("value", 0))

                # Calculate backpressure ratio for this subtask
                total_time = backpressured_time + idle_time + busy_time
                if total_time > 0:
                    total_backpressure_ratio += backpressured_time / total_time

            # Average backpressure ratio across all subtasks
            avg_backpressure_ratio = (
                total_backpressure_ratio / self.parallelism
                if self.parallelism > 0
                else 0.0
            )

            metrics.append(
                MetricData(
                    job_id=job_id,
                    vertex_id=vertex["id"],
                    vertex_name=vertex["name"],
                    num_records_in_per_second=total_records_in_per_sec,
                    num_records_out_per_second=total_records_out_per_sec,
                    cpu_usage_percent=tm_cpu_usage,
                    thread_cpu_usages=thread_usages,
                    backpressure_ratio=avg_backpressure_ratio,
                    jvm_heap_used_mb=heap_used_mb,
                    timestamp=timestamp,
                )
            )

        return metrics

    def collect_all_metrics(self) -> List[MetricData]:
        """Collect numRecordsInPerSecond metrics for all jobs and vertices"""
        all_metrics = []

        jobs = self.get_all_jobs()

        # Get TaskManager CPU usage once for all jobs
        tm_pid = self.get_taskmanager_pid()
        tm_cpu_usage = 0.0
        thread_usages: Dict[int, float] = {}
        if tm_pid:
            cpu_data = self.get_process_and_threads_cpu(tm_pid)
            if cpu_data:
                tm_cpu_usage, thread_usages = cpu_data

        # Get JVM heap memory usage once for all jobs
        heap_used_mb = self.get_jvm_heap_memory()

        for job in jobs:
            if job.state not in ["RUNNING"]:
                continue

            # Get metrics for all vertices in this job
            job_metrics = self.get_job_with_metrics(
                job.job_id, tm_cpu_usage, thread_usages, heap_used_mb
            )
            all_metrics.extend(job_metrics)

        return all_metrics

    def monitor_continuously(self):
        """Continuously monitor metrics at specified intervals"""

        log_dir = os.path.join(self.output_dir)
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, self.log_file_name)

        # Clear the file at start
        with open(log_file, "w") as f:
            f.write("")

        start_time = time.time()

        # Initial call to populate CPU times, allowing first real call to have data
        tm_pid = self.get_taskmanager_pid()  # Host PID for CPU monitoring
        tm_internal_pid = self.get_taskmanager_pid(
            internal=True
        )  # Container PID for profiling

        self.get_process_and_threads_cpu(tm_pid)

        # Start CPU profiling for the entire container lifecycle using internal PID
        self.start_profiling_flink_pids([tm_internal_pid])
        print(
            f"Started async-profiler CPU profiling for TaskManager PID: {tm_internal_pid} (container)"
        )

        # Initialize progress bar
        pbar = tqdm(
            total=self.timer,
            desc="Monitoring",
            unit="s",
            bar_format="{l_bar}{bar}| {elapsed}s/{total}s [{percentage:3.0f}%]",
        )

        while True:
            current_time = time.time()
            elapsed = current_time - start_time

            metrics = self.collect_all_metrics()
            collection_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

            # Write each metric as a separate JSON object (JSONL format)
            with open(log_file, "a") as f:
                for metric in metrics:
                    metric_json = {
                        "collection_timestamp": collection_timestamp,
                        "job_id": metric.job_id,
                        "vertex_id": metric.vertex_id,
                        "vertex_name": metric.vertex_name,
                        "num_records_in_per_second": metric.num_records_in_per_second,
                        "num_records_out_per_second": metric.num_records_out_per_second,
                        "cpu_usage_percent": metric.cpu_usage_percent,
                        "thread_cpu_usages": metric.thread_cpu_usages,
                        "backpressure_ratio": metric.backpressure_ratio,
                        "jvm_heap_used_mb": metric.jvm_heap_used_mb,
                        "metric_timestamp": metric.timestamp,
                    }
                    f.write(json.dumps(metric_json) + "\n")

                if not metrics:
                    no_metrics_json = {
                        "collection_timestamp": collection_timestamp,
                        "message": "No metrics collected (no running jobs found)",
                    }
                    f.write(json.dumps(no_metrics_json) + "\n")

            # Update progress bar with elapsed time
            pbar.n = min(int(elapsed), self.timer)
            pbar.refresh()

            # Check timer
            if elapsed >= self.timer:
                pbar.close()
                print("Monitoring complete!")

                self.stop_all_jobs()
                time.sleep(5)  # wait for jobs to finish
                # Generate CPU flamegraph for the monitored period using internal PID
                print("Generating CPU flamegraph...")
                self.stop_and_generate_flamegraph(tm_internal_pid)
                time.sleep(5)
                self.stop_container()
                break

            time.sleep(self.interval)

    def stop_all_jobs(self) -> None:
        """Stop all running Flink jobs."""
        jobs = self.get_all_jobs()
        for job in jobs:
            if job.state == "RUNNING":
                subprocess.run([self.flink_bin_path, "cancel", job.job_id], check=True)

    def stop_container(self) -> None:
        """Stop Flink cluster Docker containers and remove network using FlinkClusterService."""
        self.container_service.stop()
