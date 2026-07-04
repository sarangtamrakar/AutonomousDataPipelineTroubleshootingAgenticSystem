"""LangGraph @tool wrappers for the RCA agent.

Covers: Airflow logs, Spark History metrics, S3 event logs, local executor stderr.
"""

import os
import re
import glob
import json
import logging
from typing import List

import boto3
import requests
from langchain_core.tools import tool

from tools.airflow_client import create_default_client
from tools.s3_reader import iter_event_lines, parse_json_lines, find_executor_removed_events
from tools.spark_history_client import create_default_spark_client
from tools.lineage_tools import get_upstream_lineage_status, get_downstream_impact, check_asset_status
from tools.sql_tools import execute_athena_query

from config import cfg

logger = logging.getLogger(__name__)

_airflow = create_default_client()
_spark = create_default_spark_client()

AIRFLOW_AUTH = (cfg.AIRFLOW_USER, cfg.AIRFLOW_PASSWORD)
HOST_WORK_DIR = cfg.SPARK_WORKER_LOG_DIR


def _smart_truncate(text: str, max_lines: int = 60) -> str:
    """Keep top 15 + bottom 45 lines to stay within LLM context limits."""
    lines = text.splitlines()
    if len(lines) <= max_lines:
        return text
    return "\n".join(lines[:15] + ["\n... [TRUNCATED] ...\n"] + lines[-45:])


# ── Airflow ──────────────────────────────────────────────────────────────────

@tool
def fetch_airflow_driver_log(dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> str:
    """Use this tool FIRST when diagnosing a pipeline failure.
    Fetches stdout/stderr of the Airflow task (contains Spark Driver logs).
    Look for: Python/Java exceptions, Docker exit code 137 (OOM), and spark_application_id
    (format: app-YYYYMMDDHHMMSS-XXXX) which you'll need for other Spark tools."""
    url = (f"{_airflow.base_url}/dags/{dag_id}/dagRuns/{run_id}"
           f"/taskInstances/{task_id}/logs/{try_number}")
    try:
        resp = _airflow.session.get(url, headers={"Accept": "text/plain"}, timeout=10)
        resp.raise_for_status()
        log_text = resp.text

        # Extract Spark app ID
        app_match = re.search(r"(app-\d{14}-\d{4})", log_text)
        app_id_hint = (f"\n[DETECTED SPARK APP ID: {app_match.group(1)}]"
                       if app_match else "\n[COULD NOT DETECT SPARK APP ID]")

        # Keep only high-signal lines: errors, exceptions, OOM, exit codes
        signal_patterns = re.compile(
            r"(error|exception|traceback|killed|oom|exit code|137|failed|"
            r"caused by|java\.lang|py4j|spark app|applicationid|app-\d{14})",
            re.IGNORECASE,
        )
        signal_lines = [ln for ln in log_text.splitlines() if signal_patterns.search(ln)]

        # Hard cap: last 60 signal lines, each capped at 200 chars
        trimmed = "\n".join(ln[:200] for ln in signal_lines[-60:])
        return (trimmed or log_text[:1500]) + app_id_hint
    except Exception as e:
        return f"Error fetching Airflow log: {e}"


@tool
def airflow_list_dag_runs(dag_id: str, limit: int = 10) -> str:
    """List recent DAG runs for a dag_id. Use to find failed run_ids for further diagnosis."""
    data = _airflow.list_dag_runs(dag_id, limit=limit)
    runs = data.get("dag_runs", data)
    return json.dumps([
        {"dag_run_id": r["dag_run_id"], "airflow_pipeline_status": r["state"], "execution_date": r["execution_date"]}
        for r in runs
    ])


@tool
def airflow_get_task_instance(dag_id: str, dag_run_id: str, task_id: str) -> str:
    """Get state, duration, and try_number for an Airflow task instance."""
    data = _airflow.get_task_instance(dag_id, dag_run_id, task_id)
    return json.dumps({k: data[k] for k in
                       ("task_id", "state", "start_date", "end_date", "duration", "try_number")
                       if k in data})




@tool
def get_task_asset_urn(dag_id: str, run_id: str, task_id: str) -> str:
    """Fetches the target_asset_id from the Airflow task params."""
    # Fetch the actual task instance object first
    ti = _airflow.get_task_instance(dag_id, run_id, task_id)
    
    # Safely extract params from the response
    params = ti.get("params", {})
    return params.get("target_asset_id", f"urn:unknown:{task_id}")


# ── Spark History ─────────────────────────────────────────────────────────────

@tool
def spark_list_applications() -> str:
    """List Spark applications from the History Server with duration and completion status."""
    apps = _spark.list_applications()
    return json.dumps([
        {"id": a["id"], "name": a["name"],
         "duration": a["attempts"][0].get("duration") if a.get("attempts") else None,
         "completed": a["attempts"][0].get("completed") if a.get("attempts") else None}
        for a in apps
    ])


@tool
def analyze_spark_skew_and_spill(app_id: str) -> str:
    """Use to diagnose DATA SKEW and MEMORY/DISK SPILLS.
    Scans all stages; only returns FAILED stages or those with disk spill.
    If max executorRunTime > 5x median, declare Data Skew.
    If diskBytesSpilled > 0, declare Memory Pressure."""
    try:
        all_stages = _spark.get_stages(app_id)
        if not all_stages:
            return f"No stages found for {app_id}."

        results = []
        for stage in all_stages:
            if stage.get("status") == "FAILED" or stage.get("diskBytesSpilled", 0) > 0:
                analysis = _spark.analyze_stage_for_spill_and_skew(app_id, stage["stageId"])
                analysis["status"] = stage.get("status")
                analysis["name"] = stage.get("name")
                results.append(analysis)

        if not results:
            return "All stages completed with no detected disk spills or failures."
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error analyzing stages: {e}"


@tool
def get_spark_executor_health(app_id: str) -> str:
    """Use to diagnose EXECUTOR OOM errors.
    Returns peak OS/JVM memory per executor.
    If ProcessTreeJVMRSSMemory approaches the container limit, the container was killed by the OS.
    Recommend increasing spark.executor.memoryOverhead."""
    try:
        url = _spark._url(f"applications/{app_id}/executors")
        resp = _spark.session.get(url, timeout=_spark.timeout)
        resp.raise_for_status()
        executors = resp.json()
        # Keep only key memory fields to stay within token limits
        slim = [
            {k: e.get(k) for k in ("id", "hostPort", "isActive", "maxMemory",
                                    "memoryUsed", "peakMemoryMetrics", "failedTasks", "isBlacklisted")}
            for e in (executors if isinstance(executors, list) else [executors])
        ]
        return json.dumps(slim[:20], indent=2)  # cap at 20 executors
    except Exception as e:
        return f"Error fetching executor health: {e}"


@tool
def spark_get_stages(app_id: str) -> str:
    """List all stages for a Spark app with status, task counts, and executor runtime."""
    stages = _spark.get_stages(app_id)
    return json.dumps([
        {"stageId": s["stageId"], "status": s["status"], "numTasks": s["numTasks"],
         "numFailedTasks": s["numFailedTasks"], "executorRunTime": s.get("executorRunTime"),
         "diskBytesSpilled": s.get("diskBytesSpilled", 0)}
        for s in stages
    ])


# ── S3 / Spark event logs ─────────────────────────────────────────────────────

@tool
def fetch_s3_spark_events(app_id: str, bucket: str = "sarang-de", prefix: str = "spark-event-logs") -> str:
    """Use if executor died silently or History API returns empty.
    Reads raw SparkListener events from S3 filtered to critical events only.
    Look for SparkListenerExecutorRemoved to find exact exit reason (e.g. code 137 = OOM)."""
    s3 = boto3.client("s3")
    key = f"{prefix}/{app_id}"
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        critical = [
            line for line in body.splitlines()
            if "SparkListenerExecutorRemoved" in line or "SparkListenerTaskEnd" in line
        ]
        if not critical:
            return "No critical failure events found in S3 event log."
        return "\n".join(critical[-30:])
    except Exception as e:
        return f"Error fetching S3 events: {e}"


@tool
def s3_find_executor_removed_events(app_id: str) -> str:
    """Scan the Spark event log on S3 for all executor-removed events.
    Returns executor IDs and removal reasons (e.g. OOM, exit code 137).
    Uses SPARK_EVENT_LOGS_PATH from config as the base path."""
    base = cfg.SPARK_EVENT_LOGS_PATH.rstrip("/").replace("s3a://", "s3://")
    s3_path = f"{base}/{app_id}"
    try:
        lines = iter_event_lines(s3_path)
        events = parse_json_lines(lines)
        removed = find_executor_removed_events(events)
        return json.dumps([{"executor_id": r["executor_id"], "reason": r["reason"]} for r in removed])
    except Exception as e:
        return f"Error scanning executor events: {e}"


# ── Local worker logs ─────────────────────────────────────────────────────────

@tool
def fetch_local_executor_stderr(app_id: str) -> str:
    """Read physical worker stderr logs from the host filesystem.
    Contains Python print statements, warning logs, and Java stack traces from worker nodes.
    Set SPARK_WORKER_LOG_DIR env var to the volume-mounted log directory."""
    pattern = os.path.join(HOST_WORK_DIR, app_id, "*", "stderr")
    try:
        files = glob.glob(pattern)
        if not files:
            return "No local executor stderr files found. Check SPARK_WORKER_LOG_DIR."
        logs = {}
        for path in files:
            executor_id = path.split(os.sep)[-2]
            with open(path) as f:
                logs[f"Executor_{executor_id}"] = _smart_truncate(f.read(), max_lines=50)
        return json.dumps(logs, indent=2)
    except Exception as e:
        return f"Error reading stderr files: {e}"
    

# ----- Athena Executor ---------

@tool
def execute_athena_query_tool(sql_query:str) -> str:
    """it uses aws boto3 library to connect with athena & run sql query & fetch the results"""
    try:
        final_result = execute_athena_query(sql_query)
        return json.dumps(final_result)
    except Exception as e:
        return f"Error execute_athena_query_tool: {e}"
    


# ── Tool registry ─────────────────────────────────────────────────────────────

TOOLS = [
    fetch_airflow_driver_log,
    airflow_list_dag_runs,
    airflow_get_task_instance,
    spark_list_applications,
    analyze_spark_skew_and_spill,
    get_spark_executor_health,
    spark_get_stages,
    fetch_s3_spark_events,
    s3_find_executor_removed_events,
    fetch_local_executor_stderr,
    get_upstream_lineage_status,
    get_downstream_impact,
    check_asset_status,
    get_task_asset_urn,
    execute_athena_query_tool
]
