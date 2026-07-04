import os
import re
import glob
import json
import boto3
import requests
from typing import Dict, Any, Optional
from langchain_core.tools import tool

# Initialize Spark History client for reuse across tools
from src.tools.spark_history_client import create_default_spark_client


import os
import json
import operator
from typing import TypedDict, Annotated, Dict, Any, List
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.tools import tool
from langgraph.graph import StateGraph, START, END
from sqlalchemy import create_engine, text
import requests



# Instantiate your global client to reuse the requests.Session
spark_client = create_default_spark_client()

# =====================================================================
# Configuration & Helpers
# =====================================================================
AIRFLOW_API_BASE = os.environ.get("AIRFLOW_API_BASE", "http://localhost:8080/api/v1")
AIRFLOW_AUTH = (os.environ.get("AIRFLOW_USER", "admin"), os.environ.get("AIRFLOW_PASS", "admin"))
SPARK_HISTORY_API = os.environ.get("SPARK_HISTORY_API", "http://localhost:18080/api/v1/applications")
HOST_WORK_DIR = os.environ.get("SPARK_WORKER_LOG_DIR", "./spark-worker-logs")

def _smart_truncate(text: str, max_lines: int = 150) -> str:
    """Helper to prevent LLM context window limits by keeping top and bottom of logs."""
    lines = text.splitlines()
    if len(lines) <= max_lines:
        return text
    
    top_lines = lines[:40]
    bottom_lines = lines[-110:]
    return "\n".join(top_lines + ["\n... [TRUNCATED TO SAVE CONTEXT WINDOW] ...\n"] + bottom_lines)

# =====================================================================
# LangGraph Agent Tools
# =====================================================================

@tool
def fetch_airflow_driver_log(dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> str:
    """
    Use this tool FIRST when diagnosing a pipeline failure. 
    It fetches the stdout/stderr of the Airflow task, which contains the Spark Driver logs.
    
    Look closely at the output to find:
    1. Python/Java Exceptions (Tracebacks).
    2. Docker Exit Code 137 (Driver OOM).
    3. The spark_application_id (format: app-YYYYMMDDHHMMSS-XXXX).
    
    Args:
        dag_id: The Airflow DAG ID.
        run_id: The Airflow Run ID.
        task_id: The failed Task ID.
        try_number: The attempt number (default 1).
    """
    url = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}"
    try:
        response = requests.get(url, auth=AIRFLOW_AUTH, headers={"Accept": "text/plain"}, timeout=10)
        response.raise_for_status()
        
        # Automatically extract the app ID to help the LLM
        log_text = response.text
        match = re.search(r'(app-\d{14}-\d{4})', log_text)
        app_id_hint = f"\n\n[SYSTEM DETECTED SPARK APP ID: {match.group(1)}]\n" if match else "\n\n[SYSTEM COULD NOT DETECT SPARK APP ID]\n"
        
        return _smart_truncate(log_text) + app_id_hint
    except Exception as e:
        return f"Error fetching Airflow logs: {str(e)}"

@tool
def get_spark_stage_metrics(app_id: str) -> str:
    """
    Use this tool to diagnose DATA SKEW or DISK SPILL.
    It returns a JSON summary of all stages for a given Spark application.
    
    Analyze the 'taskMetrics' in the response. Compare the Max vs Median (50th percentile) 
    for 'executorRunTime' and 'shuffleReadBytes'. If Max is > 5x the Median, declare Data Skew.
    If 'diskBytesSpilled' > 0, declare Memory Pressure due to Skew.
    
    Args:
        app_id: The Spark application ID (e.g., app-20260616120000-0001).
    """
    url = f"{SPARK_HISTORY_API}/{app_id}/stages"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        stages = response.json()
        
        # Only return failed or highly skewed stages to save LLM context
        problematic_stages = [
            s for s in stages 
            if s.get("status") == "FAILED" or s.get("diskBytesSpilled", 0) > 0
        ]
        
        # If no explicit failures, return all (truncated)
        target = problematic_stages if problematic_stages else stages
        return json.dumps(target)[:4000] + "... (truncated)"
    except Exception as e:
        return f"Error fetching Spark stages: {str(e)}"

@tool
def get_spark_executor_metrics(app_id: str) -> str:
    """
    Use this tool to diagnose EXECUTOR OUT-OF-MEMORY (OOM) errors.
    It returns OS-level and JVM-level memory consumption for each executor.
    
    Look for 'peakMemoryMetrics'. If 'ProcessTreeJVMRSSMemory' approaches the Docker 
    container limit, the container was killed by the OS. Recommend increasing spark.executor.memoryOverhead.
    
    Args:
        app_id: The Spark application ID.
    """
    url = f"{SPARK_HISTORY_API}/{app_id}/executors"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        executors = response.json()
        return json.dumps(executors, indent=2)
    except Exception as e:
        return f"Error fetching Spark executors: {str(e)}"

@tool
def fetch_s3_spark_events(app_id: str, bucket_name: str = "sarang-de", prefix: str = "spark-event-logs") -> str:
    """
    Use this tool if the executor died silently or the History API returns empty.
    It reads raw SparkListener events from S3. 
    
    Look specifically for 'SparkListenerExecutorRemoved' to find the exact exit reason (e.g., code 137).
    
    Args:
        app_id: The Spark application ID.
        bucket_name: The S3 bucket name.
        prefix: The folder prefix in S3.
    """
    s3_client = boto3.client('s3')
    object_key = f"{prefix}/{app_id}"
    
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        lines = response['Body'].read().decode('utf-8').splitlines()
        
        # Filter only critical events to save LLM context window
        critical_events = [
            line for line in lines 
            if "SparkListenerExecutorRemoved" in line or "SparkListenerTaskEnd" in line
        ]
        
        if not critical_events:
            return "No critical failure events found in S3 event log."
            
        return "\n".join(critical_events[-30:]) # Return the last 30 critical events
    except Exception as e:
        return f"Error fetching S3 events: {str(e)}"

@tool
def fetch_local_executor_stderr(app_id: str) -> str:
    """
    Use this tool to read the physical worker stderr logs from the host filesystem.
    This contains the actual Python print statements, warning logs, and Java stack traces 
    emitted by the worker nodes.
    
    Args:
        app_id: The Spark application ID.
    """
    search_pattern = os.path.join(HOST_WORK_DIR, app_id, "*", "stderr")
    executor_logs = {}
    
    try:
        files = glob.glob(search_pattern)
        if not files:
            return "No local executor stderr files found. Check volume mounts or cleanup rules."
            
        for log_path in files:
            executor_id = log_path.split(os.sep)[-2]
            with open(log_path, 'r') as file:
                lines = file.readlines()
                executor_logs[f"Executor_{executor_id}"] = _smart_truncate("".join(lines), max_lines=50)
                
        return json.dumps(executor_logs, indent=2)
    except Exception as e:
        return f"Error reading local stderr files: {str(e)}"


# (client instantiated above via create_default_spark_client)

@tool
def analyze_spark_skew_and_spill(app_id: str) -> str:
    """
    Use this tool to diagnose DATA SKEW and MEMORY/DISK SPILLS in a Spark application.
    It automatically scans all stages, calculates task duration percentiles, and 
    checks for bytes spilled to disk.
    
    Args:
        app_id: The Spark application ID (e.g., app-20260616120000-0001).
    """
    try:
        # 1. Fetch all stages to find which ones to analyze
        all_stages = spark_client.get_stages(app_id)
        
        if not all_stages:
            return f"No stages found for application {app_id}."

        analysis_results = []
        
        # 2. Filter for problematic stages (Failed stages, or stages that took a long time)
        # Note: We filter to save LLM context window size.
        for stage in all_stages:
            status = stage.get("status")
            stage_id = stage.get("stageId")
            
            # Analyze if it failed, or if it had a high failure/spill indicator at the summary level
            if status == "FAILED" or stage.get("diskBytesSpilled", 0) > 0:
                # Use your custom skew and spill analyzer
                stage_analysis = spark_client.analyze_stage_for_spill_and_skew(app_id, stage_id)
                stage_analysis["status"] = status
                stage_analysis["name"] = stage.get("name")
                
                analysis_results.append(stage_analysis)
        
        if not analysis_results:
            return "All stages completed successfully with no detected disk spills or failures."
            
        return json.dumps(analysis_results, indent=2)

    except Exception as e:
        return f"Error analyzing stages for skew: {str(e)}"

@tool
def get_spark_executor_health(app_id: str) -> str:
    """
    Use this tool to diagnose EXECUTOR OUT-OF-MEMORY (OOM) errors.
    It returns OS-level and JVM-level memory consumption for each executor.
    
    Look for 'peakMemoryMetrics'. If 'ProcessTreeJVMRSSMemory' approaches the Docker 
    container limit, the container was killed by the OS.
    
    Args:
        app_id: The Spark application ID.
    """
    try:
        # Utilizing your client's session and base URL construction
        url = spark_client._url(f"applications/{app_id}/executors")
        resp = spark_client.session.get(url, timeout=spark_client.timeout)
        resp.raise_for_status()
        executors = resp.json()
        
        # Optional: Filter out the "driver" if in cluster mode, but keep all for client mode
        return json.dumps(executors, indent=2)
    except Exception as e:
        return f"Error fetching executor health: {str(e)}"
    




# =====================================================================
# 1. NEW TOOLS: Dependency & Impact Agents (SQL & OpenMetadata)
# =====================================================================

# Setup SQLAlchemy Engine for Custom Metadata/Lineage DB
METADATA_DB_URL = os.environ.get("METADATA_DB_URL", "postgresql://user:pass@localhost:5432/metadata_db")
engine = create_engine(METADATA_DB_URL)

@tool
def check_upstream_tables_sql(target_table: str) -> str:
    """
    Use this tool for the Dependency Agent.
    Queries the custom lineage SQL database to check if upstream tables for a given target 
    were successfully updated within the last 24 hours.
    
    Args:
        target_table: The name of the table that the failed Spark job was trying to write to.
    """
    query = """
        SELECT upstream_table_name, last_updated_at, status 
        FROM pipeline_lineage 
        WHERE downstream_table_name = :target_table
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), {"target_table": target_table}).fetchall()
            if not result:
                return f"No upstream dependencies found for {target_table}."
            
            # Convert to list of dicts for LLM readability
            deps = [{"table": r[0], "last_updated": str(r[1]), "status": r[2]} for r in result]
            return json.dumps(deps, indent=2)
    except Exception as e:
        return f"SQL Execution Error: {str(e)}"

OPENMETADATA_URL = os.environ.get("OPENMETADATA_URL", "http://localhost:8585/api/v1")
OPENMETADATA_TOKEN = os.environ.get("OPENMETADATA_TOKEN", "")

@tool
def fetch_downstream_impact_openmetadata(table_name: str) -> str:
    """
    Use this tool for the Impact Agent.
    Queries the OpenMetadata Lineage API to find all downstream Dashboards and 
    Gold tables affected if this current table fails to update.
    
    Args:
        table_name: The fully qualified name of the failed table (e.g., 'catalog.schema.table').
    """
    headers = {"Authorization": f"Bearer {OPENMETADATA_TOKEN}", "Content-Type": "application/json"}
    try:
        # 1. Get Table ID from Name
        search_url = f"{OPENMETADATA_URL}/search/query?q={table_name}&index=table_search_index"
        search_res = requests.get(search_url, headers=headers).json()
        
        if not search_res.get('hits', {}).get('hits'):
            return "Table not found in OpenMetadata."
            
        table_id = search_res['hits']['hits'][0]['_source']['id']
        
        # 2. Fetch Lineage using the ID
        lineage_url = f"{OPENMETADATA_URL}/lineage/table/name/{table_name}?upwardDepth=0&downwardDepth=3"
        lineage_res = requests.get(lineage_url, headers=headers).json()
        
        # Filter for dashboards or tier-1 assets
        affected_nodes = [
            node['fullyQualifiedName'] for node in lineage_res.get('nodes', [])
            if node['entityType'] in ['dashboard', 'table']
        ]
        
        return json.dumps({"affected_assets": affected_nodes}, indent=2)
    except Exception as e:
        return f"OpenMetadata API Error: {str(e)}"


# Combine tools for respective agents
# Note: spark_rca_tools is imported from your previously provided codebase
# from src.tools.spark_rca_tools import spark_rca_tools
dependency_tools = [check_upstream_tables_sql]
impact_tools = [fetch_downstream_impact_openmetadata]






# =====================================================================
# Tools Array for LangGraph
# =====================================================================
# Combine these with the Airflow/S3 log fetching tools we built earlier
spark_tools = [
    analyze_spark_skew_and_spill,
    get_spark_executor_health
]




# =====================================================================
# List of tools to bind to your LangGraph LLM Node
# Example: llm.bind_tools(spark_rca_tools)
# =====================================================================
spark_rca_tools = [
    fetch_airflow_driver_log,
    get_spark_stage_metrics,
    get_spark_executor_metrics,
    fetch_s3_spark_events,
    fetch_local_executor_stderr
]