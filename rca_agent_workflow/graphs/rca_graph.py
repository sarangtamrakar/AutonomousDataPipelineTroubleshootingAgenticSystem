"""RCA subgraph — diagnoses Spark/Airflow pipeline failures.

Executes Airflow log fetching, Spark History diagnostics, S3 event logs, 
and local worker logs deterministically via Python, then asks the LLM 
to find the root cause from the raw aggregated data.
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, TypedDict

from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph

from llm import get_llm
from tools.langgraph_tools import (
    fetch_airflow_driver_log,
    spark_get_stages,
    analyze_spark_skew_and_spill,
    get_spark_executor_health,
    fetch_s3_spark_events,
    s3_find_executor_removed_events,
    fetch_local_executor_stderr,
)


# ── Subgraph state ─────────────────────────────────────────────────────────────

class RCAState(TypedDict):
    dag_id: str
    run_id: str
    task_id: str
    spark_app_id: str
    rca_result: Dict[str, Any]

# ── Nodes ──────────────────────────────────────────────────────────────────────

def gather_data_node(state: RCAState) -> dict:
    """Deterministically execute all diagnostic tools via Python."""
    raw_data = {}
    
    # 1. Always fetch Airflow logs (contains Python tracebacks and Spark Driver logs)
    try:
        raw_data["airflow_logs"] = fetch_airflow_driver_log.invoke({
            "dag_id": state["dag_id"],
            "run_id": state["run_id"],
            "task_id": state["task_id"]
        })
    except Exception as e:
        raw_data["airflow_logs"] = f"Error fetching Airflow logs: {e}"

    # 2. If we have a Spark App ID, fetch deep Spark metrics and worker logs
    app_id = state.get("spark_app_id")
    if app_id:
        # --- Spark REST API Metrics ---
        try:
            raw_data["spark_stages"] = spark_get_stages.invoke({"app_id": app_id})
        except Exception as e:
            raw_data["spark_stages"] = f"Error: {e}"
            
        try:
            raw_data["executor_health"] = get_spark_executor_health.invoke({"app_id": app_id})
        except Exception as e:
            raw_data["executor_health"] = f"Error: {e}"
            
        try:
            raw_data["skew_and_spill"] = analyze_spark_skew_and_spill.invoke({"app_id": app_id})
        except Exception as e:
            raw_data["skew_and_spill"] = f"Error: {e}"
            
        # --- S3 Event Logs and Local Worker Logs ---
        try:
            raw_data["s3_spark_events"] = fetch_s3_spark_events.invoke({"app_id": app_id})
        except Exception as e:
            raw_data["s3_spark_events"] = f"Error: {e}"

        try:
            raw_data["s3_executor_removed_events"] = s3_find_executor_removed_events.invoke({"app_id": app_id})
        except Exception as e:
            raw_data["s3_executor_removed_events"] = f"Error: {e}"

        try:
            raw_data["local_executor_stderr"] = fetch_local_executor_stderr.invoke({"app_id": app_id})
        except Exception as e:
            raw_data["local_executor_stderr"] = f"Error: {e}"

    else:
        raw_data["spark_diagnostics"] = "No spark_app_id provided. Relying solely on Airflow logs."

    # Store all this raw text/json in the state for the LLM to read
    return {"rca_result": {"raw_data": raw_data}}


def llm_node(state: RCAState) -> dict:
    """Ask the LLM to read the raw diagnostic data and find the root cause."""
    llm = get_llm(temperature=0)
    
    sys_msg = SystemMessage(content="""You are an expert Spark/Airflow RCA engineer.
Read the raw logs, Spark metrics, S3 events, and worker stderr provided to you.

ANALYSIS RULES:
1. Look for Python/Java exceptions, Exit Code 137 (OOM), or memory spills across ALL provided logs.
2. If you see BlockManager evictions in Airflow logs, or 'executorremoved' events in S3 logs, it is an Out Of Memory (OOM) or node crash.
3. Check 'local_executor_stderr' for low-level Python worker crashes or Py4J errors.
4. If the skew_and_spill data shows high skew ratios (>5), it is Data Skew.
5. If everything looks clean, indicate that the exact failure reason is unclear.

You MUST return a raw JSON object matching this exact schema:
{
  "status": "failure",
  "root_cause": "Detailed explanation of the error",
  "evidence": "Specific log lines or metric anomalies that prove the root cause",
  "recommendation": "Actionable fix (e.g., increase memory, repartition, fix code)"
}""")

    # Pass the gathered data to the LLM
    raw_data = state.get("rca_result", {}).get("raw_data", {})
    human_msg = HumanMessage(content=f"Raw Data to Analyze:\n{json.dumps(raw_data, indent=2)}")
    
    response = llm.invoke([sys_msg, human_msg])
    
    # Extract JSON robustly
    text = response.content
    m = re.search(r"\{.*\}", text, re.DOTALL)
    try:
        result = json.loads(m.group()) if m else {"raw": text}
    except json.JSONDecodeError:
        result = {"raw": text}
        
    # Guarantee the expected keys exist so the master report_node doesn't fail
    result.setdefault("status", "failure")
    result.setdefault("root_cause", "Unable to determine from logs.")
    result.setdefault("evidence", "No conclusive evidence extracted.")
    result.setdefault("recommendation", "Investigate logs manually.")
    
    return {"rca_result": result}


# ── Build & compile ────────────────────────────────────────────────────────────

g = StateGraph(RCAState)
g.add_node("gather_data", gather_data_node)
g.add_node("llm",         llm_node)

g.add_edge(START, "gather_data")
g.add_edge("gather_data", "llm")
g.add_edge("llm", END)

rca_subgraph = g.compile()