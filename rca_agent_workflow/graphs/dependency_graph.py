"""Dependency subgraph — checks upstream table/DAG health.

Executes Airflow and Lineage tools deterministically, then asks the LLM to summarize.
"""
import json
import re
from typing import Any, Dict, TypedDict

from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import END, START, StateGraph

from llm import get_llm
from tools.langgraph_tools import airflow_list_dag_runs, get_upstream_lineage_status , get_task_asset_urn

class DependencyState(TypedDict):
    dag_id: str
    run_id: str
    task_id: str
    dependency_result: Dict[str, Any]


def gather_data_node(state: DependencyState) -> dict:
    # 1. Get Airflow Data
    try:
        airflow_data = airflow_list_dag_runs.invoke({"dag_id": state["dag_id"], "limit": 3})
    except Exception as e:
        airflow_data = f"Airflow Error: {e}"

    # 2. Get Postgres Lineage Data
    try:
        asset_urn = get_task_asset_urn.invoke({
            "dag_id": state["dag_id"], 
            "run_id": state["run_id"], 
            "task_id": state["task_id"]
        })
        
        if "unknown" in asset_urn:
            lineage_data = "Lineage Warning: No URN found for this task."
        else:
            lineage_data = get_upstream_lineage_status.invoke({"target_asset": asset_urn})
            
    except Exception as e:
        lineage_data = f"Lineage Error: {e}"
        asset_urn = None

    return {
        "dependency_result": {
            "raw_airflow": airflow_data,
            "raw_lineage": lineage_data,
            "resolved_asset": "unknown"
        }
    }


def llm_node(state: DependencyState) -> dict:
    """Ask the LLM to parse the raw tool output into strict JSON."""
    llm = get_llm(temperature=0)
    
    sys_msg = SystemMessage(content="""You are a strict Data Dependency Analyst.
Read the raw data provided. 
- If Airflow runs show 'failed', the upstream pipeline failed.
- If Lineage shows tables as 'STALE' or 'FAILED', the upstream data is broken.
- Otherwise, everything is healthy.

Return a raw JSON object matching this exact schema:
{
  "status": "success" or "failure",
  "upstream_status": "healthy" or "upstream failure",
  "missing_tables": ["table_name_here"],
  "notes": "Brief explanation"
}""")

    human_msg = HumanMessage(content=f"Raw Data:\n{json.dumps(state['dependency_result'], indent=2)}")
    
    response = llm.invoke([sys_msg, human_msg])
    
    # Extract JSON
    text = response.content
    m = re.search(r"\{.*\}", text, re.DOTALL)
    try:
        result = json.loads(m.group()) if m else {"raw": text}
    except json.JSONDecodeError:
        result = {"raw": text}
        
    return {"dependency_result": result}

# Build graph
g = StateGraph(DependencyState)
g.add_node("gather_data", gather_data_node)
g.add_node("llm", llm_node)

g.add_edge(START, "gather_data")
g.add_edge("gather_data", "llm")
g.add_edge("llm", END)

dependency_subgraph = g.compile()