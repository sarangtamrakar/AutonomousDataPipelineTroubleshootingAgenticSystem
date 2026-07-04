"""Main incident response graph — parallel fan-out via compiled subgraphs.

Architecture:
    START
      ├──► check_status_node ──(if success)──────────────────────────┐
                │                                                    │
             (if failed)                                             │
                │                                                    │
      ├──► fan_out_node                                              │
                ├──► rca_node        ──┐                             │
                ├──► dependency_node ──┼──► report_node ─────────────┼──► END
                └──► impact_node     ──┘
"""
from __future__ import annotations

import argparse
import json
from typing import Any, Dict, TypedDict

from langchain_core.messages import HumanMessage, SystemMessage
from config import cfg
from llm import get_llm
from langgraph.graph import END, START, StateGraph

from graphs.rca_graph        import rca_subgraph
from graphs.dependency_graph import dependency_subgraph
from graphs.impact_graph     import impact_subgraph
from graphs.analytics_graph import analytics_graph
from tools.airflow_client    import create_default_client 

# ── Main graph state ───────────────────────────────────────────────────────────

class IncidentState(TypedDict):
    # Inputs
    dag_id:       str
    task_id:      str
    run_id:       str
    spark_app_id: str
    user_message: str

    # Gatekeeper Status
    task_status:  str

    # Subgraph outputs
    rca_result:         Dict[str, Any]
    dependency_result:  Dict[str, Any]
    impact_result:      Dict[str, Any]

    # Analytics outputs
    generated_sql: str
    query_results: list
    final_answer:  str

    # Final
    final_incident_report: str


# ── Nodes ──────────────────────────────────────────────────────────────────────

def check_status_node(state: IncidentState) -> dict:
    """The Gatekeeper: Fast-fail if the task is actually successful."""
    client = create_default_client()
    try:
        # Fetch the exact task instance from Airflow
        task_data = client.get_task_instance(state["dag_id"], state["run_id"], state["task_id"])
        status = task_data.get("state", "unknown")
    except Exception as e:
        status = "failed" # Assume failed so the agent investigates why it can't reach Airflow
        
    return {"task_status": status}


def rca_node(state: IncidentState) -> dict:
    result = rca_subgraph.invoke({
        "dag_id":       state["dag_id"],
        "run_id":       state["run_id"],
        "task_id":      state["task_id"],
        "spark_app_id": state["spark_app_id"],
        "messages":     [],
        "rca_result":   {},
    })
    return {"rca_result": result["rca_result"]}


def dependency_node(state: IncidentState) -> dict:
    
    result = dependency_subgraph.invoke({
        "dag_id":            state["dag_id"],
        "run_id":            state["run_id"],
        "task_id":           state["task_id"], 
        "messages":          [],
        "dependency_result": {},
    })
    return {"dependency_result": result["dependency_result"]}


def impact_node(state: IncidentState) -> dict:
    result = impact_subgraph.invoke({
        "dag_id":        state["dag_id"],
        "task_id":       state["task_id"],
        "messages":      [],
        "impact_result": {},
    })
    return {"impact_result": result["impact_result"]}


def analytics_node(state: IncidentState) -> dict:
    """Passes the user message to the SQL analytics subgraph."""
    result = analytics_graph.invoke({"user_message": state["user_message"]})
    return {
        "generated_sql": result.get("generated_sql"),
        "query_results": result.get("query_results"),
        "final_answer": result.get("final_answer")
    }


def report_node(state: IncidentState) -> dict:
    # Early exit if the Gatekeeper marked it as a success
    if state.get("task_status") == "success":
        report = f"""# ✅ Pipeline Execution Successful

**DAG:** {state['dag_id']} | **Task:** {state['task_id']} | **Run:** {state['run_id']}

## Status
The Airflow task executed successfully. No incident report, RCA, or downstream impact analysis is required.
"""
        return {"final_incident_report": report}

    # Otherwise, generate the full failure report
    llm = get_llm(temperature=0)
    
    sys_msg = SystemMessage(content="""You are a Senior Data Engineering Reporter. 
Your job is to synthesize findings from three distinct AI agents into a single, cohesive Markdown report.

CRITICAL RULES:
1. "Dependencies" refers to upstream data pipelines and Airflow DAGs. 
2. Do NOT confuse the names of Python tools (like `airflow_list_dag_runs`) with the actual Data Pipelines.
3. If an agent reports a "tool failure" or says a function "did not return expected results", do NOT list the Python function as the root cause of the data pipeline failure. Ignore the tool error and focus on the data/code logic.""")

    prompt = HumanMessage(content=(
        "Generate a concise Markdown incident report from these findings:\n\n"
        f"RCA: {json.dumps(state.get('rca_result', {}), indent=2)}\n\n"
        f"Dependencies: {json.dumps(state.get('dependency_result', {}), indent=2)}\n\n"
        f"Impact: {json.dumps(state.get('impact_result', {}), indent=2)}\n\n"
        f"DAG: {state['dag_id']} | Task: {state['task_id']} | Run: {state['run_id']}\n"
        "Include: 🚨 header, Root Cause, Evidence, Upstream Status, Blast Radius, Recommendation."
    ))
    response = llm.invoke([sys_msg, prompt])
    report = response.content

    return {"final_incident_report": report}


# ── Routing Logic ──────────────────────────────────────────────────────────────
def entry_router(state: IncidentState) -> str:
    """Determine if this is a user chat message or an automated pipeline alert."""
    if state.get("user_message"):
        return "chat"
    return "incident"

def route_status(state: IncidentState) -> str:
    """Determine whether to investigate or skip directly to the report."""
    if state.get("task_status") == "success":
        return "success"
    return "failed"


# ── Build main graph ───────────────────────────────────────────────────────────

def build_graph():
    g = StateGraph(IncidentState)

    g.add_node("check_status",    check_status_node)
    g.add_node("fan_out",         lambda state: state) # Dummy pass-through node for fan-out
    
    g.add_node("rca_node",        rca_node)
    g.add_node("dependency_node", dependency_node)
    g.add_node("impact_node",     impact_node)
    g.add_node("report_node",     report_node)
    g.add_node("analytics_node",     analytics_node)

    # 1. Start -> Gatekeeper
    g.set_conditional_entry_point(
        entry_router,
        {
            "incident": "check_status",
            "chat": "analytics_node"
        }
    )

    # 2. Gatekeeper Conditional Logic
    g.add_conditional_edges("check_status", route_status, {
        "success": "report_node", # Bypass the AI subgraphs
        "failed": "fan_out"       # Proceed to fan-out to the agents
    })

    # 3. Parallel Fan-out to Subgraphs
    g.add_edge("fan_out", "rca_node")
    g.add_edge("fan_out", "dependency_node")
    g.add_edge("fan_out", "impact_node")

    # 4. Converge at the Report generator
    g.add_edge("rca_node",        "report_node")
    g.add_edge("dependency_node", "report_node")
    g.add_edge("impact_node",     "report_node")

    g.add_edge("report_node", END)
    g.add_edge("analytics_node", END)
    return g.compile()


app = build_graph()


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    
    state = {
        "dag_id": "master_medallion_pipeline_flat",
        "run_id": "manual__2026-06-17T20:15:17.094587+00:00",
        "task_id": "run_silver_products",
        "spark_app_id": "app-20260617201622-0018",
        "task_status": "unknown", # ADDED FOR THE GATEKEEPER
    }

    
    print("\n=== RUNNING AIRFLOW INCIDENT TEST ===")
    result = app.invoke(state)
    print("\n" + "=" * 60)
    print(result["final_incident_report"])


    print("\n=== RUNNING HUMAN CHAT ANALYTICS TEST ===")
    chat_state = {
        "user_message": "what is region wise total sale amount?"
    }
    
    chat_result = app.invoke(chat_state)
    print("\n" + "=" * 60)
    print(chat_result.get("final_answer"))