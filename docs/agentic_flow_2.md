LangGraph Multi-Agent Incident Response ArchitectureOverviewThis LangGraph application orchestrates an automated Incident Response (IR) system for failed Airflow/Spark data pipelines. It utilizes a Parallel Fan-Out architecture where three specialized agents investigate the failure concurrently.1. Graph State SchemaThe graph relies on a shared state. We use Python's TypedDict with operator.add reducers or standard overwrites depending on how we want to aggregate the data.from typing import TypedDict, Annotated, List, Dict, Any
import operator

class IncidentState(TypedDict):
    # Inputs from Airflow Callback
    dag_id: str
    task_id: str
    run_id: str
    spark_app_id: str
    
    # Outputs from Parallel Agents
    rca_result: Dict[str, Any]         # e.g., {"root_cause": "Data Skew"}
    dependency_result: Dict[str, Any]  # e.g., {"missing_table": "bronze_sales"}
    impact_result: Dict[str, Any]      # e.g., {"affected_dashboards": 12}
    
    # Final Output
    final_incident_report: str
2. The Specialized Agent NodesEach node acts as an isolated sub-agent with its own specific tools.RCA Node (rca_agent):Tools: Spark History API, S3 Event Logs, Airflow Logs.Goal: Diagnose code, OOMs, and Skew.Dependency Node (dependency_agent):Tools: Airflow Metadata DB (SQLAlchemy), Delta Lake Catalog.Goal: Check if required upstream partitions or tables were missing prior to execution.Impact Node (impact_agent):Tools: Lineage API (e.g., DataHub, OpenLineage), BI Tool API (Tableau/PowerBI).Goal: Determine downstream blast radius (which Gold tables and Dashboards are stale).3. LangGraph Routing Implementation (Parallel Execution)By defining edges from a START node to all three agents, and edges from all three agents to the report_generator, LangGraph automatically handles the parallel threading and synchronization.from langgraph.graph import StateGraph, START, END

def run_rca_agent(state: IncidentState) -> IncidentState:
    # LLM logic utilizing Spark Tools goes here
    # ...
    return {"rca_result": {"root_cause": "Data Skew in partition key_id"}}

def run_dependency_agent(state: IncidentState) -> IncidentState:
    # LLM logic utilizing Airflow/Delta tools goes here
    # ...
    return {"dependency_result": {"missing_table": "None - All upstream data present"}}

def run_impact_agent(state: IncidentState) -> IncidentState:
    # LLM logic utilizing Lineage tools goes here
    # ...
    return {"impact_result": {"affected_dashboards": ["Daily Exec Sales", "Marketing Spend"]}}

def generate_final_report(state: IncidentState) -> IncidentState:
    # A final LLM call that reads rca_result, dependency_result, and impact_result
    # and writes a cohesive Markdown alert for Slack/Teams.
    report = f"""
    🚨 **Pipeline Failure Alert** 🚨
    **DAG:** {state['dag_id']}
    
    **Root Cause:** {state['rca_result'].get('root_cause')}
    **Upstream Dependencies:** {state['dependency_result'].get('missing_table')}
    **Blast Radius:** {state['impact_result'].get('affected_dashboards')}
    """
    return {"final_incident_report": report}

# Build the Graph
workflow = StateGraph(IncidentState)

# Add Nodes
workflow.add_node("rca_agent", run_rca_agent)
workflow.add_node("dependency_agent", run_dependency_agent)
workflow.add_node("impact_agent", run_impact_agent)
workflow.add_node("report_generator", generate_final_report)

# --- PARALLEL ROUTING ---
# Start node branches out to all three agents simultaneously
workflow.add_edge(START, "rca_agent")
workflow.add_edge(START, "dependency_agent")
workflow.add_edge(START, "impact_agent")

# All three agents must converge on the report generator
workflow.add_edge("rca_agent", "report_generator")
workflow.add_edge("dependency_agent", "report_generator")
workflow.add_edge("impact_agent", "report_generator")

# End execution
workflow.add_edge("report_generator", END)

# Compile
incident_triage_app = workflow.compile()
4. Why this pattern?Speed: Web requests to Airflow APIs, Spark APIs, and Lineage APIs happen simultaneously. Total execution time equals the time of the slowest agent, not the sum of all three.Decoupling: If the Lineage API goes down, the impact_agent can safely return {"error": "Lineage API timeout"} without crashing the critical rca_agent.