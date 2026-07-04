"""Impact subgraph — estimates downstream blast radius.

Executes Lineage tool deterministically, then asks the LLM to summarize.
"""
import json
import re
from typing import Any, Dict, TypedDict

from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import END, START, StateGraph

from llm import get_llm
from tools.langgraph_tools import get_downstream_impact

class ImpactState(TypedDict):
    dag_id: str
    task_id: str
    impact_result: Dict[str, Any]

def gather_data_node(state: ImpactState) -> dict:
    """Deterministically execute the tools via Python.
    
    This guarantees the Postgres/Lineage data is fetched without relying 
    on the LLM to format a tool call correctly.
    """
    try:
        impact_data = get_downstream_impact.invoke({"source_asset": state["task_id"]})
    except Exception as e:
        impact_data = f"Lineage Error: {e}"

    return {"impact_result": {"raw_impact": impact_data}}

def llm_node(state: ImpactState) -> dict:
    """Ask the LLM to parse the raw tool output into strict JSON."""
    llm = get_llm(temperature=0)
    
    sys_msg = SystemMessage(content="""You are a strict Downstream Impact Analyst.
Read the raw data provided to you.
- Count the number of downstream assets listed in the raw data.
- 0 to 1 assets = "low" blast radius.
- 2 to 3 assets = "medium" blast radius.
- 4+ assets = "high" blast radius.

You MUST return a raw JSON object matching this exact schema:
{
  "status": "failure",
  "affected_assets": ["list", "of", "asset_names", "here"],
  "blast_radius_estimate": "low" or "medium" or "high"
}""")

    human_msg = HumanMessage(content=f"Raw Data:\n{json.dumps(state['impact_result'], indent=2)}")
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
    result.setdefault("affected_assets", [])
    result.setdefault("blast_radius_estimate", "unknown")
        
    return {"impact_result": result}

# ── Build & compile ────────────────────────────────────────────────────────────

g = StateGraph(ImpactState)
g.add_node("gather_data", gather_data_node)
g.add_node("llm", llm_node)

g.add_edge(START, "gather_data")
g.add_edge("gather_data", "llm")
g.add_edge("llm", END)

impact_subgraph = g.compile()