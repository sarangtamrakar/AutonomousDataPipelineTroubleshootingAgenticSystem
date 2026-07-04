"""Dependency subgraph — checks upstream table/DAG health.

Executes Airflow and Lineage tools deterministically, then asks the LLM to summarize.
"""
import json
import re
from typing import Any, Dict, TypedDict , Optional ,List 

from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import END, START, StateGraph
from pydantic import Field , BaseModel



try:
    from config import cfg
    from llm import get_llm
    from tools.langgraph_tools import execute_athena_query_tool
except ImportError:
    import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
    from rca_agent_workflow.config import cfg
    from rca_agent_workflow.llm import get_llm
    from rca_agent_workflow.tools.langgraph_tools import execute_athena_query_tool

from langchain_core.prompts import ChatPromptTemplate

class AnalyticsState(TypedDict):
    user_message : str
    generated_sql: str
    generated_sql_explanation: str
    query_results: List[Dict[str, Any]]
    final_answer: str



class GeneratedSQL(BaseModel):
    rationale: str = Field(description="Brief explanation of why this SQL query answers the question.")
    sql_query: str = Field(description="The exact executable Presto/Trino SQL query for Athena.")



ATHENA_METADATA_PROMPT = """
You are an expert data analyst translating natural language queries into Athena-compliant SQL.
The database contains a Star Schema with 3 tables:

1. TABLE: dim_customer
   - customer_id (STRING): Unique ID for each customer
   - customer_name (STRING): Customer name
   - region (STRING): Regional location (e.g., North, South, East, West, Central)

2. TABLE: dim_product
   - product_id (STRING): Unique ID for each product
   - product_name (STRING): Name of the product
   - category (STRING): Category classification (e.g., Electronics, Clothing, Home & Kitchen)
   - unit_price (DOUBLE): Price per unit

3. TABLE: fact_sales
   - sale_id (STRING): Unique identifier for each transaction
   - customer_id (STRING): Foreign key referencing dim_customer
   - product_id (STRING): Foreign key referencing dim_product
   - sale_date (DATE): Date of sale (YYYY-MM-DD)
   - quantity_sold (INT): Quantity purchased
   - total_sale_amount (DOUBLE): Computed transaction value (quantity_sold * unit_price)

DIALECT RULES:
- Athena uses Presto/Trino SQL.
- Always quote table and column names with double quotes if they conflict with keywords.
- Use standard aggregations: SUM(), AVG(), COUNT().
- For date operations, use Presto date functions (e.g., date_trunc('month', sale_date)).
- Respond ONLY with the executable SQL string. Do not wrap in markdown or backticks.

CRITICAL JOIN RULES:
- The `fact_sales` table ONLY contains IDs (`customer_id`, `product_id`).
- If the user asks for `region` or `customer_name`, you MUST JOIN `fact_sales` with `dim_customer`.
- If the user asks for `product_name` or `category`, you MUST JOIN `fact_sales` with `dim_product`.

EXAMPLES:
User: "what is region wise total sale amount?"
Rationale: We need to group by region from dim_customer and sum the total_sale_amount from fact_sales.
SQL: SELECT c.region, SUM(f.total_sale_amount) AS total_sale_amount FROM fact_sales f JOIN dim_customer c ON f.customer_id = c.customer_id GROUP BY c.region;

User: "total sales by product category"
Rationale: We need to group by category from dim_product and sum the total_sale_amount.
SQL: SELECT p.category, SUM(f.total_sale_amount) AS total_sale_amount FROM fact_sales f JOIN dim_product p ON f.product_id = p.product_id GROUP BY p.category;
"""


# defining the each graph nodes

def get_llm_with_structured_output():
   llm = get_llm(temperature=0)
   structured_llm = llm.with_structured_output(GeneratedSQL)
   
   prompt_template = ChatPromptTemplate.from_messages([
         ("system", ATHENA_METADATA_PROMPT),
         ("user", "Translate this question to SQL: {question}")
      ])
   

   return structured_llm , prompt_template


def generate_sql_llm(state: AnalyticsState) -> dict:
   """Translates user question to validated Athena SQL."""
   structured_llm , prompt_template = get_llm_with_structured_output()
   chain = prompt_template | structured_llm
   response = chain.invoke({"question": state['user_message']})

   rationale = response.rationale
   sql_query = response.sql_query

   return {"generated_sql_explanation" : rationale, "generated_sql": sql_query}


def execute_sql_query(state: AnalyticsState) -> dict:
    """ Execute the SQL Query """
    generated_sql = state["generated_sql"]

    # SECURITY GUARDRAIL
    clean_sql = generated_sql.strip().upper()
    if not clean_sql.startswith("SELECT"):
        raise ValueError(f"Security violation: Only SELECT operations are allowed. Generated query: {generated_sql}")

    response = execute_athena_query_tool.invoke({"sql_query":generated_sql})

    # Safely extract records whether the tool returns a dictionary, an object, or a raw list
    if isinstance(response, dict):
        records = response.get("records", [])
    elif hasattr(response, "records"):
        records = response.records
    else:
        records = response

    return {"query_results" : records}


def synthesize_analytics_answer(state: AnalyticsState) -> dict:
    """ Takes the raw data from Athena and makes it human readable """
    print("--- [Node] Synthesizing Final Answer ---")
    llm = get_llm(temperature=0)
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a helpful data analyst. Use the provided database results to answer the user's original question concisely. Do not mention the SQL query itself."),
        ("user", "Question: {question}\n\nDatabase Results: {results}")
    ])
    
    chain = prompt | llm
    response = chain.invoke({
        "question": state["user_message"],
        "results": state["query_results"]
    })
    
    return {"final_answer": response.content}

   
graph = StateGraph(AnalyticsState)
graph.add_node("generate_sql_llm" , generate_sql_llm)
graph.add_node("execute_sql_query" , execute_sql_query)
graph.add_node("synthesize_analytics_answer" , synthesize_analytics_answer)

graph.add_edge(START,"generate_sql_llm")
graph.add_edge("generate_sql_llm","execute_sql_query")
graph.add_edge("execute_sql_query" , "synthesize_analytics_answer")
graph.add_edge("synthesize_analytics_answer" , END)

analytics_graph = graph.compile()



    


if __name__ == "__main__":
    result = analytics_graph.invoke({
        "user_message" : "what is customer name wise total sale amount? "
    })

    print(result['final_answer'])

