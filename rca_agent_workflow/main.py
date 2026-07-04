from graph_builder import build_graph # Adjust based on your file structure
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from typing import Optional
import logging


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Unified Data Ops API")

# Initialize graph once at startup
graph = build_graph()

# ==========================================
# 1. REQUEST MODELS (Made Optional)
# ==========================================
class IncidentRequest(BaseModel):
    dag_id: str
    run_id: str
    task_id: str
    spark_app_id: Optional[str] = None

class ChatRequest(BaseModel):
    user_message: str

# ==========================================
# 2. BACKGROUND TASK LOGIC (For Airflow)
# ==========================================
def run_incident_workflow(request: IncidentRequest):
    """Background execution logic for Airflow incidents."""
    try:
        logger.info(f"Triggering RCA for {request.dag_id}/{request.task_id}")
        
        initial_state = {
            "dag_id": request.dag_id,
            "run_id": request.run_id,
            "task_id": request.task_id,
            "spark_app_id": request.spark_app_id,
            "task_status": "unknown"
        }
        
        result = graph.invoke(initial_state)
        report = result.get("final_incident_report", "No report generated.")
        
        logger.info("Incident report generation complete.")
        # In a real app, send 'report' to Slack, Teams, or an Email here!
        print(f"\n--- RCA REPORT ---\n{report}")
        
    except Exception as e:
        logger.error(f"Incident Workflow failed: {str(e)}")

# ==========================================
# 3. ENDPOINTS
# ==========================================

@app.post("/trigger-incident")
async def trigger_incident(request: IncidentRequest, background_tasks: BackgroundTasks):
    """
    Endpoint for Airflow Callbacks. 
    Runs in the background so Airflow doesn't timeout waiting for the LLM.
    """
    background_tasks.add_task(run_incident_workflow, request)
    return {
        "status": "accepted", 
        "message": f"Incident analysis started in background for {request.task_id}"
    }

@app.post("/chat")
async def chat(request: ChatRequest):
    """
    Endpoint for Human Chat (UI, Slackbot, etc).
    Runs synchronously to return the answer directly in the HTTP response.
    """
    try:
        logger.info(f"Received chat query: {request.user_message}")
        
        # Invoke the graph synchronously
        result = graph.invoke({"user_message": request.user_message})
        
        return {
            "status": "success",
            "answer": result.get("final_answer"),
            "sql_used": result.get("generated_sql"),
            "data": result.get("query_results")
        }
    except Exception as e:
        logger.error(f"Chat Workflow failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health():
    return {"status": "alive"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)