End-to-End Project Plan: AI-Powered SRE Agent

Phase 1: Project Folder Structure

A clean, modular architecture is critical for multi-agent systems. Create this structure in your repository:

ai-sre-agent/
├── .env.example                 # Template for API keys and URLs
├── requirements.txt             # Python dependencies
├── Dockerfile                   # To containerize the Agent API
├── docker-compose.agent.yml     # To run the agent locally with Spark/Airflow
├── src/
│   ├── __init__.py
│   ├── main.py                  # FastAPI Application (Webhook receiver)
│   ├── config.py                # Environment variable loading
│   ├── graph/
│   │   ├── __init__.py
│   │   ├── state.py             # IncidentState TypedDict definition
│   │   └── workflow.py          # StateGraph compilation & edge routing
│   ├── agents/
│   │   ├── __init__.py
│   │   ├── rca_node.py          # LLM logic for RCA
│   │   ├── dependency_node.py   # LLM logic for Dependencies
│   │   ├── impact_node.py       # LLM logic for Impact
│   │   └── report_node.py       # LLM logic for Final Synthesis
│   ├── tools/
│   │   ├── __init__.py
│   │   ├── spark_tools.py       # fetch_airflow_driver_log, get_spark_stage_metrics, etc.
│   │   ├── sql_tools.py         # check_upstream_tables_sql
│   │   └── openmetadata_tools.py# fetch_downstream_impact_openmetadata
│   └── clients/
│       ├── __init__.py
│       └── spark_client.py      # Your custom Python SparkHistoryClient
└── tests/
    └── test_spark_client.py     # Unit tests for skew/spill logic


Phase 2: Environment & Dependencies

1. Create virtual environment:

python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate


2. requirements.txt:

# Web Server
fastapi
uvicorn

# LLM & Graphs
langchain
langchain-openai # Or langchain-anthropic
langgraph

# Data Fetching & AWS
requests
boto3
beautifulsoup4

# Database
sqlalchemy
psycopg2-binary # Or your specific DB driver


3. Configure .env:

# LLM Settings
OPENAI_API_KEY=sk-...

# Airflow & Spark Targets
AIRFLOW_API_BASE=http://airflow-webserver:8080/api/v1
AIRFLOW_USER=admin
AIRFLOW_PASS=admin
SPARK_HISTORY_API=http://spark-history:18080/api/v1/applications
SPARK_WORKER_LOG_DIR=/opt/spark-worker-logs

# Metadata Targets
METADATA_DB_URL=postgresql://user:pass@localhost:5432/metadata
OPENMETADATA_URL=http://localhost:8585/api/v1
OPENMETADATA_TOKEN=jwt_token_here


Phase 3: The FastAPI Webhook Layer (src/main.py)

Airflow's on_failure_callback needs a server to send the JSON payload to. We use FastAPI to receive the trigger and start the LangGraph workflow in the background.

from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from src.graph.workflow import app as incident_graph

app = FastAPI(title="AI SRE Agent Webhook")

class AirflowFailurePayload(BaseModel):
    dag_id: str
    task_id: str
    run_id: str
    spark_application_id: str | None = None
    target_table: str | None = "unknown"

def run_incident_graph(payload: dict):
    """Executes the LangGraph workflow synchronously in the background."""
    print(f"Starting Incident Triage for {payload['dag_id']}...")
    result = incident_graph.invoke(payload)
    
    final_report = result.get("final_report")
    print(f"Triage Complete. Sending Alert to Slack...")
    # Add Slack/Teams webhook POST request here

@app.post("/api/v1/trigger-rca")
async def trigger_rca(payload: AirflowFailurePayload, background_tasks: BackgroundTasks):
    """Airflow hits this endpoint immediately upon task failure."""
    # Run the heavy LLM graph in the background so Airflow doesn't timeout
    background_tasks.add_task(run_incident_graph, payload.dict())
    return {"status": "Incident acknowledged. Triage initiated in background."}


Phase 4: Containerization (Dockerfile)

To deploy this alongside your Airflow and Spark stack, wrap it in a Docker image.

FROM python:3.10-slim

WORKDIR /app

# Install OS dependencies for database drivers
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/

# Run FastAPI using Uvicorn
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]


Phase 5: Deployment via Docker Compose

Add the Agent to your existing local infrastructure stack. Crucially, it must be on the spark-net network so it can communicate directly with the Spark History Server, Airflow, and the mounted worker logs.

Create/Append to docker-compose.yml:

services:
  sre-agent:
    build: 
      context: ./ai-sre-agent
      dockerfile: Dockerfile
    container_name: sre-agent
    ports:
      - "8000:8000"
    env_file:
      - ./ai-sre-agent/.env
    networks:
      - spark-net
    volumes:
      # MUST mount the same worker logs path so the agent can read stderr
      - ./spark-worker-logs:/opt/spark-worker-logs


Phase 6: Testing the End-to-End Pipeline

Start the Infrastructure: docker compose up -d (Spins up Spark, Airflow, and the SRE Agent).

Submit a Bad Spark Job: Intentionally trigger an error in your Airflow DAG (e.g., 1/0 division error or configure spark.executor.memory artificially low to force an OOM).

Observe the Flow:

Airflow task fails.

Airflow executes handle_spark_failure_callback.

Callback POSTs to http://sre-agent:8000/api/v1/trigger-rca.

FastAPI receives it, returns 200 OK to Airflow immediately.

LangGraph starts in the background.

RCA Agent scrapes the Spark API and worker logs.

Dependency/Impact agents hit their respective databases.

Report node synthesizes the output.

Check your Agent Docker logs (docker logs -f sre-agent) to see the final output!