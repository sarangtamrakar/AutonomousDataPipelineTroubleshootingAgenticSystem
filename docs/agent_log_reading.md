This is actually one of the most important implementation details in your project.

Since everything is running in Docker locally, you don't have CloudWatch. Instead, your agents will read logs from Docker containers, Airflow log volumes, Spark event logs, and Spark History Server.

Option 1: Read Airflow Logs Directly (Recommended)

Most Airflow Docker setups mount logs to a host volume.

Example:

airflow:
  volumes:
    - ./logs:/opt/airflow/logs

Airflow stores logs like:

logs/
 └── dag_id/
      └── task_id/
           └── run_id/
                └── attempt=1.log

Example:

logs/daily_sales_pipeline/
     spark_gold_job/
     scheduled__2026-06-14/
     attempt=1.log

Your Log Agent tool:

from pathlib import Path

def get_airflow_log(log_path):
    return Path(log_path).read_text()

Supervisor receives:

{
  "dag_id":"daily_sales_pipeline",
  "log_path":"./logs/daily_sales_pipeline/..."
}

Then Log Agent analyzes the content.

Option 2: Read Docker Container Logs

For Spark failures this is very useful.

Example:

docker ps

Output:

spark-master
spark-worker-1
airflow-scheduler
airflow-webserver

Agent tool:

import subprocess

def get_container_logs(container_name):
    result = subprocess.run(
        ["docker","logs",container_name],
        capture_output=True,
        text=True
    )
    return result.stdout

Example:

logs = get_container_logs("spark-master")

Gemini receives:

java.lang.OutOfMemoryError

and classifies it.

Option 3: Spark Event Logs (Best for RCA Agent)

Enable:

spark.eventLog.enabled=true
spark.eventLog.dir=file:///tmp/spark-events

or

spark.eventLog.dir=s3a://spark-events

Local Docker example:

spark-master:
  volumes:
    - ./spark-events:/tmp/spark-events

Spark writes:

spark-events/
  application_123
  application_124

Your RCA Agent can parse:

Stage failures
Executor failures
Task failures
Shuffle issues

This is much richer than plain logs.

Option 4: Spark History Server API

This is what I'd do.

If you already run History Server:

http://localhost:18080

APIs:

/api/v1/applications
/api/v1/applications/{appId}
/api/v1/applications/{appId}/jobs
/api/v1/applications/{appId}/stages

Example:

import requests

apps = requests.get(
    "http://spark-history:18080/api/v1/applications"
).json()

Now RCA Agent can discover:

Failed Stage
Failed Executor
Shuffle Read Size
Skewed Partition

without parsing raw logs.

For your project this is much more impressive.

How Airflow Triggers Investigation

Airflow supports callbacks.

Example:

def failure_callback(context):

    event = {
        "dag_id": context["dag"].dag_id,
        "task_id": context["task"].task_id,
        "run_id": context["run_id"],
        "status": "FAILED"
    }

    producer.send(
        "pipeline_failures",
        event
    )

When a DAG fails:

Airflow
    ↓
Kafka Topic
    ↓
LangGraph Supervisor

No polling required.

What Each Agent Reads
Log Agent

Sources:

Airflow logs
Docker logs
Spark driver logs

Example result:

{
  "error_type":"OOM"
}

RCA Agent

Sources:
Spark History Server
Spark event logs
Airflow metadata

Example result:

{
  "root_cause":"Data Skew"
}
Dependency Agent

Sources:

Airflow metadata DB
Delta tables
Pipeline lineage table

Example:

{
  "missing_table":"bronze_sales"
}

Impact Agent
Sources:

Lineage metadata
Dashboard metadata
Gold tables

Example:

{
  "affected_dashboards":12
}


What I'd Build for Your Local Setup
Docker Compose

├── Airflow
├── Kafka
├── Spark Master
├── Spark Worker
├── Spark History Server
├── Postgres
├── Qdrant
├── LangGraph App

Agents read:

Airflow Logs
     ↓

Mounted Volume

Spark Logs
     ↓

Docker Logs

Spark Metrics
     ↓

History Server API

Incident Memory
     ↓

Qdrant

This is very close to how a real platform team investigates failures, except that in AWS you'd swap:

Airflow Logs      → CloudWatch
Spark Logs        → CloudWatch
History Server    → EMR APIs
Postgres          → RDS
Qdrant            → ECS/EKS/Qdrant Cloud

while keeping almost all of your LangGraph agent logic unchanged.