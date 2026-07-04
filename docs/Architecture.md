# Agentic Root Cause Analysis (RCA) Architecture for Airflow-Spark Pipelines

## 1. Architecture Overview

This document outlines the observability and automated Root Cause Analysis (RCA) architecture for a local data engineering stack.
The system orchestrates Apache Spark (3.5.1) batch jobs using Apache Airflow (2.7.0) via Docker Compose.
Upon any task failure, an Airflow callback intercepts the event and dispatches a payload to a custom Agentic Application (LLM-powered SRE).
The agent then fetches distributed logs across the infrastructure to diagnose OOMs, data skew, and code exceptions.

### Core Infrastructure State

* **Cluster:** Spark Standalone Master + 2 workers attached to a custom bridge network (`spark-net`).
* **Storage:** Spark event logs and data warehouse are routed to S3 (`s3a://sarang-de/spark-event-logs`).
* **Execution:** PySpark jobs are executed using the **ephemeral driver pattern**. Airflow's `BashOperator` spins up a temporary Spark container on `spark-net` running in `--deploy-mode client`.
* **Telemetry:** Spark History Server is exposed on `localhost:18080`.

---

## 2. Job Submission Pattern (Ephemeral Driver)

Airflow avoids memory bloat by delegating the Spark driver to a temporary Docker container that destroys itself after execution.

```python
from datetime import datetime
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

HOST_TRAINING_DIR = os.environ.get("TRAINING_PIPELINE_DIR")

with DAG(dag_id="ml_spark_training_pipeline", start_date=datetime(2026, 6, 16), schedule_interval=None) as dag:
    submit_pyspark_training = BashOperator(
        task_id='submit_pyspark_training_bash',
        bash_command=f"""
            docker run --rm \
                --name airflow-spark-driver-{{{{ run_id }}}} \
                --network spark-net \
                -v {HOST_TRAINING_DIR}:/opt/airflow/training_pipeline \
                -v ./conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
                apache/spark:3.5.1 \
                /opt/spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                --deploy-mode client \
                /opt/airflow/training_pipeline/main_train.py
        """
    )
```

## 3. Failure Callback Trigger

When a Spark job fails, Airflow executes a callback to extract the dynamic `spark_application_id` and send the context to the RCA Agent API.

```python
import re
import requests
import os
from airflow.utils.log.log_reader import TaskLogReader


def handle_spark_failure_callback(context):
    ti = context.get("task_instance")

    # 1. Read Airflow task log to find Spark App ID
    log_reader = TaskLogReader()
    log_data, metadata = log_reader.read_log_stream(ti, ti.try_number)

    spark_app_id = None
    spark_app_pattern = re.compile(r"(app-\d{14}-\d{4})")

    for line in log_data.splitlines():
        match = spark_app_pattern.search(line)
        if match:
            spark_app_id = match.group(1)
            break

    # 2. Build agent payload
    payload = {
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context.get("run_id"),
        "spark_application_id": spark_app_id,
        "try_number": ti.try_number,
    }

    # 3. Trigger agent
    agent_url = os.environ.get(
        "RCA_AGENT_URL",
        "http://host.docker.internal:8000/api/v1/trigger-rca",
    )
    requests.post(agent_url, json=payload, timeout=10)
```

## 4. Agentic Log Fetching Strategies

Once the agent receives the payload, it must execute specific log-fetching strategies based on the availability of `spark_application_id`.

### Strategy A: Fetching Driver Logs (via Airflow API)

The agent retrieves the synchronous stdout/stderr from the ephemeral Docker container, which is captured natively by Airflow.

API endpoint:

`GET http://<airflow-host>:8080/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}`

Agent diagnostic logic:

* **Python exception:** Scan for `Traceback (most recent call last):` or `PySparkRuntimeException`.
* **Driver OOM:** Scan the final lines for `Command exited with return code 137`.
* **JVM OOM:** Scan for `java.lang.OutOfMemoryError: Java heap space`.

### Strategy B: Fetching Stage & Task Metrics (via Spark History API)

If the driver log shows no immediate Python syntax errors, the agent queries the Spark History Server to detect data skew or performance bottlenecks.

Endpoints:

* `GET http://localhost:18080/api/v1/applications/{spark_application_id}`
* `GET http://localhost:18080/api/v1/applications/{spark_application_id}/stages`

Agent diagnostic logic (data skew detection):

* Parse the `taskMetrics` JSON block for the failed stage.
* Extract executor runtime percentiles.
  * Rule: if `max` duration > `5 * median` duration, flag task skew.
* Extract `memoryBytesSpilled` and `diskBytesSpilled`.
  * Rule: if `diskBytesSpilled > 0`, flag memory pressure due to skew.

### Strategy C: Fetching Raw Event Logs (via S3 / boto3)

If the executor died silently or the History Server API times out, the agent parses raw Spark listener events from S3.

Target S3 path:

`s3a://sarang-de/spark-event-logs/{spark_application_id}`

Agent diagnostic logic (executor OOM):

* Download and read the JSON Lines (`.jsonl`) file.
* Search for the event: `"Event": "SparkListenerExecutorRemoved"`.
* Rule: if `Removed Reason` contains `137` or `Killed`, flag executor OOM.

## 5. Agent LLM Prompting Guidelines

When constructing the final RCA prompt for the LLM, structure the context explicitly:

* **System prompt:** `You are an expert Data Engineering SRE. Diagnose this failed Apache Spark pipeline based on the provided logs.`
* **Context Block 1 (Airflow/Driver):** Include the last 50 lines of the Airflow task log.
* **Context Block 2 (Spark API):** Include the failed stage's `taskMetrics` JSON filtered for max, median, and spill data.
* **Context Block 3 (S3 Events):** Include the exact JSON line for `SparkListenerExecutorRemoved` if found.

### Output requirement

Force the LLM to return a JSON object containing:

* `failure_category` (e.g. `OOM`, `Skew`, `Code`)
* `root_cause_summary`
* `remediation_code_snippet` (e.g. adjust `spark.executor.memory` or add `.repartition()`)
