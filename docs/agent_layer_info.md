# Agent Layer Info

These four agents are the heart of the project. Think of them as four engineers sitting together during a production incident, each with a different responsibility.

## Scenario

At 2 AM:

**daily_sales_pipeline FAILED**

Airflow sends:

```json
{
  "dag_id": "daily_sales_pipeline",
  "status": "FAILED"
}
```

The LangGraph Supervisor receives this event and starts the investigation.

## Agent 1: Log Analysis Agent

### Responsibility

Read logs and identify the actual error.

### Input

- Airflow logs
- Spark logs

### Example log

```text
ExecutorLostFailure

java.lang.OutOfMemoryError:
Java heap space
```

### Output

```json
{
  "error_type": "OUT_OF_MEMORY",
  "confidence": 0.98
}
```

### Tools

- `read_airflow_logs()`
- `read_spark_logs()`

### Interview Explanation

This agent converts unstructured logs into structured failure categories.

**Without this agent:**

- 1000 lines of logs

**With this agent:**

- OOM Error

## Agent 2: RCA (Root Cause Analysis) Agent

### Responsibility

Determine why the error happened.

Many candidates stop at:

- OOM Error

But that's not the root cause.

### Example 1

If the Log Agent says OOM, the RCA Agent investigates:

- Input dataset size
- Spark executor configuration
- Historical runs
- Data skew metrics

It discovers:

- One partition contains 85% of records

Actual Root Cause:

- Data Skew

### Output

```json
{
  "root_cause": "Data Skew",
  "confidence": 0.91
}
```

### Example 2

Log:

- Table not found

RCA Agent checks upstream DAG status and finds:

- `customer_master` failed earlier

Root Cause:

- Upstream dependency failure

### Tools

- `get_airflow_dependencies()`
- `get_spark_metrics()`
- `get_pipeline_metadata()`

## Agent 3: Impact Agent

### Responsibility

Determine business impact.

This is what separates a senior engineer from a junior engineer.

### Example 1

Failure:

- `daily_sales_pipeline`

Impact Agent checks lineage:

```text
daily_sales_pipeline
       │
       ▼
sales_gold
       │
       ▼
Finance Dashboard
       │
       ▼
CEO Revenue Report
```

Impact:

```json
{
  "severity": "HIGH",
  "affected_dashboards": 12,
  "affected_tables": 5,
  "affected_models": 2
}
```

### Example 2

Pipeline:

- `test_pipeline`

Only affects:

- Sandbox Dashboard

Output:

```json
{
  "severity": "LOW"
}
```

Now management knows whether to wake engineers at 2 AM.

### Tools

- `get_lineage()`
- `get_dashboard_dependencies()`
- `get_ml_dependencies()`

## Agent 4: Resolution Agent

### Responsibility

Suggest the fix.

This is where AI becomes useful.

### Inputs

- Error Type: OOM
- Root Cause: Data Skew
- Impact: High

The agent searches historical incidents and recommends a fix.

### Example

Similar Incident #123

Cause:

- Data Skew

Fix:

- Repartition by customer_id

### Output

```json
{
  "recommended_fix": "Repartition data by customer_id before aggregation"
}
```

## LangGraph & MCP Integration (implementation notes)

This section explains how to map the four agents into a LangGraph (or MCP) project, how to register the tool adapters, and example tool schemas you can use for orchestration.

### Mapping agents to LangGraph nodes
- **Supervisor (LangGraph)**: orchestrates the flow below. Receives Kafka events and runs the RCA pipeline by calling tool nodes and the LLM node.
- **Log Analysis Agent**: LangGraph node that calls `airflow_get_log` and `spark_history_get_stages` tools, then calls the LLM to extract `error_type` and `spark_application_id`.
- **RCA Agent**: LangGraph node that runs structured analysis (calls `spark_history_client.analyze_stage_for_spill_and_skew`, `s3_reader` fallback, and lineage queries).
- **Impact Agent**: LangGraph node that queries `postgres_store` / `lineage` services and returns severity.
- **Resolution Agent**: LangGraph node that composes remediation text and optionally calls `jira_create_issue`.

### Tool adapters (recommendation)
Run small HTTP adapters (FastAPI) that wrap the Python modules under `src/tools/`:

- `airflow_get_log` -> POST `/airflow/get_task_log` (body: dag_id, dag_run_id, task_id, try_number)
- `spark_history_get_stages` -> POST `/spark/history/stages` (body: application_id)
- `s3_reader_parse` -> POST `/s3/parse_events` (body: path)
- `postgres_store` -> POST `/incidents/save` (body: incident JSON)
- `jira_create_issue` -> POST `/jira/create` (body: summary, description, priority)

Each adapter should validate inputs and return consistent JSON responses that LangGraph nodes can consume.

### Example LangGraph tool declaration (YAML)

```yaml
tools:
  - name: airflow_get_log
    type: http
    endpoint: http://localhost:9000/airflow/get_task_log
    method: POST
    input: {dag_id: string, dag_run_id: string, task_id: string, try_number: integer}
    output: {log: string}

  - name: spark_history_get_stages
    type: http
    endpoint: http://localhost:9001/spark/history/stages
    method: POST
    input: {application_id: string}
    output: {stages: json}
```

### Supervisor flow (pseudocode)
1. Receive failure event from Kafka: `{dag_id, task_id, run_id, log_path}`
2. Call `airflow_get_log` => parse for `spark_application_id` and error snippet
3. If app id found: call `spark_history_get_stages` and analyze; if empty, call `s3_reader_parse` fallback using `SPARK_EVENT_LOGS_PATH` env var
4. Aggregate findings and call the LLM node with a strict JSON output requirement
5. Persist RCA and, if severity HIGH, call `jira_create_issue`

### MCP / Tool Schema (JSON example)

```json
{
  "tool": "spark_history_get_stages",
  "schema": {
    "input": {"application_id": "string"},
    "output": {"stages": "array"}
  },
  "endpoint": "http://localhost:9001/spark/history/stages"
}
```

### Environment and runtime
- All adapters and LangGraph should read configuration from environment variables (see `.env.example`).
- Important env vars: `AIRFLOW_BASE`, `SPARK_HISTORY_BASE`, `SPARK_EVENT_LOGS_PATH`, DB and Kafka connection strings.

### Next deliverables (I can implement)
- Scaffold small FastAPI adapters under `src/adapters/` for each tool and a `docker-compose` to run them on ports 9000..9010.
- Generate LangGraph YAML that registers those tools and provides a Supervisor graph.

Tell me which of the two you want me to implement next and I will scaffold it.
