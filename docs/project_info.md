# Project Info

## Project Name

**DataOps AI Copilot**

(Autonomous Data Pipeline Troubleshooting System)

## Problem Statement

A company has:

- 500 Airflow DAGs
- 200 Spark jobs
- Kafka streaming pipelines
- Redshift data warehouse

Every day:

- DAG failures occur
- Data quality issues happen
- Upstream tables arrive late
- Spark jobs run OOM
- Schema changes break pipelines

Engineers spend hours debugging.

## AI Copilot Goals

The AI Copilot should:

- Detect failures automatically
- Investigate root causes
- Determine impact
- Recommend fixes
- Create Jira tickets
- Notify Slack
- Learn from historical incidents

## High-Level Architecture

```text
                ┌─────────────┐
                │ Airflow DAG │
                └──────┬──────┘
                       │
                Failure Event
                       │
                       ▼

                 Kafka Topic
             pipeline_failures

                       │
                       ▼

            Spark Streaming Job

                       │

       ┌───────────────┼────────────────┐
       │               │                │
       ▼               ▼                ▼

   OpenSearch      Incident DB      S3 Logs

       │               │                │
       └───────┬───────┴────────┬───────┘
               │                │

               ▼

      LangGraph Supervisor

               │

 ┌─────────────┼─────────────┐
 │             │             │
 ▼             ▼             ▼

Log Agent   Dependency   Impact Agent
            Agent

               │
               ▼

         Resolution Agent

               │

      ┌────────┼────────┐
      ▼                 ▼

    Slack            Jira
```

## Event Flow

### Step 1: Airflow failure occurs

Example:

- `daily_sales_pipeline`
- Status: FAILED
- Error: ExecutorLostFailure

Airflow callback publishes event.

```json
{
  "dag_id": "daily_sales_pipeline",
  "status": "FAILED",
  "execution_date": "2026-06-14",
  "log_path": "s3://logs/airflow/123.log"
}
```

### Step 2: Kafka Topic

Topic:

- `pipeline_failure_events`

All failures go here.

### Step 3: Spark Streaming

Consumes failures and enriches them with:

- DAG metadata
- Owner
- SLA
- Downstream tables
- Business domain

Stores enriched records in:

- OpenSearch for semantic incident search
- Incident DB for reporting and MTTR
- S3 logs for raw capture

## Incident DB Example

Columns:

- `incident_id`
- `dag_id`
- `status`
- `root_cause`
- `resolution`
- `timestamp`

## Agent Architecture

Use LangGraph.

### Supervisor Agent

Responsible for orchestration.

Flow:

```text
Failure Event
      │
      ▼
Supervisor
      │
 ┌────┼────┐
 ▼    ▼    ▼

Log  Dependency Impact
Agent 1: Log Analysis Agent

Most important agent.

Inputs:

- Airflow logs
- Spark logs


Tool:

- `get_logs()`

Example output:

```json
{
  "error_type": "OOM",
  "confidence": 0.94
}
```

### Agent 2: Dependency Agent

Checks:

- Upstream tables
- Kafka lag
- Late partitions
- Missing files

### Agent 3: Impact Agent

Determines business impact and severity based on downstream dependencies.

Example output:

```json
{
  "severity": "HIGH",
  "affected_dashboards": 12,
  "affected_tables": 5,
  "affected_models": 2
}
```

### Agent 4: Resolution Agent

Suggests the recommended fix and escalations.

The agent uses:

- error type
- root cause
- impact severity
- historical incident data

Example recommendation:

- Repartition data by `customer_id` before aggregation
- Increase Spark executor memory
- Open a Jira ticket if the incident is critical

### Notifications

After a resolution recommendation, the system can notify:

- Slack
- Jira

This completes the high-level project architecture and agent workflow.
```
