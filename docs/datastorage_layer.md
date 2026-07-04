# Data Storage Layer

## Overview

This layer describes how data flows from Airflow failures into incident tracking, storage, and search systems.

## Step 1: Airflow DAG Fails

Suppose:

- `daily_sales_pipeline` fails at 2 AM.

Airflow sends an event:

```json
{
  "dag_id": "daily_sales_pipeline",
  "status": "FAILED",
  "run_id": "12345"
}
```

via:

- Kafka
- Airflow Callback API

## Step 2: What Spark Streaming Was Doing

I originally suggested:

```text
Kafka
  ↓
Spark Streaming
```

Because many companies have thousands of events per minute.

Spark Streaming would:

- Enrich event
- Add metadata

Example enriched event:

```json
{
  "dag_id":"daily_sales_pipeline",
  "owner":"finance_team",
  "criticality":"HIGH",
  "status":"FAILED"
}
```

Save to:

- Qdrant
- Incident DB
- S3 Logs


## What is Incident DB?

This is simply a database table.

Example:

```sql
CREATE TABLE incidents(
    incident_id STRING,
    dag_id STRING,
    status STRING,
    root_cause STRING,
    created_at TIMESTAMP
);
```

Purpose:

- Track all incidents
- Generate reports
- Measure MTTR
- Count failures

Example queries:

```sql
SELECT count(*)
FROM incidents
WHERE status='FAILED';

SELECT dag_id,
       count(*)
FROM incidents
GROUP BY dag_id;
```

## What is S3 Logs?

When Airflow fails, its logs may be stored in:

- `s3://airflow-logs/daily_sales/`

Example log:

```text
ExecutorLostFailure
java.lang.OutOfMemoryError
```

Your Log Analysis Agent reads these logs.

## Why does LangGraph need all three?

Because each source answers a different question:

- **S3 Logs**: What exactly failed?
  - Answer: OOM
- **Incident DB**: How often does this happen?
  - Answer: 12 times last month
- **OpenSearch**: Have we seen this before?
  - Answer: Yes

Resolution was increasing executor memory.
