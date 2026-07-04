# AWS + Native Infrastructure

## Final Architecture

### AWS = System of Record

AWS owns:
- Data Lake
- Metadata
- Incident History
- Logs
- Lineage
- Monitoring

### Local MacBook = AI Brain

Local owns:
- LangGraph
- Gemini
- Qdrant
- FastAPI
- Slack/Jira integrations

---

## Architecture Diagram

```text
                     AWS

               ┌─────────────┐
               │ Kafka (MSK) │
               └──────┬──────┘
                      │
                      ▼

                EMR Spark Jobs

                      │

        ┌─────────────┼─────────────┐
        │             │             │

        ▼             ▼             ▼

 Bronze Delta    Silver Delta   Gold Delta

        │             │             │

        └─────────────┼─────────────┘
                      │

                      ▼

                S3 Lakehouse

                      │

                Airflow (MWAA)

                      │

                DAG Failure

                      │

                      ▼

                SQS Queue
          pipeline_failures

                      │

       ┌──────────────┼──────────────┐
       │              │              │

       ▼              ▼              ▼

 CloudWatch      Glue Catalog    RDS Postgres
    Logs          Metadata       Incident DB

                      │

---------------- INTERNET ----------------

                      │

                      ▼

                Local MacBook

                LangGraph

                      │

     ┌────────┬────────┬────────┬────────┐

     ▼        ▼        ▼        ▼

   Log      RCA     Impact   Resolution
  Agent    Agent    Agent     Agent

                      │

                      ▼

                    Gemini

                      │

                      ▼

                    Qdrant

                      │

             Slack / Jira
```

---

## AWS Service Selection

### Data Lake

Use:
- Amazon S3

Structure:
- `s3://lakehouse/`
  - `bronze/`
  - `silver/`
  - `gold/`

Delta Lake format.

### Spark

Use:
- Amazon EMR

Jobs:
- `bronze_job`
- `silver_job`
- `gold_job`

Write Delta tables.

### Orchestration

Use:
- Amazon MWAA

DAG:
- `bronze`
  - `silver`
  - `gold`

### Failure Event Bus

Use:
- Amazon SQS

Queue:
- `pipeline_failures`

Why SQS instead of Kafka?
- Because your AI agents are outside AWS.

Benefits:
- Simple
- Cheap
- Durable
- Easy local consumption

Airflow callback:
- `send_message_to_sqs()`

### Logs

Use:
- Amazon CloudWatch

Store:
- Airflow logs
- Spark logs
- Application logs

Log Agent reads:
- `boto3.client("logs")`

### Metadata

Use:
- AWS Glue Data Catalog

Stores:
- `bronze_sales`
- `silver_sales`
- `gold_sales`

Dependency Agent queries:
- `glue.get_table(...)`

### Incident Database

Use:
- Amazon RDS PostgreSQL

Tables:
- `incidents`
- `pipeline_runs`
- `sla_history`

Example schema:
- `incident_id`
- `dag_id`
- `root_cause`
- `resolution`
- `severity`

This becomes your historical truth.

### Lineage

#### Version 1

Use Postgres tables.

Table:
- `lineage_edges`
  - `source_asset`
  - `target_asset`

Example:
- `bronze_sales` → `silver_sales` → `gold_sales` → `executive_dashboard`

Impact Agent traverses lineage.

#### Version 2 (FAANG Upgrade)

Use:
- Amazon Neptune

Store:
- lineage graph

Impact Agent executes graph traversal.

Example:
- `sales_gold` failed
- Find all downstream assets

Result:
- `Dashboard A`
- `Dashboard B`
- `ML Model C`
- `Local AI Layer`

This is where your project becomes interesting.

### LangGraph

Supervisor receives event from SQS:

```json
{
  "dag_id": "sales_gold",
  "status": "FAILED"
}
```

### Log Agent

Reads:
- CloudWatch Logs

Tool:
- `get_cloudwatch_logs()`

Output:
```json
{
  "error": "OOM"
}
```

### RCA Agent

Reads:
- EMR metadata
- CloudWatch logs
- Spark metrics

Output:
```json
{
  "root_cause": "Data Skew"
}
```

### Impact Agent

Reads:
- RDS
- Glue Catalog
- Lineage Graph

Output:
```json
{
  "severity": "HIGH",
  "affected_assets": 15
}
```

### Resolution Agent

Reads:
- Historical incidents
- Qdrant memory

Finds:
- Similar Incident `#101`

Recommendation:
```json
{
  "fix": "Repartition by customer_id"
}
```

### Qdrant

Stores embeddings for:
- Root Cause
- Error Type
- Resolution
- Runbooks
- Incidents

Example embeddings:
- `OOM`
- `Data Skew`
- `Repartition`

Embedding source:
- Gemini Embedding

Stored in Qdrant.

### Gemini

Use:
- Google AI Studio

Models:
- `gemini-2.5-flash`

For:
- Log Analysis
- RCA
- Resolution Suggestions

Embedding:
- `text-embedding-004`

For Qdrant.

### Notification Layer

Slack:
- `🚨 Pipeline Failure`
- `Pipeline: sales_gold`
- `Root Cause: Data Skew`
- `Severity: HIGH`
- `Recommended Fix: Repartition by customer_id`

Jira:
- Create Ticket
- Assign Owner
- Set Priority

---

## What Happens During a Real Failure?

1. Airflow DAG fails.
   - `sales_gold`
2. MWAA callback sends event to SQS.

```json
{
  "dag_id": "sales_gold",
  "status": "FAILED"
}
```

3. Local LangGraph polls SQS.
4. Log Agent reads CloudWatch and reports `OOM`.
5. RCA Agent reads EMR metrics and identifies `Data Skew`.
6. Impact Agent traverses lineage and finds `12 dashboards affected`.
7. Resolution Agent queries Qdrant and finds a previous similar incident.
   - Recommendation: `Repartition customer_id`
8. Slack alert is sent.
9. Jira ticket is created.
10. Incident is saved to RDS.
11. Embedding is saved to Qdrant.
