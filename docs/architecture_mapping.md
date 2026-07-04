# Architecture Mapping

This table compares a local Docker-based architecture with AWS-native alternatives.

| Function               | Local Docker               | AWS Native              |
| ---------------------- | -------------------------- | ----------------------- |
| Workflow Orchestration | Airflow                    | MWAA or Airflow on EKS  |
| Event Bus              | Kafka                      | MSK                     |
| Streaming              | Spark Structured Streaming | EMR Serverless / EMR    |
| Data Lake              | MinIO + Delta              | S3 + Delta/Iceberg      |
| Incident DB            | PostgreSQL                 | RDS PostgreSQL          |
| Log Storage            | Local files / MinIO        | CloudWatch + S3         |
| Search                 | OpenSearch Docker          | OpenSearch Service      |
| Agents                 | LangGraph                  | Bedrock + LangGraph     |
| Embeddings             | Ollama/OpenAI              | Bedrock Titan           |
| Vector Search          | OpenSearch                 | OpenSearch              |
| Monitoring             | Prometheus + Grafana       | CloudWatch + AMP + AMG  |
| Metadata               | OpenMetadata               | DataZone / Glue Catalog |
| Lineage                | OpenMetadata               | DataZone / Neptune      |
| Alerting               | Slack                      | Slack + SNS             |
| Ticketing              | Jira API                   | Jira API                |
| Cost Analysis          | Custom Python              | Cost Explorer API       |
