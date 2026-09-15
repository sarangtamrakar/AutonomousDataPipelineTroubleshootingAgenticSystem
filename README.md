# Autonomous Data Pipeline Troubleshooting System

An agentic root-cause-analysis system for data pipelines. When an Airflow task fails, a LangGraph multi-agent workflow automatically gathers diagnostics across Spark, Airflow and S3, traverses data lineage to find upstream causes and downstream impact, and produces a synthesized incident report — replacing the manual log-hunting that normally follows a pipeline failure.

The same service also exposes a natural-language analytics endpoint over the pipeline's gold-layer tables via Athena.

> **Scope note:** This is a self-contained reference implementation running on Docker Compose (Airflow + Spark master + 2 workers) against real AWS S3/Athena. Workload figures below marked *target* describe what the architecture is designed for, not measured production traffic. Figures marked *measured* were benchmarked on this local setup.

---

## Contents

- [What it does](#what-it-does)
- [Architecture](#architecture)
- [Design decisions](#design-decisions)
- [Implementation notes](#implementation-notes)
- [Running it](#running-it)
- [Measured performance](#measured-performance)
- [Known limitations](#known-limitations)

---

## What it does

A data platform team running Airflow + Spark hits the same failure loop repeatedly:

- A DAG task fails at 3am. Someone reads Airflow logs, finds nothing conclusive.
- They open the Spark History UI, hunt for the right application ID, check stage metrics.
- They check whether an upstream table actually landed.
- They try to work out which dashboards are now stale.

Each of these is a different system with a different interface. This project automates the whole loop.

**Incident flow** — an Airflow failure callback fires, marks the asset `FAILED` in a lineage database, and POSTs to a FastAPI endpoint. A LangGraph graph then runs three diagnostic subgraphs in parallel and synthesizes their outputs into a single markdown report.

**Analytics flow** — a separate endpoint translates natural-language questions into Presto SQL, executes against Athena with read-only validation, and returns the answer alongside the SQL used.

---

## Architecture

### Incident response

```
Airflow task failure
        │
        ├──► task_failure_callback()
        │       ├─ update lineage_db → FAILED
        │       └─ regex-extract spark_app_id from task logs
        │
        ▼
POST /trigger-incident  ──►  202 Accepted (fire-and-forget)
        │
        ▼
LangGraph: check_status_node (gatekeeper)
        │
        ├─ status == success ──────────────────► END
        │
        └─ status == failed ──► fan out in parallel
                                    ├──► rca_node        (Spark + log diagnostics)
                                    ├──► dependency_node (upstream lineage)
                                    └──► impact_node     (downstream blast radius)
                                              │
                                              ▼
                                        report_node (LLM synthesis)
                                              │
                                              ▼
                                        Slack / email
```

### Data platform

A medallion architecture on Delta Lake over S3:

| Layer | Contents | Write mode |
|---|---|---|
| Raw | Source CSVs | — |
| Bronze | Untransformed Delta copies | Append |
| Silver | Deduplicated, null-filtered, timestamped | Overwrite (idempotent) |
| Gold | Star schema — `dim_customer`, `dim_product`, `fact_sales` | Delta `MERGE` (idempotent) |

Gold dimensions use SCD Type 2 (`current_flag`, `effective_date`). Gold tables are registered in the Glue catalog and queried through Athena.

### Metadata

PostgreSQL holds `data_assets` (asset status), `data_lineage` (upstream → downstream edges), and `incident_history`. Lineage traversal uses recursive CTEs in both directions.

---

## Design decisions

| Decision | Rationale |
|---|---|
| Airflow over Prefect/Dagster | Mature callback API; `on_failure_callback` is the integration point the whole system hangs off |
| Delta Lake over plain Parquet | ACID `MERGE` gives retry-safe fact ingestion; time travel helps forensics |
| LangGraph over a ReAct agent | Diagnostics are a known, fixed workflow — a deterministic state graph is more debuggable than letting an LLM pick tools, and gives parallel fan-out for free |
| Deterministic tool calls, LLM only for synthesis | All diagnostic data is gathered by explicit Python calls. The LLM never decides *what* to fetch, only how to interpret it. Removes a large class of agent failure modes |
| `temperature=0` | Reproducible reports; makes regression testing possible |
| Fire-and-forget endpoint | RCA takes 12–15s. A synchronous call would risk Airflow callback timeouts |
| Ephemeral Spark drivers | Each job runs in a fresh `docker run --rm` container in client mode, so the Airflow worker never accumulates driver memory |
| Athena over Redshift | Serverless; no cluster to keep warm for an intermittent Q&A workload |

---

## Implementation notes

### Three-layer diagnostics

No single source explains a Spark failure, so the RCA subgraph gathers from three:

| Layer | Source | Detects |
|---|---|---|
| Driver | Airflow `TaskLogReader` + regex | Python exceptions, exit codes, OOM kills |
| Stage | Spark History REST API | Data skew, task duration variance, memory spill |
| Executor | Spark event logs on S3 | Executor death (exit 137), network failures |

Each tool call is individually wrapped in try/except. A failed Spark History call degrades the report rather than failing the incident — partial diagnostics still produce a useful root cause.

### Skew detection

Spark does not raise an error on data skew; the stage simply hangs until timeout. Detection is percentile-based over task durations within a stage:

```python
median = percentile(durations, 50)
p95    = percentile(durations, 95)

if p95 > SKEW_THRESHOLD * median:   # SKEW_THRESHOLD = 5
    return {"skew_detected": True, "skew_ratio": p95 / median}
```

The 5× threshold was chosen by observing that legitimate task-duration variance on the sample workloads sat around 2×. It is configurable and would need retuning per workload.

### Lineage traversal

Upstream and downstream traversal both use a single recursive CTE rather than iterative queries:

```sql
WITH RECURSIVE upstream AS (
    SELECT upstream_asset_id FROM data_lineage
    WHERE downstream_asset_id = %s
  UNION ALL
    SELECT dl.upstream_asset_id FROM data_lineage dl
    JOIN upstream u ON dl.downstream_asset_id = u.upstream_asset_id
)
SELECT asset_id, asset_name, status, last_updated_at
FROM data_assets WHERE asset_id IN (SELECT upstream_asset_id FROM upstream);
```

This replaces an N+1 pattern — one query per lineage depth level — with one round trip regardless of chain depth.

### Spark application ID extraction

Airflow doesn't surface the Spark application ID, and without it the Spark History API is unreachable. The failure callback reads the task's own log stream and regex-matches the `app-<timestamp>-<seq>` pattern before triggering RCA. If no match is found, RCA proceeds on Airflow logs alone.

### NL→SQL safety

SQL generation uses structured output (Pydantic `GeneratedSQL` with `rationale` and `sql_query` fields), so every query carries the model's stated reasoning for auditing. Execution is gated by an allowlist — anything not beginning with `SELECT` is rejected before it reaches Athena — with IAM read-only policies as the second layer.

### Preventing tool/root-cause confusion

Early reports would name a diagnostic function as the root cause ("the issue is in `spark_get_stages`"). The synthesis prompt now explicitly separates diagnostic utilities from pipeline dependencies, and distinguishes tool errors from data failures.

---

## Running it

**Prerequisites:** Docker + Docker Compose, an AWS account with S3 and Athena access, and an LLM API key (Groq, or Ollama for local inference).

```bash
git clone https://github.com/sarangtamrakar/AutonomousDataPipelineTroubleshootingAgenticSystem
cd AutonomousDataPipelineTroubleshootingAgenticSystem

cp .env.example .env
# set AWS creds, S3 bucket, Athena workgroup, LLM provider + key

docker compose up -d          # Airflow, Spark master + 2 workers, Postgres, FastAPI
python scripts/seed_lineage.py    # populate data_assets / data_lineage
```

Airflow UI at `localhost:8080`, RCA service at `localhost:8000`.

Trigger `master_medallion_pipeline_flat` to run the bronze → silver → gold pipeline. To see the RCA path, introduce a failure (a schema mismatch in a silver script works) and watch the incident report appear.

Ask an analytics question:

```bash
curl -X POST localhost:8000/chat \
  -H "Content-Type: application/json" \
  -d '{"user_message": "What are total sales by region?"}'
```

---

## Measured performance

Benchmarked on the local Docker setup described above, across 20 induced failure runs:

| Stage | Latency |
|---|---|
| Fetch Airflow driver logs | ~0.5s |
| Spark History API (stages + executors) | ~0.8s |
| Read Spark event logs from S3 | ~1.2s |
| LLM synthesis | 5–10s |
| **End-to-end RCA** | **12–15s** |
| NL→SQL generation | 2–3s |
| Athena execution (sample dataset) | 1–5s |

Parallel fan-out saves roughly 1.3s against sequential execution — the LLM synthesis step dominates, so the gain is modest at this scale and grows with the number of diagnostic subgraphs.

Container footprint: ~25GB RAM, ~16 cores across all services.

**Target scale** (designed for, not tested at): ~500 DAGs and a lineage graph of ~5,000 assets, via Airflow CeleryExecutor, Spark dynamic allocation (2–16 executors) and LangGraph Postgres checkpointing.

---

## Known limitations

- **No evaluation harness.** RCA report quality is assessed manually. A labelled set of failure scenarios with expected `failure_category` values would let this be measured properly — the most valuable next addition.
- **Skew threshold is a heuristic.** 5× worked on the sample workloads; it is not derived from anything principled.
- **Lineage DB can go stale.** Status is written by Airflow callbacks only. A periodic reconciliation job against Airflow's own metadata would close the gap.
- **Cold-start latency.** The first LLM call after idle adds ~5s.
- **Spark History API is fragile under load.** Currently handled by degrading to S3 event logs; a proper circuit breaker would be better.
- **Single-tenant.** No auth on the FastAPI endpoints; assumes deployment inside a trusted network.

---

## Repository layout

| Path | Contents |
|---|---|
| `batch_pipeline/` | Bronze / silver / gold Spark jobs |
| `airflow_pipeline/` | DAG definition and failure/success callbacks |
| `rca_agent_workflow/` | LangGraph graphs, tools, FastAPI app |
| `rca_agent_workflow/graphs/` | `rca_graph`, `dependency_graph`, `impact_graph`, `analytics_graph` |
| `scripts/` | Seeding and utility scripts |
| `docs/` | Diagrams and supporting notes |
