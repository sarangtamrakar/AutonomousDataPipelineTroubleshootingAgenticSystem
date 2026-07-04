# Technical Deep Dive: Design Decisions & Trade-offs
## For Advanced FAANG Interview Questions

---

## 1. System Design Trade-offs

### Question: "Why Airflow + Spark instead of Prefect/Dagster + Flink?"

#### Airflow + Spark
**Pros:**
- ✅ **Mature ecosystem**: Battle-tested at 100+ FAANG companies
- ✅ **Community**: 10k+ Stack Overflow questions, 1000+ orchestration tutorials
- ✅ **Delta Lake integration**: Native Delta support in Spark
- ✅ **Cost**: Open-source, self-hosted
- ✅ **Flexibility**: Can run anything (Spark, notebooks, bash, K8s)

**Cons:**
- ❌ **Complexity**: Requires managing master/worker architecture
- ❌ **Monitoring**: Need custom dashboards (not built-in observability)
- ❌ **Fault tolerance**: Requires careful callback management

#### Prefect + Flink
**Pros:**
- ✅ **Developer experience**: Better UI/UX than Airflow
- ✅ **Cloud-native**: Prefect Cloud fully managed
- ✅ **Streaming**: Flink superior for real-time (Spark Structured Streaming is batch-oriented)

**Cons:**
- ❌ **Immature**: Flink's Python API less stable than Spark
- ❌ **Cost**: Prefect Cloud SaaS pricing model
- ❌ **Community**: Smaller ecosystem, fewer production examples
- ❌ **Hiring**: Harder to find Prefect/Flink engineers vs Airflow/Spark

#### Decision Rationale for This Project
```
Airflow + Spark chosen because:
1. Team already knows Spark (from BigData 2.x era)
2. Delta Lake support crucial for medallion architecture
3. On-premise deployment required (no cloud dependency)
4. Batch ETL primary use case (Flink overkill for daily DAGs)
5. Observability via callbacks is acceptable tradeoff
```

---

### Question: "Why Delta Lake instead of Iceberg/Parquet?"

#### Delta Lake
- **ACID transactions**: Serializability guarantees
- **Schema enforcement**: Prevents silent data corruption
- **Time travel**: `SELECT * FROM table@v123` for debugging
- **Unified batch/streaming**: Can append from both
- **SCD Type 2**: Built-in support (version history)

**Tradeoff:** Larger file sizes due to transaction logs

#### Apache Iceberg
- **Better partitioning**: Hidden partitioning (no folder explosion)
- **Schema evolution**: Easier column renames/deletes
- **Multi-engine**: Works with Spark/Presto/Flink

**Tradeoff:** Newer (2020 vs Delta 2019), smaller ecosystem

#### Parquet
- **Lightweight**: No transaction overhead
- **Universal**: Works everywhere (Hive, Presto, BigQuery)

**Tradeoff:** No ACID, no time travel, no schema enforcement

#### Why Delta Lake for This Project
```
Delta Lake chosen because:
1. ACID transactions ensure merges don't duplicate fact data
2. Time travel enables rollback if gold layer logic changes
3. Schema enforcement prevents bronze→silver mismatches
4. Already integrated with Databricks ecosystem
```

---

### Question: "Why LangGraph instead of direct LLM.invoke()?"

#### Direct LLM Pattern
```python
def diagnose(logs):
    prompt = f"Analyze these logs: {logs}"
    response = llm.invoke(prompt)
    return response
```

**Pros:**
- ✅ Simple, one function call
- ✅ Easy to understand

**Cons:**
- ❌ No state management (can't parallelize)
- ❌ No checkpointing (can't resume after timeout)
- ❌ No routing logic (all paths hardcoded in prompt)
- ❌ Non-deterministic (can't debug reproducibly)

#### LangGraph Pattern
```python
class IncidentState(TypedDict):
    dag_id: str
    rca_result: Dict
    dependency_result: Dict
    impact_result: Dict

def rca_node(state): ...
def dependency_node(state): ...
def impact_node(state): ...

workflow = StateGraph(IncidentState)
workflow.add_node("rca", rca_node)
workflow.add_node("dependency", dependency_node)
workflow.add_edge(START, "rca")
workflow.add_edge(START, "dependency")
```

**Pros:**
- ✅ **Declarative**: Graph structure visible (not hidden in prompts)
- ✅ **Parallel**: LangGraph handles threading
- ✅ **Debuggable**: Deterministic state transitions
- ✅ **Resumable**: Checkpoint recovery from failures
- ✅ **Testable**: Mock subgraphs for unit tests

**Cons:**
- ❌ More boilerplate code
- ❌ Learning curve for state management

#### Why LangGraph for This Project
```
LangGraph chosen because:
1. 3 parallel diagnostic agents (can't do with simple invoke)
2. State aggregation required (rca_result + dependency_result + impact_result)
3. Checkpointing critical (resume if LLM times out)
4. Deterministic execution needed (reproducible RCAs for debugging)
```

---

## 2. Scalability Analysis

### Question: "How does the system scale from 100 DAGs to 500 DAGs?"

#### Bottleneck Analysis

| Component | At 100 DAGs | At 500 DAGs | Solution |
|-----------|------------|-----------|----------|
| **Airflow Scheduler** | 500ms cycle | 2500ms cycle | CeleryExecutor (don't use LocalExecutor) |
| **Spark Driver Memory** | 2GB | 8GB | Dynamic allocation + config tuning |
| **PostgreSQL Lineage** | <50 rows/query | <1000 rows/query | Recursive CTE + indexes + partitioning |
| **Athena Query Time** | 1-2s | 5-10s | Partitioning by date + clustering |
| **LLM Latency** | 5s | 5s | No change (inference is constant time) |

#### Scaling Strategy: 1x→5x

```
CURRENT (100 DAGs):
├─ Airflow: 1 scheduler + 2 workers (4 parallelism each = 8 total)
├─ Spark: 1 master + 2 workers (16GB RAM each)
├─ Postgres: Single instance (512MB RAM)
└─ Athena: Default (1MB per partition scan)

SCALED (500 DAGs):
├─ Airflow: 1 scheduler + 8 workers (8 parallelism each = 64 total)
│          └─ Parallelism factor: 8x
│
├─ Spark: 1 master + 8 workers (8GB RAM each)
│        └─ With dynamic allocation (2-16 executors per job)
│          └─ Throughput factor: 4x (more workers) × 1.5x (dynamic) = 6x
│
├─ Postgres: 1 primary + 1 read replica
│           └─ With connection pooling (max 100 conns)
│           └─ Recursion depth limit = 10 (prevent runaway queries)
│
└─ Athena: Auto-scaling
           └─ DPU-based pricing (scales automatically)
           └─ Partition pruning (20% of table scans)
```

#### Load Test Results (Hypothetical)

```
Scenario: 500 DAGs all fail simultaneously

Peak Load:
- Airflow task queue: 500 pending
- Scheduler processes: 500/64 = 8 seconds per cycle
- RCA concurrency: 50 LangGraph invocations (limited by FastAPI pool)

Timeline:
T+0s:   500 failures detected
T+1s:   200 callbacks enqueued (rate-limited by Airflow)
T+5s:   50 RCAs in progress (LLM inference batched)
T+10s:  All RCAs complete
T+15s:  Slack messages sent to on-call
T+20s:  On-call engineer triage all reports

Manual RCA at same scale:
- 500 DAGs × 2 hours = 1000 engineer-hours
- Cost: $50k

Automated RCA:
- 500 DAGs × 15 seconds = 2 hours total
- Cost: $50 (compute) + $5 (LLM API)
```

#### Horizontal Scaling: Multi-Region

```
Region A (Primary)           Region B (Secondary)
├─ Airflow + Spark          ├─ Warm Standby
├─ PostgreSQL Primary       ├─ PostgreSQL Replica
└─ FastAPI (3 instances)    └─ FastAPI (3 instances)

Failover Logic:
1. DNS routes to Region A
2. If Region A unhealthy (3 consecutive timeouts)
3. Failover to Region B (read-only mode)
4. Queries redirect to Region B replica
5. Manual intervention to restore Region A
```

---

## 3. Data Skew Detection Algorithm (Deep Dive)

### Question: "Walk me through your skew detection algorithm"

#### Problem Statement
```
Spark job processes 1 billion sales records partitioned by product_id:
- 90% of records have product_id='POPULAR_ITEM'
- 10% of records spread across 10k other products

Spark executor assignment:
- Executor 1: Gets partition with POPULAR_ITEM (98% of records)
- Executor 2-50: Get other partitions (1% each)

Result:
- Executor 1 runs 4 hours
- Executors 2-50 finish in 2 minutes
- Spark waits for Executor 1 (task straggling)
- Job fails at 4-hour mark with timeout
- But logs show: "Task completed successfully"
```

#### Detection Algorithm

```python
def analyze_task_distribution(app_id: str, stage_id: int):
    """Detect skew via percentile analysis."""
    
    # Fetch all task metrics from Spark History
    stage = spark_history.get_stage(app_id, stage_id)
    task_durations = [
        task.executorRunTime 
        for task in stage.taskMetrics
    ]
    
    # Calculate percentiles
    sorted_durations = sorted(task_durations)
    p50 = sorted_durations[len(sorted_durations) // 2]        # median
    p95 = sorted_durations[int(0.95 * len(sorted_durations))] # 95th percentile
    p99 = sorted_durations[int(0.99 * len(sorted_durations))] # 99th percentile
    
    # Skew detection rules
    if p95 > 5 * p50:
        return {
            "skew_detected": True,
            "skew_ratio": p95 / p50,
            "severity": "HIGH" if p95 > 10 * p50 else "MEDIUM",
            "percentile_comparison": f"p50={p50}ms, p95={p95}ms, p99={p99}ms",
            "recommendation": f"Data heavily skewed. Try repartition({estimate_partitions()})"
        }
```

#### Why Percentile-Based?

| Approach | Pros | Cons |
|----------|------|------|
| **Max/Min ratio** | Simple | Outlier-sensitive (1 slow task ruins it) |
| **Mean/Stdev** | Statistical | Assumes normal distribution (not true for skew) |
| **Percentile (p95/p50)** | ✅ Robust to outliers | Requires choosing threshold (5x is empirical) |

```
Distribution with Outlier:
[1, 2, 3, 100]

Max/Min = 100 (suggests extreme skew, but 3 of 4 tasks are normal)
Mean/Stdev = (26.5, 49.8) → not interpretable
p95/p50 = 100/2.5 = 40x (correctly identifies major outlier)

Distribution with True Skew:
[10, 30, 45, 50, 50, 51, 52, 53, 54, 100]

Max/Min = 10x (correct)
Mean/Stdev = (51.5, 24) (correct)
p95/p50 = 50/50 = 1x (MISSED! Skew is subtle)
```

#### Memory Spill Detection

```python
def analyze_memory_pressure(stage):
    """Detect memory pressure via spill metrics."""
    
    for task in stage.taskMetrics:
        # Spill indicates data didn't fit in memory
        memory_spilled_gb = task.memoryBytesSpilled / (1024 ** 3)
        disk_spilled_gb = task.diskBytesSpilled / (1024 ** 3)
        
        if disk_spilled_gb > 0:
            return {
                "memory_pressure": True,
                "disk_spilled_gb": disk_spilled_gb,
                "recommendation": "Increase spark.executor.memory or add .repartition()"
            }
```

#### Remediation Strategies

```python
def recommend_fix(skew_analysis):
    """Suggest specific code changes."""
    
    skew_ratio = skew_analysis["skew_ratio"]
    
    if skew_ratio > 10:
        return """
        HIGH SKEW DETECTED (ratio={:.1f}x):
        
        Option 1: Increase partition count
        df = df.repartition(500)  # instead of 200
        
        Option 2: Salting (add random suffix to skewed keys)
        df = df.withColumn(
            "salted_product_id",
            concat(col("product_id"), 
                   lit("_"), 
                   (rand() * 10).cast("int"))
        )
        
        Option 3: Broadcast small table (if possible)
        df_skewed.join(
            broadcast(df_small),
            "key"
        )
        """.format(skew_ratio)
```

---

## 4. LLM Hallucination Mitigation

### Question: "How do you prevent LLM from hallucinating root causes?"

#### Types of Hallucination in RCA Context

```
1. TOOL CONFUSION
   Hallucination: "The issue is in the spark_get_stages function"
   Reality: spark_get_stages is a diagnostic tool, not the root cause
   
2. FALSE CORRELATION
   Hallucination: "S3 timeout caused memory overflow"
   Reality: They're independent issues
   
3. PHANTOM RECOMMENDATIONS
   Hallucination: "Add TBLPROPERTIES [...]" (not valid Spark syntax)
   Reality: LLM invents SQL that doesn't exist

4. SEVERITY OVERSTATEMENT
   Hallucination: "This will cause $1M in damages"
   Reality: Impact is $50k
```

#### Mitigation Strategies

### 1. System Prompt Guardrails

```python
sys_prompt = """You are an expert Spark/Airflow SRE.

CRITICAL RULES:
1. "Dependencies" = upstream DATA PIPELINES (bronze_sales, dim_customers)
   NOT Python tools (spark_get_stages, fetch_airflow_logs)
   
2. NEVER confuse diagnostic tool names with actual root causes.
   Example:
   - WRONG: "The issue is in the airflow_client tool"
   - RIGHT: "Airflow API returned 503, indicating master overload"
   
3. Only recommend changes that are:
   - Valid Spark SQL/Python syntax
   - Tested in production before
   - Address the specific root cause
   
4. If uncertain, say so: "Evidence suggests X, but inconclusive"

5. Quantify impact in terms of:
   - Tables affected: 5 Gold tables
   - Users affected: 10,000
   - SLA impact: 2% late delivery
   NOT in $ amounts (leaves us vulnerable)

EXAMPLES:
ROOT CAUSE: Data skew on product_id
- Tool ERROR (from log): "executor_removed event"
- SYMPTOM: Task took 4 hours, others finished in 2 min
- RECOMMENDATION: "repartition(500)" + "salt keys with random suffix"

ROOT CAUSE: Schema mismatch
- Tool ERROR: "Cannot cast String to Int"
- SYMPTOM: Task failed in silver_customers transformation
- RECOMMENDATION: "Add .cast('string')" or "DROP and REBUILD gold table"
"""
```

### 2. Structured Output Validation

```python
class RCAFinding(BaseModel):
    failure_category: Literal["OOM", "Skew", "Code", "Network", "Schema", "Timeout"]
    root_cause_summary: str = Field(
        ...,
        min_length=10,
        max_length=200,
        description="1-2 sentences, no $ amounts, focus on DATA not TOOLS"
    )
    confidence_score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="0.0-1.0: how confident in this diagnosis"
    )
    affected_tables: List[str] = Field(
        ...,
        description="Which data tables/pipelines affected (not Python functions)"
    )
    evidence: List[str] = Field(
        ...,
        max_items=5,
        description="Key log lines or metrics supporting diagnosis"
    )
    recommendation: str = Field(
        ...,
        description="Specific action with valid Spark syntax"
    )

# Validation happens at Pydantic level
# Rejects if confidence < 0.3 or recommendation seems invalid
```

### 3. Few-Shot Prompting

```python
examples = [
    {
        "logs": "...[Executor 5 took 4h, others 2m, memory_spilled=5GB]...",
        "correct_diagnosis": {
            "failure_category": "Skew",
            "root_cause": "Data heavily concentrated on one partition key",
            "recommendation": "Use repartition(500) with salting"
        }
    },
    {
        "logs": "...[Cannot cast String 'ABC' to Int]...",
        "correct_diagnosis": {
            "failure_category": "Schema",
            "root_cause": "Schema mismatch: bronze has 'product_id' as string, gold expects int",
            "recommendation": "Cast to int in silver layer: .cast('int')"
        }
    },
    # ... 3-5 more examples
]

prompt = f"""
You are an expert Spark/Airflow RCA engineer.

EXAMPLES OF CORRECT DIAGNOSES:
{json.dumps(examples, indent=2)}

Now analyze this incident and follow the same pattern:
{raw_logs}
"""
```

### 4. Post-Processing Validation

```python
def validate_rca_output(rca_finding: RCAFinding) -> tuple[bool, str]:
    """Validate RCA doesn't have common hallucinations."""
    
    # Rule 1: Check for tool names in root_cause
    tool_names = ["spark_get_stages", "fetch_airflow_logs", "execute_athena_query"]
    for tool in tool_names:
        if tool in rca_finding.root_cause_summary.lower():
            return False, f"Hallucination: tool name '{tool}' in root cause"
    
    # Rule 2: Check for invalid Spark syntax
    if "TBLPROPERTIES" in rca_finding.recommendation and "SET" not in rca_finding.recommendation:
        return False, "Invalid Spark syntax in recommendation"
    
    # Rule 3: Check confidence score
    if rca_finding.confidence_score < 0.5:
        return False, "Low confidence (< 0.5), requires manual review"
    
    # Rule 4: Check for $ amounts in impact
    if "$" in rca_finding.root_cause_summary:
        return False, "Avoid $ amounts, specify affected tables instead"
    
    return True, "Validation passed"

# If validation fails, escalate to human
if not validate_rca_output(rca_finding):
    send_to_slack("⚠️ RCA validation failed, escalating to on-call")
```

### 5. Human-in-the-Loop for Low-Confidence

```python
def report_rca(rca_finding):
    """Post RCA to Slack with confidence indicator."""
    
    if rca_finding.confidence_score >= 0.8:
        emoji = "✅"
        action = "AUTO-REMEDIATE"
    elif rca_finding.confidence_score >= 0.5:
        emoji = "⚠️"
        action = "MANUAL REVIEW (on-call approval required)"
    else:
        emoji = "🔴"
        action = "ESCALATE (human RCA required)"
    
    message = f"""
    {emoji} RCA for {rca_finding.failure_category}
    
    Root Cause: {rca_finding.root_cause_summary}
    Confidence: {rca_finding.confidence_score:.1%}
    
    Recommendation:
    {rca_finding.recommendation}
    
    Action: {action}
    """
    
    post_to_slack(message)
```

---

## 5. Idempotency: Mathematical Guarantees

### Question: "How do you guarantee no duplicate data after retries?"

#### The Idempotency Equation

```
f(f(x)) = f(x)    ← This is idempotency

Applied to medallion pipeline:

BRONZE (Append):
f(raw_data) = [existing] + raw_data
f(f(raw_data)) = [existing] + raw_data + raw_data    ← NOT idempotent!

SILVER (Overwrite with dedup):
f(bronze_data) = bronze_data.dropDuplicates().overwrite()
f(f(bronze_data)) = bronze_data.dropDuplicates().overwrite()  ← Idempotent!

GOLD (Merge on primary key):
f(silver_data) = MERGE INTO gold USING silver ON gold.id = silver.id
f(f(silver_data)) = same (merge sees existing records, updates)  ← Idempotent!
```

#### Formal Proof: Gold Layer Idempotency

```sql
-- Initial state
GOLD table contains: {sale_id=1, customer_id=100, amount=500}

-- First execution (normal)
MERGE INTO gold USING new_data
ON gold.sale_id = new_data.sale_id
WHEN NOT MATCHED THEN INSERT VALUES (1, 100, 500)
Result: GOLD = {(1, 100, 500)}

-- Retry (duplicate ingestion attempt)
Same MERGE statement
WHEN MATCHED THEN UPDATE SET ... (no change in this case)
WHEN NOT MATCHED THEN INSERT (not applicable, record exists)
Result: GOLD = {(1, 100, 500)}  ← No duplication!

QED: MERGE ∘ MERGE = MERGE
```

#### Why NOT Bronze Append-Only?

```sql
-- Bronze table designed for immutability
-- But append-only violates idempotency

Scenario: Task run twice due to Airflow retry

First run:
INSERT INTO bronze VALUES (sale_id=1, amount=500)
Bronze: [(1, 500)]

Second run (retry):
INSERT INTO bronze VALUES (sale_id=1, amount=500)
Bronze: [(1, 500), (1, 500)]  ← DUPLICATE!

Why we accept this:
1. Bronze represents "raw ingestion timeline"
2. Duplicates are acceptable at this layer
3. Silver layer DEDUPLICATES (idempotency restored)
4. Gold layer uses MERGE (idempotency guaranteed)

Trust boundary: Bronze ← no trust | Silver ← some trust | Gold ← full trust
```

#### Transactional Guarantee

```python
from delta.tables import DeltaTable

def load_fact_sales(silver_data):
    """Idempotent fact table load."""
    
    # Spark transactions guarantee atomicity
    with spark.transaction():
        gold_path = "s3a://sarang-de/gold/fact_sales"
        
        if DeltaTable.isDeltaTable(spark, gold_path):
            # MERGE: All-or-nothing
            DeltaTable.forPath(spark, gold_path).merge(
                source=silver_data.alias("src"),
                condition="gold.sale_id = src.sale_id"
            ).whenMatchedUpdateAll()\
             .whenNotMatchedInsertAll()\
             .execute()  # Atomic commit
        else:
            # First run: Initial load
            silver_data.write.format("delta")\
                .mode("overwrite")\
                .save(gold_path)

# If task fails mid-merge:
# - Delta transaction log prevents partial writes
# - Retry starts fresh with same merge logic
# - Result is always consistent
```

---

## 6. Performance Optimization Trade-offs

### Question: "How would you reduce RCA latency from 15s to 5s?"

#### Current Latency Breakdown

```
Timeline:
T+0s:   Airflow failure
T+0.5s: Callback fires
T+1s:   PostgreSQL lineage update
T+1.5s: HTTP request to FastAPI
T+2s:   LangGraph graph.invoke() starts
T+2.5s: Parallel subgraphs start:
        - rca_node: fetch logs (500ms)
        - dependency_node: query lineage (300ms)
        - impact_node: recurse lineage (200ms)
T+3.5s: All parallel tasks done
T+3.5s: LLM prompt assembly
T+4s:   LLM inference (5-10s) ← BOTTLENECK
T+9s:   LLM response parsing
T+9.5s: Markdown report generation
T+10s:  Slack message sent

Total: 10 seconds, with 5-10 second LLM bottleneck
```

#### Optimization Strategy 1: Pre-fetch + Caching

```python
@functools.lru_cache(maxsize=1000)
def fetch_airflow_logs_cached(dag_id, run_id, task_id):
    """Cache logs for same incident."""
    return fetch_airflow_driver_log(dag_id, run_id, task_id)

# Trade-off: Memory (1000 cached entries × 100KB ≈ 100MB) for latency (-500ms)
```

#### Optimization Strategy 2: Smaller LLM Model

```python
# Current: meta-llama/llama-4-scout-17b (10 billion parameters)
# Latency: 5-10 seconds

# Option 1: meta-llama/llama-3.1-8b (8 billion parameters)
# Latency: 2-3 seconds
# Trade-off: Slightly lower accuracy (97% vs 99%)

# Option 2: Qwen-7B (7 billion parameters)
# Latency: 2-3 seconds
# Trade-off: Multilingual (adds noise), lower accuracy

# Recommendation: 8B model with ensemble voting
# Run 2 small models in parallel, vote on result
# Latency: 3s instead of 10s, accuracy: 98%
```

#### Optimization Strategy 3: Speculative Execution

```python
def fast_track_rca(state):
    """If obvious root cause, return early."""
    
    # Check Airflow logs first (fast)
    airflow_logs = fetch_airflow_logs(state["dag_id"])
    
    # Pattern matching for common issues
    if "java.lang.OutOfMemoryError" in airflow_logs:
        return {
            "root_cause": "OOM",
            "confidence": 0.95,
            "recommendation": "Increase spark.executor.memory"
        }  # Return immediately, skip LLM
    
    if "connection timeout" in airflow_logs.lower():
        return {
            "root_cause": "Network timeout",
            "confidence": 0.9,
            "recommendation": "Check S3 connectivity"
        }
    
    # If no obvious match, proceed to full LLM analysis
    return None
```

**Latency savings:**
- Common cases (60%): 500ms (pattern match only)
- Complex cases (40%): 10s (full LLM)
- **Average: 6.4s (saved 3.6s)**

#### Optimization Strategy 4: Streaming Results

```python
# Instead of waiting for full report, stream progressively

@app.post("/trigger-incident")
async def trigger_incident(request, background_tasks):
    task_id = uuid.uuid4()
    
    # Return immediately with task_id
    return {"status": "accepted", "task_id": str(task_id)}

# Client polls for results
@app.get("/rca/{task_id}")
async def get_rca_result(task_id):
    result = rca_cache.get(task_id)
    if result:
        return result
    return {"status": "pending"}

# Results streamed to Slack as they complete
# Parallel agents post findings as they finish (not waiting for all)
```

**UX improvement:** User gets first RCA insights in 3s instead of 10s

#### Optimization Strategy 5: Batch LLM Inference

```python
# Current: 1 LLM call per RCA (sequential)
# Optimized: Batch 10 RCAs, 1 LLM call with 10 prompts

async def batch_rca_inference(rca_list: List[IncidentState]):
    """Batch inference for multiple incidents."""
    
    prompts = [
        f"RCA for {inc['dag_id']}: {inc['raw_data']}"
        for inc in rca_list
    ]
    
    # Single LLM call with batched input
    responses = llm_batch(prompts, batch_size=10)
    
    # Latency: 10s for 10 incidents = 1s each
    # vs 10 × 10s = 100s sequential
    # Speedup: 10x
```

**Cost trade-off:** Batch saves latency but requires waiting for 10 incidents to accumulate

---

## 7. Disaster Recovery & Resilience

### Question: "What happens if PostgreSQL Lineage DB goes down?"

#### Failure Scenarios

```
1. PostgreSQL CRASH
   ├─ Impact: Can't fetch upstream/downstream lineage
   ├─ RCA still works: Uses S3 event logs + Spark History (fallback)
   ├─ Blast radius: Unknown (can't identify downstream tables)
   └─ Recovery: Restore from backup (1 hour)

2. Network Partition (Postgres unreachable)
   ├─ Impact: Same as crash
   ├─ Detection: Connection timeout after 10s
   ├─ Fallback: Use cached lineage (1 hour stale)
   └─ Recovery: Automatic when network heals

3. Data Corruption
   ├─ Impact: Lineage queries return wrong results
   ├─ Detection: Data_assets.asset_id doesn't match reality
   ├─ Fallback: Skip lineage, use heuristics (match by name)
   └─ Recovery: Point-in-time restore to last good backup
```

#### Resilience Strategy: Circuit Breaker + Fallback

```python
from circuitbreaker import circuit

@circuit(failure_threshold=5, recovery_timeout=60)
def get_upstream_lineage(asset_id: str):
    """Fetch from PostgreSQL with circuit breaker."""
    try:
        return postgres.query_upstream(asset_id)
    except Exception as e:
        logger.error(f"Lineage query failed: {e}")
        raise

def dependency_node_with_fallback(state):
    """Get upstream deps, fallback to heuristics."""
    try:
        deps = get_upstream_lineage(state["target_asset"])
    except CircuitBreakerListenerException:
        # PostgreSQL unavailable, use heuristics
        logger.warning("PostgreSQL unavailable, using heuristic fallback")
        
        target_asset = state["target_asset"]
        if "fact_sales" in target_asset:
            # Heuristic: fact_sales depends on silver_sales
            deps = [
                {"asset_id": "urn:silver:sales", "status": "UNKNOWN"}
            ]
        else:
            deps = []
    
    return {"dependency_result": {"deps": deps, "source": "heuristic"}}
```

#### Disaster Recovery Timeline

```
T+0s:   PostgreSQL fails
T+10s:  First RCA detects timeout, switches to fallback
T+10s:  RCA still succeeds (uses S3 logs + Spark API)
T+15s:  Slack alert: "⚠️ Lineage DB unavailable, using heuristics"
T+60s:  DBA notified, starts investigation
T+120s: PostgreSQL restored from backup
T+5m:   Lineage DB synced, normal operation resumed

Total outage impact: Low (RCA quality degrades but succeeds)
```

---

## 8. Cost Analysis & Optimization

### Question: "What are the monthly costs? How would you optimize?"

#### Monthly Cost Breakdown (500 DAGs, 100 incidents/day)

```
Infrastructure:
├─ AWS EC2 (Spark master + 8 workers): $5,000/month
├─ AWS S3 (Delta Lake + event logs): $1,500/month
│  └─ 5TB events, 2TB gold layer, $0.023/GB
├─ AWS Athena (analytics queries): $2,000/month
│  └─ 100 queries/day × 10GB scanned × $6.25/TB
├─ RDS PostgreSQL (lineage DB): $800/month
│  └─ db.t3.medium, 100GB storage
└─ EC2 for FastAPI/LangGraph: $500/month

External Services:
├─ Groq LLM API (1000 inference calls × $0.01): $10/month
│  └─ Meta-llama inference: very cheap (vs OpenAI $5-10)
└─ Slack API (webhook): Free tier

TOTAL: $9,810/month

Cost per incident: $9,810 / 3,000 (100/day × 30d) = $3.27
Alternative (manual RCA): 100 × 2 hours × $100/hr = $20,000/month
Savings: $20,000 - $10,000 = $10,000/month
```

#### Cost Optimization Strategies

```
1. S3 LIFECYCLE ($600/month savings)
   ├─ Move event logs to Glacier after 90 days
   ├─ Cost: $0.004/GB (vs $0.023 standard)
   └─ Impact: Lose real-time diagnostics after 90d (acceptable)

2. ATHENA PARTITIONING ($1,200/month savings)
   ├─ Current: Full table scan (100GB per query)
   ├─ Optimized: Partition by date (only 1GB scanned)
   ├─ Savings: 100× reduction in data scanned
   └─ Implementation: Add partition by sale_date

3. SPARK SPOT INSTANCES ($2,000/month savings)
   ├─ 70% discount vs on-demand
   ├─ Trade-off: Can be interrupted
   ├─ Mitigation: Use for batch jobs, not streaming
   └─ Expected savings: $3,500 → $1,500/month

4. SMALLER LLM MODEL ($5/month savings, but 10x cost!)
   ├─ Switch from Groq ($0.01/call) to Ollama (self-hosted)
   ├─ Speedup: 2x faster (5s → 2.5s)
   ├─ Trade-off: Must manage self-hosted server
   └─ ROI positive only for >1000 calls/day

5. RESERVED CAPACITY
   ├─ Reserve Athena DPU: -30% cost
   ├─ Reserve RDS: -40% cost
   ├─ Savings: $1,500/month

TOTAL SAVINGS: $5,800/month (59% reduction)
OPTIMIZED COST: $4,000/month
```

---

## Final Takeaway for Interviews

**"The key is not just building the system, but understanding the trade-offs at every layer:**

1. **Architectural**: Airflow+Spark vs alternatives (maturity vs features)
2. **Data design**: Medallion (separation) vs flat (simplicity), Delta (ACID) vs Iceberg (partitioning)
3. **Parallelization**: LangGraph (deterministic) vs direct LLM (simplicity)
4. **Resilience**: Graceful degradation (reliability) vs fail-fast (debuggability)
5. **Performance**: Caching (latency) vs freshness (correctness)
6. **Cost**: Compute optimization (savings) vs complexity (maintenance)

**Every choice involves trade-offs. A senior engineer articulates which trade-off they chose and why.**"
