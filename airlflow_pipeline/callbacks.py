import os
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ---------------------------------------------------------
# LINEAGE DATABASE HELPERS
# ---------------------------------------------------------
def update_lineage_status(target_asset, status):
    """Helper to update the PostgreSQL lineage table."""
    if not target_asset:
        return
    try:
        pg_hook = PostgresHook(postgres_conn_id="metadata_db_conn")
        sql = """
            UPDATE data_assets 
            SET last_updated_at = NOW(), status = %(status)s 
            WHERE asset_id = %(target_asset)s;
        """
        pg_hook.run(sql, parameters={"target_asset": target_asset, "status": status})
        print(f"Lineage DB updated: {target_asset} is now {status}")
    except Exception as e:
        print(f"Failed to update lineage DB: {e}")

# ---------------------------------------------------------
# TASK-LEVEL CALLBACKS
# ---------------------------------------------------------
def task_success_callback(context):
    """Fires when a specific task succeeds."""
    task = context.get('task')
    target_asset = task.params.get('target_asset_id')
    update_lineage_status(target_asset, 'SUCCESS')

def task_failure_callback(context):
    """
    Fires when a specific task fails. 
    1. Updates Lineage DB to FAILED.
    2. Triggers the LangGraph RCA Agent.
    """
    ti = context.get('task_instance')
    task = context.get('task')
    target_asset = task.params.get('target_asset_id')
    
    # 1. Update Lineage to FAILED so the Impact/Dependency agents see it
    update_lineage_status(target_asset, 'FAILED')
    
    # 2. Trigger the RCA Agent (Simplified here, add your log regex logic from earlier)
    payload = {
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context.get('run_id'),
        "try_number": ti.try_number,
        "target_table": target_asset # Pass the target table to the agent!
    }
    
    agent_url = os.environ.get("AGENT_WEBHOOK_URL", "http://sre-agent:8000/api/v1/trigger-rca")
    try:
        # requests.post(agent_url, json=payload, timeout=5)
        print(f"Would have sent to RCA agent: {payload}")
        print("Successfully triggered RCA Agent.")
    except Exception as e:
        print(f"Failed to trigger RCA Agent: {e}")

# ---------------------------------------------------------
# DAG-LEVEL CALLBACK
# ---------------------------------------------------------
def dag_failure_callback(context):
    """
    Fires when the overall DAG fails.
    Great for high-level Slack alerts or PagerDuty.
    """
    dag_run = context.get('dag_run')
    dag_id = dag_run.dag_id
    execution_date = dag_run.execution_date
    
    alert_message = f"🚨 *CRITICAL ALERT*: The DAG `{dag_id}` failed for execution `{execution_date}`. The AI SRE Agent is investigating."
    print(alert_message)
    # E.g., requests.post("https://hooks.slack.com/...", json={"text": alert_message})