"""Simple Airflow REST API client for the RCA agent."""

from __future__ import annotations

import os
import logging
from typing import Any, Dict, Optional
import requests

try:
    from config import cfg
except ImportError:
    import sys; sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
    from rca_agent_workflow.config import cfg

logger = logging.getLogger(__name__)

class AirflowClient:
    def __init__(self, base_url: Optional[str] = None, token: Optional[str] = None, auth: Optional[tuple] = None, timeout: int = 10):
        self.base_url = base_url or cfg.AIRFLOW_BASE
        self.timeout = timeout
        self.session = requests.Session()
        if token:
            self.session.headers.update({"Authorization": f"Bearer {token}"})
        elif auth:
            self.session.auth = auth

    def _url(self, path: str) -> str:
        return f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"

    def list_dag_runs(self, dag_id: str, limit: int = 100) -> Dict[str, Any]:
        url = self._url(f"dags/{dag_id}/dagRuns")
        resp = self.session.get(url, params={"limit": limit}, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def get_task_instance(self, dag_id: str, dag_run_id: str, task_id: str) -> Dict[str, Any]:
        """Fetch a single task instance metadata object."""
        url = self._url(f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}")
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def get_task_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int = 1) -> str:
        url = self._url(f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}")
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        try:
            data = resp.json()
            if isinstance(data, dict):
                for key in ("content", "data", "message", "log"):
                    if key in data: return data[key]
            return str(data)
        except ValueError:
            return resp.text

    def get_task_asset_urn(self, dag_id: str, run_id: str, task_id: str) -> str:
        """Fetches the target_asset_id from the Airflow task instance params."""
        try:
            # Use 'self' to call the existing instance method
            ti = self.get_task_instance(dag_id, run_id, task_id)
            params = ti.get("params", {})
            return params.get("target_asset_id", f"urn:unknown:{task_id}")
        except Exception as e:
            logger.error(f"Error fetching URN for {task_id}: {e}")
            return f"urn:unknown:{task_id}"

def create_default_client() -> AirflowClient:
    auth = (cfg.AIRFLOW_USER, cfg.AIRFLOW_PASSWORD) if cfg.AIRFLOW_USER else None
    return AirflowClient(token=cfg.AIRFLOW_TOKEN, auth=auth)

if __name__ == "__main__":
    client = create_default_client()
    urn = client.get_task_asset_urn("example_dag", "run_123", "task_abc")
    print(f"Found URN: {urn}")