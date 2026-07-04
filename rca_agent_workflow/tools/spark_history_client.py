"""Spark History Server REST client and simple diagnostics helpers.

Provides methods to query application and stage data and basic rules to detect
data skew and memory/disk spill indicators.

Configuration via environment variables:
  SPARK_HISTORY_BASE (default: http://localhost:18080/api/v1)
"""

from __future__ import annotations

import os
import logging
from typing import Any, Dict, List, Optional

import statistics
import requests

try:
    from config import cfg
except ImportError:
    import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
    from rca_agent_workflow.config import cfg

logger = logging.getLogger(__name__)


class SparkHistoryClient:
    def __init__(self, base_url: Optional[str] = None, timeout: int = 10):
        self.base_url = base_url or cfg.SPARK_HISTORY_BASE
        self.timeout = timeout if timeout != 10 else cfg.SPARK_HISTORY_TIMEOUT
        self.session = requests.Session()

    def _url(self, path: str) -> str:
        return f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"

    def get_application(self, app_id: str) -> Dict[str, Any]:
        url = self._url(f"applications/{app_id}")
        logger.debug("GET %s", url)
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def list_applications(self) -> List[Dict[str, Any]]:
        url = self._url("applications")
        logger.debug("GET %s", url)
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def get_stages(self, app_id: str) -> List[Dict[str, Any]]:
        url = self._url(f"applications/{app_id}/stages")
        logger.debug("GET %s", url)
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def get_stage_details(self, app_id: str, stage_id: int) -> Dict[str, Any]:
        # The /stages/{stageId} endpoint returns a list of attempts for that stage.
        # Choose the latest attempt and try to fetch its task summary which contains per-task metrics.
        url = self._url(f"applications/{app_id}/stages/{stage_id}")
        logger.debug("GET %s", url)
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        attempts = resp.json()

        # attempts is usually a list; pick the last (most recent) attempt
        if isinstance(attempts, list) and attempts:
            latest = attempts[-1]
            # attempt may include 'attemptId' or 'attempt'
            attempt_id = latest.get("attemptId") or latest.get("attempt")
            # If we have an attempt id, try the taskSummary endpoint which contains task metrics
            if attempt_id is not None:
                task_summary_url = self._url(f"applications/{app_id}/stages/{stage_id}/{attempt_id}/taskSummary")
                try:
                    logger.debug("GET %s", task_summary_url)
                    r2 = self.session.get(task_summary_url, timeout=self.timeout)
                    r2.raise_for_status()
                    return r2.json()
                except Exception:
                    # Fall back to returning the whole attempt object
                    return latest

        # Fallback: return whatever the endpoint returned (legacy behavior)
        return attempts

    def detect_skew(self, tasks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze a list of task info dictionaries for skew.

        Expects tasks to include numeric `duration` (ms) or `executorRunTime` fields.

        Returns a dict with `is_skew`, `max`, `median`, `ratio`, and `reason`.
        Rule: if max > 5 * median => skew.
        """
        durations: List[float] = []
        for t in tasks:
            # try common fields
            v = None
            for k in ("duration", "executorRunTime", "taskDurationMillis", "getExecutorRunTime"):
                if k in t and isinstance(t[k], (int, float)):
                    v = float(t[k])
                    break
            if v is not None:
                durations.append(v)

        if not durations:
            return {"is_skew": False, "reason": "no duration data"}

        med = statistics.median(durations)
        mx = max(durations)
        ratio = mx / med if med > 0 else float("inf")
        is_skew = ratio > 5
        reason = "max > 5 * median" if is_skew else "no skew detected"
        return {"is_skew": is_skew, "max": mx, "median": med, "ratio": ratio, "reason": reason}

    def analyze_stage_for_spill_and_skew(self, app_id: str, stage_id: int) -> Dict[str, Any]:
        """Fetch stage details and analyze for skew and spills."""
        details = self.get_stage_details(app_id, stage_id)
        # The API returns a list of attempts/stages; try to locate taskMetrics
        tasks = []
        if isinstance(details, dict):
            # stages may include 'taskMetrics' under tasks in different shapes
            if "tasks" in details and isinstance(details["tasks"], list):
                for t in details["tasks"]:
                    tm = t.get("taskMetrics") or {}
                    task_info = {}
                    # collect executor runtime and spills
                    if "executorRunTime" in t:
                        task_info["executorRunTime"] = t["executorRunTime"]
                    if isinstance(tm, dict):
                        for k in ("diskBytesSpilled", "memoryBytesSpilled"):
                            if k in tm:
                                task_info[k] = tm[k]
                    tasks.append(task_info)

        skew = self.detect_skew(tasks)
        total_disk_spill = sum(t.get("diskBytesSpilled", 0) for t in tasks)
        total_mem_spill = sum(t.get("memoryBytesSpilled", 0) for t in tasks)

        return {
            "stage_id": stage_id,
            "skew": skew,
            "total_disk_spill": total_disk_spill,
            "total_memory_spill": total_mem_spill,
        }


def create_default_spark_client() -> SparkHistoryClient:
    return SparkHistoryClient()


if __name__ == "__main__":
    client = create_default_spark_client()
    try:
        apps = client.list_applications()
        if not apps:
            logger.error("No applications found")
            raise SystemExit(1)

        app_id = apps[0]["id"]
        logger.info("Using app_id: %s", app_id)

        stages = client.get_stages(app_id)
        completed = [s for s in stages if s.get("status") == "COMPLETE"]
        logger.info("Completed stages: %s", [s["stageId"] for s in completed])

        for stage in completed:
            stage_id = stage["stageId"]

            # detect_skew: build task list from aggregated stage metrics
            tasks = [{"executorRunTime": stage.get("executorRunTime", 0)}]
            skew = client.detect_skew(tasks)
            logger.info("Stage %s detect_skew: %s", stage_id, skew)

            # analyze_stage_for_spill_and_skew
            analysis = client.analyze_stage_for_spill_and_skew(app_id, stage_id)
            logger.info("Stage %s analyze_stage_for_spill_and_skew: %s", stage_id, analysis)

    except Exception as e:
        logger.error("Error: %s", e)
