"""Read Spark event logs from S3 (or local) and detect executor removal events.

Features:
- Support `s3://` and `s3a://` URIs via `boto3`.
- Fall back to local file reads if `boto3` is not available or a local path is provided.
- Parse JSON-lines and surface `SparkListenerExecutorRemoved` events with reason and executor id.
"""

from __future__ import annotations

import os
import json
import logging
from typing import Generator, Iterable, Dict, Any, List, Tuple, Optional

try:
    from config import cfg
except ImportError:
    import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
    from rca_agent_workflow.config import cfg

logger = logging.getLogger(__name__)

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
    _HAS_BOTO3 = True
except Exception:
    boto3 = None  # type: ignore
    BotoCoreError = Exception  # type: ignore
    ClientError = Exception  # type: ignore
    _HAS_BOTO3 = False


def parse_s3_path(path: str) -> Tuple[str, str]:
    """Parse s3 or s3a path into (bucket, key_prefix).

    Raises ValueError for invalid paths.
    """
    if path.startswith("s3a://"):
        p = path[len("s3a://"):]
    elif path.startswith("s3://"):
        p = path[len("s3://"):]
    else:
        raise ValueError("Not an s3 path: %s" % path)
    parts = p.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix


def list_s3_keys(bucket: str, prefix: str = "") -> List[str]:
    if not _HAS_BOTO3:
        raise RuntimeError("boto3 is required to list S3 keys")
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    keys: List[str] = []
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
    except (BotoCoreError, ClientError) as e:
        logger.error("Error listing S3 keys for %s/%s: %s", bucket, prefix, e)
        raise
    return keys


def read_s3_object_lines(bucket: str, key: str) -> Iterable[str]:
    if not _HAS_BOTO3:
        raise RuntimeError("boto3 is required to read S3 objects")
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"]
    for raw in body.iter_lines():
        try:
            yield raw.decode("utf-8")
        except Exception:
            # fallback if already str
            yield raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")


def read_local_file_lines(path: str) -> Iterable[str]:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            yield line.rstrip("\n")


def iter_event_lines(path: str) -> Iterable[str]:
    """Yield each JSON line from a local file or S3 path/prefix."""
    # Local path
    if not path.startswith("s3://") and not path.startswith("s3a://"):
        # support file or directory
        if os.path.isdir(path):
            for fname in sorted(os.listdir(path)):
                full = os.path.join(path, fname)
                if os.path.isfile(full):
                    for line in read_local_file_lines(full):
                        yield line
        else:
            for line in read_local_file_lines(path):
                yield line
        return

    # S3 path
    bucket, prefix = parse_s3_path(path)
    # If prefix is a single object, read it; otherwise list keys
    keys = list_s3_keys(bucket, prefix)
    # If exact match
    if prefix and prefix in keys:
        keys = [prefix]

    for key in keys:
        logger.debug("Reading s3://%s/%s", bucket, key)
        for line in read_s3_object_lines(bucket, key):
            yield line


def parse_json_lines(lines: Iterable[str]) -> Iterable[Dict[str, Any]]:
    for line in lines:
        if not line:
            continue
        try:
            obj = json.loads(line)
            yield obj
        except json.JSONDecodeError:
            # some lines may be empty or non-json; skip
            continue


def _get_field(event: Dict[str, Any], candidates: List[str]) -> Optional[Any]:
    for c in candidates:
        if c in event:
            return event[c]
    # try case-insensitive / normalized keys
    lowmap = {k.lower().replace(" ", ""): v for k, v in event.items()}
    for c in candidates:
        key = c.lower().replace(" ", "")
        if key in lowmap:
            return lowmap[key]
    return None


def find_executor_removed_events(events: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for ev in events:
        # Common format uses key 'Event' == 'SparkListenerExecutorRemoved'
        ev_type = _get_field(ev, ["Event", "event", "EventType"]) or ""
        if isinstance(ev_type, str) and "executorremoved" in ev_type.lower():
            exe_id = _get_field(ev, ["Executor ID", "executorId", "executor_id"]) or _get_field(ev, ["ExecutorID"]) or None
            reason = _get_field(ev, ["Removed Reason", "removedReason", "RemovedReason", "Reason"]) or _get_field(ev, ["message", "details"]) or None
            results.append({"executor_id": exe_id, "reason": reason, "raw": ev})
        else:
            # Some event payloads nest the event name under a key
            # Check if any value indicates executor removal
            try:
                # flatten string values to search
                s = json.dumps(ev).lower()
                if "executorremoved" in s or "executor removed" in s or "sparklistenerexecutorremoved" in s:
                    exe_id = _get_field(ev, ["Executor ID", "executorId", "executor_id"]) or None
                    reason = _get_field(ev, ["Removed Reason", "removedReason"]) or None
                    results.append({"executor_id": exe_id, "reason": reason, "raw": ev})
            except Exception:
                continue
    return results


if __name__ == "__main__":
    import sys

    path = cfg.SPARK_EVENT_LOGS_PATH if cfg.SPARK_EVENT_LOGS_PATH else (sys.argv[1] if len(sys.argv) > 1 else None)
    if not path:
        print("Usage: provide SPARK_EVENT_LOGS_PATH env var or path argument (s3://bucket/prefix or local file)")
        raise SystemExit(2)

    lines = iter_event_lines(path)
    events = parse_json_lines(lines)
    removed = find_executor_removed_events(events)
    print(f"Found {len(removed)} executor-removed events")
    for r in removed:
        print("executor_id:", r.get("executor_id"), "reason:", r.get("reason"))
