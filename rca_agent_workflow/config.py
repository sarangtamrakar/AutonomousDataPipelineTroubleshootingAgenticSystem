"""Central configuration — single source of truth for all env vars.

Usage (anywhere in the project):
    from src.config import cfg

    client = AirflowClient(base_url=cfg.AIRFLOW_BASE, ...)
"""

from __future__ import annotations

import logging
import os
from typing import Optional

# ── Load .env first (before any getenv calls) ─────────────────────────────────

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"), override=True)
except ImportError:
    # Manual fallback parser
    _env_path = os.path.join(os.path.dirname(__file__), "../.env")
    if os.path.exists(_env_path):
        with open(_env_path) as _f:
            for _line in _f:
                _line = _line.strip()
                if _line and not _line.startswith("#") and "=" in _line:
                    _k, _v = _line.split("=", 1)
                    _k = _k.strip()
                    _v = _v.strip().strip('"').strip("'")
                    if _k:
                        os.environ[_k] = _v


# ── Settings ──────────────────────────────────────────────────────────────────

class _Settings:
    # Airflow
    AIRFLOW_BASE:     str = os.getenv("AIRFLOW_BASE",     "http://localhost:8080/api/v1")
    AIRFLOW_USER:     str = os.getenv("AIRFLOW_USER",     "admin")
    AIRFLOW_PASSWORD: str = os.getenv("AIRFLOW_PASSWORD", "admin")
    AIRFLOW_TOKEN: Optional[str] = os.getenv("AIRFLOW_TOKEN")

    # Spark History
    SPARK_HISTORY_BASE:    str = os.getenv("SPARK_HISTORY_BASE",    "http://localhost:18080/api/v1")
    SPARK_HISTORY_TIMEOUT: int = int(os.getenv("SPARK_HISTORY_TIMEOUT", "10"))

    # S3 / Event logs
    AWS_ACCESS_KEY_ID:     Optional[str] = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY: Optional[str] = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_REGION:            str = os.getenv("AWS_REGION", "us-east-1")
    SPARK_EVENT_LOGS_PATH: str = os.getenv("SPARK_EVENT_LOGS_PATH", "s3://sarang-de/spark-event-logs")

    # Local worker logs
    SPARK_WORKER_LOG_DIR: str = os.getenv("SPARK_WORKER_LOG_DIR", "./spark-worker-logs")

    # athena db & output s3 bucket 
    ATHENA_DB: str = os.getenv("ATHENA_DB","gluedb")
    ATHENA_OUTPUT:str = os.getenv("ATHENA_OUTPUT",None)
    

    # LLM provider configuration
    LLM_PROVIDER: str = os.getenv("LLM_PROVIDER", "groq")

    GROQ_API_KEY: Optional[str] = os.getenv("GROQ_API_KEY")
    GROQ_MODEL:   str = os.getenv("GROQ_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct")

    OLLAMA_URL: Optional[str] = os.getenv("OLLAMA_URL", "http://localhost:11434")
    OLLAMA_API_KEY: Optional[str] = os.getenv("OLLAMA_API_KEY")
    OLLAMA_MODEL: str = os.getenv("OLLAMA_MODEL", "gpt-4o-mini")

    # Logging
    LOG_LEVEL:  str = os.getenv("LOG_LEVEL",  "INFO")
    LOG_FORMAT: str = os.getenv("LOG_FORMAT", "%(asctime)s %(levelname)s %(name)s: %(message)s")
    LOG_TO_FILE: Optional[str] = os.getenv("LOG_TO_FILE")


cfg = _Settings()


# ── Logging setup ─────────────────────────────────────────────────────────────

def configure_logging() -> None:
    level = getattr(logging, cfg.LOG_LEVEL.upper(), logging.INFO)
    handlers = (
        [logging.FileHandler(cfg.LOG_TO_FILE, encoding="utf-8")]
        if cfg.LOG_TO_FILE else
        [logging.StreamHandler()]
    )
    logging.basicConfig(level=level, format=cfg.LOG_FORMAT, handlers=handlers)


# Auto-configure on import
configure_logging()
