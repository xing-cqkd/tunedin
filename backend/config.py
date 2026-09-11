"""Centralized application configuration (XIN-42).

Single source of truth for environment-variable-driven settings. Every module
should read config through :func:`get_settings` instead of calling
``os.getenv`` directly. Environment variable names are kept identical to the
historical ad-hoc read sites so existing deployments do not break.

Note: ``backend/ingestion/task_queue/gcp.py`` and
``backend/ingestion/task_queue/__init__.py`` still read their env vars
directly; those read sites are owned by the task-queue batch and should
migrate to this module as a follow-up.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

_BACKEND_DIR = Path(__file__).resolve().parent
_DEFAULT_DATA_DIR = _BACKEND_DIR / ".data"
_DEFAULT_SIMPLE_DB_PATH = _BACKEND_DIR / "ingestion" / "simple.db"


def _str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _optional_str(name: str) -> Optional[str]:
    return os.environ.get(name)


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(
            f"Invalid value for {name}: {raw!r} (expected an integer)"
        ) from None


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        raise ValueError(
            f"Invalid value for {name}: {raw!r} (expected a number)"
        ) from None


def _data_dir() -> Path:
    return Path(_str("DATA_DIR", str(_DEFAULT_DATA_DIR)))


@dataclass(frozen=True)
class TuneInSettings:
    """All TuneIn configuration, sourced from the environment at load time."""

    # Databases (historical env names preserved)
    database_url: str = field(
        default_factory=lambda: _str("DATABASE_URL", "sqlite+aiosqlite:///./tunedin.db")
    )
    ingestion_database_url: str = field(
        default_factory=lambda: _str(
            "INGESTION_DATABASE_URL",
            f"sqlite+aiosqlite:///{_DEFAULT_SIMPLE_DB_PATH.resolve()}",
        )
    )

    # Task queue / GCP Cloud Tasks (historical env names preserved)
    task_queue_driver: str = field(default_factory=lambda: _str("TASK_QUEUE_DRIVER", "local"))
    gcp_project_id: str = field(default_factory=lambda: _str("GCP_PROJECT_ID", "tunedin-prod"))
    gcp_location: str = field(default_factory=lambda: _str("GCP_LOCATION", "us-central1"))
    gcp_queue_name: str = field(
        default_factory=lambda: _str("GCP_CLOUD_TASKS_QUEUE", "podcast-processing-queue")
    )
    gcp_service_account_email: Optional[str] = field(
        default_factory=lambda: _optional_str("GCP_SERVICE_ACCOUNT_EMAIL")
    )
    worker_webhook_url: str = field(
        default_factory=lambda: _str(
            "WORKER_WEBHOOK_URL", "http://localhost:8000/api/worker/process-episode"
        )
    )

    # Logging (XIN-63)
    log_level: str = field(default_factory=lambda: _str("LOG_LEVEL", "INFO").upper())

    # Data dir + progress tracking (XIN-63)
    data_dir: Path = field(default_factory=_data_dir)
    progress_file: Path = field(
        default_factory=lambda: Path(
            os.environ.get("PODCAST_PROGRESS_FILE", str(_data_dir() / "podcast_ingest.md"))
        )
    )

    # Batch-runner defaults (XIN-63)
    batch_size: int = field(default_factory=lambda: _int("BATCH_SIZE", 25))
    batch_delay_seconds: float = field(default_factory=lambda: _float("BATCH_DELAY_SECONDS", 0.35))
    ingestion_concurrency: int = field(default_factory=lambda: _int("INGESTION_CONCURRENCY", 5))


_SETTINGS: Optional[TuneInSettings] = None


def get_settings() -> TuneInSettings:
    """Return the cached process-wide settings instance."""
    global _SETTINGS
    if _SETTINGS is None:
        _SETTINGS = TuneInSettings()
    return _SETTINGS


def reload_settings() -> TuneInSettings:
    """Rebuild settings from the current environment (tests, mainly)."""
    global _SETTINGS
    _SETTINGS = TuneInSettings()
    return _SETTINGS


def configure_logging(level: Optional[str] = None) -> None:
    """Configure root logging. Call from entry points only, never at import.

    Importing a module must not touch the root logger (XIN-63); CLI
    ``main()`` functions call this once at startup.
    """
    resolved = (level or get_settings().log_level).upper()
    logging.basicConfig(
        level=getattr(logging, resolved, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
