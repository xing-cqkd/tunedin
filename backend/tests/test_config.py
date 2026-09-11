"""XIN-42: coverage for backend.config — centralized settings.

Verifies defaults, environment overrides (env-var names unchanged from the
historical ad-hoc read sites), immutability, and the logging helper.
"""

import dataclasses
import logging
from pathlib import Path

import pytest

from backend.config import (
    TuneInSettings,
    configure_logging,
    get_settings,
    reload_settings,
)

_CONFIG_ENV_VARS = [
    "DATABASE_URL",
    "INGESTION_DATABASE_URL",
    "TASK_QUEUE_DRIVER",
    "GCP_PROJECT_ID",
    "GCP_LOCATION",
    "GCP_CLOUD_TASKS_QUEUE",
    "GCP_SERVICE_ACCOUNT_EMAIL",
    "WORKER_WEBHOOK_URL",
    "LOG_LEVEL",
    "DATA_DIR",
    "PODCAST_PROGRESS_FILE",
    "BATCH_SIZE",
    "BATCH_DELAY_SECONDS",
    "INGESTION_CONCURRENCY",
]


@pytest.fixture(autouse=True)
def _clean_config_env(monkeypatch):
    """Isolate config env vars per test and drop the cached settings."""
    for var in _CONFIG_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    reload_settings()
    yield
    # monkeypatch restores env after this finalizer; rebuild defaults anyway
    reload_settings()


def _backend_dir() -> Path:
    return Path(__file__).resolve().parent.parent


def test_defaults():
    s = get_settings()
    assert s.database_url == "sqlite+aiosqlite:///./tunedin.db"
    assert s.ingestion_database_url == (
        f"sqlite+aiosqlite:///{(_backend_dir() / 'ingestion' / 'simple.db').resolve()}"
    )
    assert s.task_queue_driver == "local"
    assert s.gcp_project_id == "tunedin-prod"
    assert s.gcp_location == "us-central1"
    assert s.gcp_queue_name == "podcast-processing-queue"
    assert s.gcp_service_account_email is None
    assert s.worker_webhook_url == "http://localhost:8000/api/worker/process-episode"
    assert s.log_level == "INFO"
    assert s.data_dir == _backend_dir() / ".data"
    assert s.progress_file == _backend_dir() / ".data" / "podcast_ingest.md"
    assert s.batch_size == 25
    assert s.batch_delay_seconds == 0.35
    assert s.ingestion_concurrency == 5


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://db/tunedin")
    monkeypatch.setenv("INGESTION_DATABASE_URL", "sqlite+aiosqlite:////tmp/x.db")
    monkeypatch.setenv("TASK_QUEUE_DRIVER", "gcp")
    monkeypatch.setenv("GCP_PROJECT_ID", "my-proj")
    monkeypatch.setenv("GCP_LOCATION", "us-east1")
    monkeypatch.setenv("GCP_CLOUD_TASKS_QUEUE", "my-queue")
    monkeypatch.setenv("GCP_SERVICE_ACCOUNT_EMAIL", "svc@my-proj.iam.gserviceaccount.com")
    monkeypatch.setenv("WORKER_WEBHOOK_URL", "https://worker.example.com/hook")
    monkeypatch.setenv("LOG_LEVEL", "debug")
    monkeypatch.setenv("PODCAST_PROGRESS_FILE", "/tmp/progress/custom.md")
    monkeypatch.setenv("BATCH_SIZE", "50")
    monkeypatch.setenv("BATCH_DELAY_SECONDS", "1.5")
    monkeypatch.setenv("INGESTION_CONCURRENCY", "8")

    s = reload_settings()
    assert s.database_url == "postgresql+asyncpg://db/tunedin"
    assert s.ingestion_database_url == "sqlite+aiosqlite:////tmp/x.db"
    assert s.task_queue_driver == "gcp"
    assert s.gcp_project_id == "my-proj"
    assert s.gcp_location == "us-east1"
    assert s.gcp_queue_name == "my-queue"
    assert s.gcp_service_account_email == "svc@my-proj.iam.gserviceaccount.com"
    assert s.worker_webhook_url == "https://worker.example.com/hook"
    assert s.log_level == "DEBUG"
    assert s.progress_file == Path("/tmp/progress/custom.md")
    assert s.batch_size == 50
    assert s.batch_delay_seconds == 1.5
    assert s.ingestion_concurrency == 8


def test_data_dir_drives_default_progress_file(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "mydata"))
    s = reload_settings()
    assert s.data_dir == tmp_path / "mydata"
    assert s.progress_file == tmp_path / "mydata" / "podcast_ingest.md"


def test_explicit_progress_file_wins_over_data_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "mydata"))
    monkeypatch.setenv("PODCAST_PROGRESS_FILE", str(tmp_path / "elsewhere.md"))
    s = reload_settings()
    assert s.progress_file == tmp_path / "elsewhere.md"


def test_settings_are_frozen():
    s = get_settings()
    with pytest.raises(dataclasses.FrozenInstanceError):
        s.batch_size = 99  # type: ignore[misc]


def test_get_settings_returns_singleton():
    assert get_settings() is get_settings()
    assert reload_settings() is get_settings()


def test_database_modules_read_from_config():
    """XIN-42: the two DB modules source their URLs from the settings object."""
    import importlib

    from backend.ingestion import simple_db
    from backend.persistence import database

    # Rebind from the (clean) environment: earlier tests may have reloaded
    # these modules under a monkeypatched DATABASE_URL.
    importlib.reload(database)
    importlib.reload(simple_db)

    assert database.DATABASE_URL == get_settings().database_url
    assert simple_db.DATABASE_URL == get_settings().ingestion_database_url
    # Env-var names preserved for existing deployments
    assert database.DATABASE_URL == "sqlite+aiosqlite:///./tunedin.db"
    assert simple_db.INGESTION_DB_PATH.name == "simple.db"


def test_configure_logging_respects_level():
    for h in list(logging.root.handlers):
        logging.root.removeHandler(h)
    try:
        configure_logging("WARNING")
        assert logging.root.handlers, "expected a root handler to be installed"
        assert logging.root.level == logging.WARNING
    finally:
        for h in list(logging.root.handlers):
            logging.root.removeHandler(h)
        logging.root.setLevel(logging.WARNING)
        configure_logging()  # restore a sane default for subsequent tests
