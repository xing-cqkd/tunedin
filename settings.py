"""Central settings for the TuneIn app.

Reads `settings.yaml` from the repo root, applies environment-variable
overrides, and exposes the configured database backend's session helpers.

Resolution order (highest precedence first):
  1. Environment variables:
       DATABASE_BACKEND      -> database.backend ("simple" | "app")
       DATABASE_SIMPLE_PATH  -> database.simple.path
       DATABASE_APP_URL      -> database.app.url
     (The legacy INGESTION_DATABASE_URL / DATABASE_URL variables keep working:
     the database modules read them directly at import time and they take
     precedence over the values mirrored from settings.yaml.)
  2. `settings.yaml` at the repo root.
  3. Built-in defaults (SimpleDB at backend/ingestion/simple.db).

The database modules (`backend.ingestion.simple_db` and
`backend.persistence.database`) read their configuration from the environment
at import time, so this module mirrors the resolved settings into the
environment *before* those modules are imported. Always go through
`settings` (`session_scope`, `init_db`, `get_db`) instead of
importing the backend modules directly.
"""

import copy
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List
from urllib.parse import urlsplit, urlunsplit

from sqlalchemy.ext.asyncio import AsyncSession

try:
    import yaml
except ImportError:  # pragma: no cover - PyYAML is a required dependency
    yaml = None  # type: ignore[assignment]

REPO_ROOT = Path(__file__).resolve().parent
SETTINGS_PATH = REPO_ROOT / "settings.yaml"

DEFAULTS: Dict[str, Any] = {
    "database": {
        "backend": "simple",
        "simple": {"path": "backend/ingestion/simple.db"},
        "app": {"url": "sqlite+aiosqlite:///./tunedin.db"},
    },
    "ingestion": {
        "crawler_countries": ["us", "gb", "ca", "au", "de", "fr"],
        "auto_queue_episodes": 3,
    },
}

# Environment variable -> dotted path into the settings dict.
_ENV_OVERRIDES = {
    "DATABASE_BACKEND": "database.backend",
    "DATABASE_SIMPLE_PATH": "database.simple.path",
    "DATABASE_APP_URL": "database.app.url",
}

_VALID_BACKENDS = ("simple", "app")


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if value is None and isinstance(merged.get(key), dict):
            # Empty section (e.g. `database:` with nothing under it):
            # treat as {} so the defaults for that section survive.
            continue
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _set_dotted(settings: Dict[str, Any], dotted: str, value: Any) -> None:
    node = settings
    *parts, leaf = dotted.split(".")
    for part in parts:
        node = node.setdefault(part, {})
    node[leaf] = value


_yaml_cache: Dict[str, Any] | None = None


def _load_yaml() -> Dict[str, Any]:
    """Load settings.yaml once; return {} when the file is missing."""
    global _yaml_cache
    if _yaml_cache is None:
        if yaml is None:
            raise RuntimeError("PyYAML is required: pip install pyyaml")
        if SETTINGS_PATH.exists():
            with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
                _yaml_cache = yaml.safe_load(f) or {}
        else:
            _yaml_cache = {}
    return _yaml_cache


def get_settings() -> Dict[str, Any]:
    """Return the fully resolved settings (defaults < settings.yaml < env)."""
    settings = _deep_merge(DEFAULTS, _load_yaml())
    for env_var, dotted in _ENV_OVERRIDES.items():
        if env_var in os.environ:
            _set_dotted(settings, dotted, os.environ[env_var])
    return settings


def get_database_backend() -> str:
    """Return the configured database backend: "simple" or "app"."""
    backend = str(get_settings()["database"]["backend"]).lower()
    if backend not in _VALID_BACKENDS:
        source = (
            "the DATABASE_BACKEND environment variable"
            if "DATABASE_BACKEND" in os.environ
            else "settings.yaml"
        )
        raise ValueError(
            f"Unknown database backend {backend!r} in {source} "
            f"(expected one of {_VALID_BACKENDS})"
        )
    return backend


def get_crawler_countries() -> List[str]:
    """Configured iTunes storefront countries (ingestion.crawler_countries)."""
    countries = get_settings()["ingestion"]["crawler_countries"]
    if (
        not isinstance(countries, list)
        or not countries
        or not all(isinstance(c, str) and c.strip() for c in countries)
    ):
        raise ValueError(
            "Invalid ingestion.crawler_countries in settings.yaml: "
            f"expected a non-empty list of country codes, got {countries!r}"
        )
    return [c.strip() for c in countries]


def get_auto_queue_episodes() -> int:
    """Validated ingestion.auto_queue_episodes (a non-negative integer)."""
    raw = get_settings()["ingestion"]["auto_queue_episodes"]
    value: int | None = None
    if isinstance(raw, int) and not isinstance(raw, bool):
        value = raw
    elif isinstance(raw, str):
        try:
            value = int(raw.strip())
        except ValueError:
            value = None
    if value is None or value < 0:
        raise ValueError(
            "Invalid ingestion.auto_queue_episodes in settings.yaml: "
            f"expected a non-negative integer, got {raw!r}"
        )
    return value


def _path_to_sqlite_url(path: str) -> str:
    """Turn a filesystem path (or an already-formed URL) into a SQLAlchemy URL."""
    if "://" in path:
        return path
    absolute = (REPO_ROOT / path).resolve()
    return f"sqlite+aiosqlite:///{absolute}"


def _apply_to_env() -> None:
    """Mirror the resolved database settings into the environment.

    The backend modules read INGESTION_DATABASE_URL / DATABASE_URL at import
    time, so this runs at import of settings — before those modules
    are imported anywhere. Explicitly-set environment variables win
    (setdefault), preserving the legacy override behavior.
    """
    settings = get_settings()
    backend = str(settings["database"]["backend"]).lower()
    if backend == "simple":
        simple_path = str(settings["database"]["simple"]["path"])
        os.environ.setdefault("INGESTION_DATABASE_URL", _path_to_sqlite_url(simple_path))
    elif backend == "app":
        os.environ.setdefault("DATABASE_URL", str(settings["database"]["app"]["url"]))


def describe_database() -> str:
    """Human-readable label for the configured database, for CLI/status output."""
    backend = get_database_backend()
    if backend == "simple":
        path = str(get_settings()["database"]["simple"]["path"])
        mirrored = _path_to_sqlite_url(path)
        # Honor an explicit legacy env override: it is what simple_db actually
        # uses (_apply_to_env only mirrors the yaml value when the var is unset,
        # in which case the env value equals `mirrored`).
        env_url = os.environ.get("INGESTION_DATABASE_URL")
        if env_url and env_url != mirrored:
            return f"SimpleDB (SQLite): {env_url}"
        if "://" in path:
            # Already a URL: display it as-is instead of resolving it as a
            # filesystem path.
            return f"SimpleDB (SQLite): {path}"
        return f"SimpleDB (SQLite): {(REPO_ROOT / path).resolve()}"
    url = os.environ.get("DATABASE_URL", str(get_settings()["database"]["app"]["url"]))
    # Mask any password embedded in the URL.
    parts = urlsplit(url)
    if parts.password:
        netloc = parts.hostname or ""
        if parts.username:
            netloc = f"{parts.username}:***@{netloc}"
        if parts.port:
            netloc = f"{netloc}:{parts.port}"
        url = urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    return f"App database: {url}"


@asynccontextmanager
async def session_scope() -> AsyncGenerator[AsyncSession, None]:
    """Yield an async session from the configured database backend."""
    backend = get_database_backend()
    if backend == "simple":
        from backend.ingestion import simple_db

        async with simple_db.session_scope() as session:
            yield session
    else:
        from backend.persistence import database

        async with database.session_scope() as session:
            yield session


async def init_db() -> None:
    """Create all tables in the configured database backend."""
    backend = get_database_backend()
    if backend == "simple":
        from backend.ingestion import simple_db

        await simple_db.init_db()
    else:
        from backend.persistence import database

        await database.init_db()


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Session dependency for the configured backend (e.g. FastAPI Depends)."""
    async with session_scope() as session:
        yield session


# Mirror settings into the environment before any backend module is imported.
_apply_to_env()
