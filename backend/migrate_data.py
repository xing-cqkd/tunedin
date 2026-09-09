"""Copy all rows from one configured database backend to another.

Usage:
    python -m backend.migrate_data --source simple --target app [--dry-run]

Backends are resolved from the same environment variables the backend
modules read at import time (never through the settings singleton, since
source and target may differ)::

    simple -> INGESTION_DATABASE_URL (default: backend/ingestion/simple.db)
    app    -> DATABASE_URL           (default: ./tunedin.db)

Rows are copied table-by-table in foreign-key-safe order (parents before
children) and upserted by primary key via ``session.merge()``, so re-running
a migration is idempotent and never duplicates rows.

The ``Backend`` abstraction below is the seam for a future DynamoDB backend:
it only has to implement table read/write by table name
(``table_names`` / ``read_table`` / ``write_rows``); the migration
orchestration in ``migrate()`` stays untouched.
"""

from __future__ import annotations

import abc
import argparse
import asyncio
import importlib
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List

from sqlalchemy import Table, event, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from backend.persistence.models import Base

REPO_ROOT = Path(__file__).resolve().parent.parent

_VALID_BACKENDS = ("simple", "app")

# backend name -> (module to import, env var it reads at import time)
_BACKEND_MODULES = {
    "simple": ("backend.ingestion.simple_db", "INGESTION_DATABASE_URL"),
    "app": ("backend.persistence.database", "DATABASE_URL"),
}


def _default_url(name: str) -> str:
    """The default URL each backend module falls back to (mirrors the modules)."""
    if name == "simple":
        path = (REPO_ROOT / "backend" / "ingestion" / "simple.db").resolve()
        return f"sqlite+aiosqlite:///{path}"
    return "sqlite+aiosqlite:///./tunedin.db"


def resolve_url(name: str) -> str:
    """Resolve a backend's database URL exactly the way its module would."""
    if name not in _VALID_BACKENDS:
        raise ValueError(f"Unknown backend {name!r} (expected one of {_VALID_BACKENDS})")
    _, env_var = _BACKEND_MODULES[name]
    return os.environ.get(env_var) or _default_url(name)


class SameBackendError(RuntimeError):
    """Raised when source and target resolve to the same underlying store."""


class Backend(abc.ABC):
    """Storage-agnostic migration endpoint.

    A future DynamoDB backend implements this interface -- reading and
    writing rows as plain column dicts keyed by table name -- without
    touching the migration orchestration in :func:`migrate`.
    """

    name: str

    @property
    @abc.abstractmethod
    def identity(self) -> str:
        """Unique identity of the underlying store (used to refuse self-copy)."""

    @property
    @abc.abstractmethod
    def table_names(self) -> List[str]:
        """All table names in dependency (parents-first) order."""

    @abc.abstractmethod
    async def init(self) -> None:
        """Ensure the target store exists (create tables if needed)."""

    @abc.abstractmethod
    async def read_table(self, table_name: str) -> List[Dict[str, Any]]:
        """Return every row of a table as plain ``{column: value}`` dicts."""

    @abc.abstractmethod
    async def write_rows(self, table_name: str, rows: List[Dict[str, Any]]) -> int:
        """Idempotently write rows (upsert by primary key). Returns rows written."""

    @abc.abstractmethod
    async def close(self) -> None:
        """Release underlying resources (engines, connections)."""


# Table name -> mapped ORM class, for generic merge-based upserts.
_TABLE_TO_CLASS = {
    mapper.local_table.name: mapper.class_
    for mapper in Base.registry.mappers
    if isinstance(mapper.local_table, Table)
}


class SqlAlchemyBackend(Backend):
    """A :class:`Backend` over a SQLAlchemy async engine."""

    def __init__(
        self,
        name: str,
        url: str,
        session_factory: Callable[[], AsyncSession],
        init_db: Callable[[], Awaitable[None]],
        dispose: Callable[[], Awaitable[None]],
    ) -> None:
        self.name = name
        self._url = url
        self._session_factory = session_factory
        self._init_db = init_db
        self._dispose = dispose

    @classmethod
    def from_url(cls, name: str, url: str) -> "SqlAlchemyBackend":
        """Build a backend directly from a URL (no backend-module import).

        Used by tests and programmatic callers; the CLI prefers
        :func:`get_backend` so env-var handling matches the backend modules.
        """
        engine: AsyncEngine = create_async_engine(url, echo=False, future=True)
        if url.startswith("sqlite"):

            @event.listens_for(engine.sync_engine, "connect")
            def _set_sqlite_pragma(dbapi_connection, connection_record):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()

        factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )

        async def init_db() -> None:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

        return cls(
            name=name,
            url=url,
            session_factory=factory,
            init_db=init_db,
            dispose=engine.dispose,
        )

    @property
    def identity(self) -> str:
        return f"sqlalchemy:{self._url}"

    @property
    def table_names(self) -> List[str]:
        # sorted_tables yields parents before children (FK-safe order).
        return [t.name for t in Base.metadata.sorted_tables]

    async def init(self) -> None:
        await self._init_db()

    async def _table(self, table_name: str) -> Table:
        return Base.metadata.tables[table_name]

    async def read_table(self, table_name: str) -> List[Dict[str, Any]]:
        table = await self._table(table_name)
        async with self._session_factory() as session:
            result = await session.execute(select(table))
            return [dict(row._mapping) for row in result]

    async def write_rows(self, table_name: str, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        model_cls = _TABLE_TO_CLASS[table_name]
        async with self._session_factory() as session:
            # merge() upserts by primary key: re-runs never duplicate rows.
            for row in rows:
                await session.merge(model_cls(**row))
            await session.commit()
        return len(rows)

    async def close(self) -> None:
        await self._dispose()


def get_backend(name: str) -> SqlAlchemyBackend:
    """Resolve a named backend the way its module would.

    The module is imported lazily, *after* its env var is pinned to the
    resolved URL, so ``INGESTION_DATABASE_URL`` / ``DATABASE_URL`` overrides
    are honored exactly as if the module had been imported directly.

    Note: call this before the backend modules are imported elsewhere in the
    process -- they bind their engine URL at import time.
    """
    if name not in _VALID_BACKENDS:
        raise ValueError(f"Unknown backend {name!r} (expected one of {_VALID_BACKENDS})")
    module_name, env_var = _BACKEND_MODULES[name]
    url = resolve_url(name)
    os.environ[env_var] = url
    module = importlib.import_module(module_name)
    return SqlAlchemyBackend(
        name=name,
        url=url,
        session_factory=module.AsyncSessionLocal,
        init_db=module.init_db,
        dispose=module.engine.dispose,
    )


@dataclass
class TableReport:
    table: str
    source_rows: int
    copied_rows: int


async def migrate(
    source: Backend, target: Backend, dry_run: bool = False
) -> List[TableReport]:
    """Copy every table from source to target. Returns a per-table report."""
    if source.identity == target.identity:
        raise SameBackendError(
            f"source and target resolve to the same database "
            f"({source.name} -> {source.identity}); refusing to migrate a "
            f"database into itself."
        )
    if not dry_run:
        await target.init()
    report: List[TableReport] = []
    try:
        for table_name in source.table_names:
            rows = await source.read_table(table_name)
            copied = 0
            if not dry_run and table_name in target.table_names:
                copied = await target.write_rows(table_name, rows)
            report.append(
                TableReport(
                    table=table_name, source_rows=len(rows), copied_rows=copied
                )
            )
    finally:
        await source.close()
        await target.close()
    return report


def print_report(report: List[TableReport], dry_run: bool) -> None:
    mode = "DRY RUN -- nothing written" if dry_run else "MIGRATED"
    print(f"{mode}")
    print(f"{'table':<28}{'source rows':>12}{'copied':>10}")
    print("-" * 52)
    total_source = total_copied = 0
    for r in report:
        print(f"{r.table:<28}{r.source_rows:>12}{r.copied_rows:>10}")
        total_source += r.source_rows
        total_copied += r.copied_rows
    print("-" * 52)
    print(f"{'TOTAL':<28}{total_source:>12}{total_copied:>10}")


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Copy all data from one configured database backend to another."
    )
    parser.add_argument("--source", required=True, choices=_VALID_BACKENDS)
    parser.add_argument("--target", required=True, choices=_VALID_BACKENDS)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print per-table row counts without writing anything.",
    )
    args = parser.parse_args(argv)

    try:
        source = get_backend(args.source)
        target = get_backend(args.target)
        report = asyncio.run(migrate(source, target, dry_run=args.dry_run))
    except (ValueError, SameBackendError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    print_report(report, args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
