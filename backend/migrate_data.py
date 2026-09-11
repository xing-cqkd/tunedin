"""Copy all rows from one configured database backend to another.

Usage:
    python -m backend.migrate_data --source simple --target dynamodb [--dry-run]

Backends are resolved from the same environment variables the backend
modules read at import time (never through the settings singleton, since
source and target may differ)::

    simple   -> INGESTION_DATABASE_URL (default: backend/ingestion/simple.db)
    app      -> DATABASE_URL           (default: ./tunedin.db)
    dynamodb -> DATABASE_DYNAMODB_TABLE_NAME (default: tunedin),
                DATABASE_DYNAMODB_REGION (default: us-east-1),
                DATABASE_DYNAMODB_ENDPOINT_URL (unset: real AWS)

Rows are copied table-by-table in foreign-key-safe order (parents before
children) and upserted by primary key via ``session.merge()``, so re-running
a migration is idempotent and never duplicates rows.

The ``Backend`` abstraction below is the seam every backend implements
(``table_names`` / ``read_table`` / ``write_rows``); the DynamoDB side
lives in ``backend.persistence.dynamodb.migrate_adapter`` and the
migration orchestration in ``migrate()`` stays untouched.
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
from typing import Any, Awaitable, Callable

from sqlalchemy import Table, func, make_url, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from backend.persistence._sqlite import configure_sqlite_fk
from backend.persistence.models import Base

REPO_ROOT = Path(__file__).resolve().parent.parent

_VALID_BACKENDS = ("simple", "app", "dynamodb")

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
    """Resolve a backend's database URL exactly the way its module would.

    Raises:
        ValueError: for an unknown backend name -- and for ``"dynamodb"``,
            which has no URL (use :func:`get_backend` for that backend).
    """
    if name not in _VALID_BACKENDS:
        raise ValueError(f"Unknown backend {name!r} (expected one of {_VALID_BACKENDS})")
    if name == "dynamodb":
        raise ValueError(
            'dynamodb has no database URL; use get_backend("dynamodb") instead'
        )
    _, env_var = _BACKEND_MODULES[name]
    return os.environ.get(env_var) or _default_url(name)


def _normalize_url(url: str) -> str:
    """Normalize a database URL for stable identity comparison.

    SQLite file paths are resolved to absolute form (relative to the
    current working directory, exactly as the sqlite driver interprets
    them) so equivalent spellings -- ``./tunedin.db`` vs
    ``/abs/cwd/tunedin.db`` -- produce the same identity and the
    same-database guard cannot be slipped with a different spelling.
    Non-sqlite URLs are returned unchanged.
    """
    scheme = url.split("://", 1)[0]
    if "sqlite" not in scheme:
        return url
    try:
        database = make_url(url).database
    except Exception:
        return url
    if not database or database == ":memory:":
        return url
    return f"{scheme}:///{Path(database).resolve()}"


# Rows written per session.merge() batch before the session is flushed and
# its identity map evicted (XIN-132: bounds write-side memory).
_WRITE_FLUSH_EVERY = 1000


class SameBackendError(RuntimeError):
    """Raised when source and target resolve to the same underlying store."""


def assert_distinct_backends(source: "Backend", target: "Backend") -> None:
    """Refuse to run a source/target operation against one store.

    Raises:
        SameBackendError: when both backends resolve to the same underlying
            store. Shared by :func:`migrate` and ``parity_check._run`` so the
            refusal (and its message) stays consistent.
    """
    if source.identity == target.identity:
        raise SameBackendError(
            f"source and target resolve to the same database "
            f"({source.name} -> {source.identity}); refusing."
        )


def reconcile_tables(
    source_tables: list[str], target_tables: list[str]
) -> tuple[list[str], list[str], list[str]]:
    """Partition table names into ``(common, source_only, target_only)``.

    ``common`` and ``source_only`` keep source order (which is FK-safe
    parents-first order for SQLAlchemy backends); ``target_only`` is sorted.
    Shared by :func:`migrate` (skip source-only tables) and
    ``parity_check.compare`` (missing-table / target-only-table detection).
    """
    target_set = set(target_tables)
    source_set = set(source_tables)
    common = [t for t in source_tables if t in target_set]
    source_only = [t for t in source_tables if t not in target_set]
    target_only = sorted(set(target_tables) - source_set)
    return common, source_only, target_only


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
    def table_names(self) -> list[str]:
        """All table names in dependency (parents-first) order.

        Source and target backends are expected to cover the same table
        set; any source table missing from the target is skipped with a
        warning (see :func:`migrate`).
        """

    @abc.abstractmethod
    async def init(self) -> None:
        """Ensure the target store exists (create tables if needed)."""

    @abc.abstractmethod
    async def read_table(self, table_name: str) -> list[dict[str, Any]]:
        """Return every row of a table as plain ``{column: value}`` dicts.

        Value-type contract: values must be SQLAlchemy-hydrated native
        Python values keyed by column name -- UUID objects (not strings),
        tz-aware datetimes (not ISO-8601 strings), JSON columns as
        dicts/lists -- because :meth:`write_rows` feeds each dict straight
        into ``model_cls(**row)``. A backend that stores values in another
        representation must convert them back to these native types here.
        """

    async def count_rows(self, table_name: str) -> int:
        """Return the row count of a table, without materializing rows.

        The default implementation counts what :meth:`read_table` returns;
        backends override this with a count-only read (``SELECT count(*)``,
        a ``Select="COUNT"`` scan) so callers that only need the count --
        skip warnings, target-only-table detection -- never load the table.
        """
        return len(await self.read_table(table_name))

    @abc.abstractmethod
    async def write_rows(self, table_name: str, rows: list[dict[str, Any]]) -> int:
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
            configure_sqlite_fk(engine)

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
        # Normalized so equivalent URL spellings (./x.db vs /abs/x.db)
        # compare equal for the same-database guard.
        return f"sqlalchemy:{_normalize_url(self._url)}"

    @property
    def table_names(self) -> list[str]:
        # sorted_tables yields parents before children (FK-safe order).
        return [t.name for t in Base.metadata.sorted_tables]

    async def init(self) -> None:
        await self._init_db()

    async def _table(self, table_name: str) -> Table:
        return Base.metadata.tables[table_name]

    async def read_table(self, table_name: str) -> list[dict[str, Any]]:
        table = await self._table(table_name)
        async with self._session_factory() as session:
            result = await session.execute(select(table))
            return [dict(row._mapping) for row in result]

    async def count_rows(self, table_name: str) -> int:
        # Count-only read: skipped tables and target-only detection only
        # need the count, never the row payloads.
        table = await self._table(table_name)
        async with self._session_factory() as session:
            result = await session.execute(select(func.count()).select_from(table))
            return result.scalar_one()

    async def write_rows(self, table_name: str, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        model_cls = _TABLE_TO_CLASS[table_name]
        async with self._session_factory() as session:
            # merge() upserts by primary key: re-runs never duplicate rows.
            for i, row in enumerate(rows, 1):
                await session.merge(model_cls(**row))
                if i % _WRITE_FLUSH_EVERY == 0:
                    # Bound the session identity map: without periodic
                    # flush/eviction a full-table write (556k episodes)
                    # accumulates every row in memory -- the write-side OOM
                    # counterpart of the read-side OOM tracked in XIN-111.
                    await session.flush()
                    session.expunge_all()
            await session.commit()
        return len(rows)

    async def close(self) -> None:
        await self._dispose()


def get_backend(name: str) -> Backend:
    """Resolve a named backend the way its module would.

    The module is imported lazily, *after* its env var is pinned to the
    resolved URL, so ``INGESTION_DATABASE_URL`` / ``DATABASE_URL`` overrides
    are honored exactly as if the module had been imported directly.

    Note: call this before the backend modules are imported elsewhere in the
    process -- they bind their engine URL at import time.
    """
    if name not in _VALID_BACKENDS:
        raise ValueError(f"Unknown backend {name!r} (expected one of {_VALID_BACKENDS})")
    if name == "dynamodb":
        # Same env vars the settings system maps to database.dynamodb.*;
        # read directly (not through the settings singleton) because source
        # and target may differ. Credentials always come from the standard
        # AWS chain, never from these variables.
        from backend.persistence.dynamodb.migrate_adapter import DynamoDBBackend

        return DynamoDBBackend(
            table_name=os.environ.get("DATABASE_DYNAMODB_TABLE_NAME", "tunedin"),
            region_name=os.environ.get("DATABASE_DYNAMODB_REGION", "us-east-1"),
            endpoint_url=os.environ.get("DATABASE_DYNAMODB_ENDPOINT_URL"),
        )
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
    skipped: bool = False


async def migrate(
    source: Backend, target: Backend, dry_run: bool = False
) -> list[TableReport]:
    """Copy every table from source to target. Returns a per-table report.

    Source tables missing from the target backend are skipped: a warning
    is printed to stderr for each and the table is flagged ``skipped`` in
    the returned report (their source counts come from a count-only read --
    the rows themselves are never loaded).
    """
    assert_distinct_backends(source, target)
    if not dry_run:
        await target.init()
    report: list[TableReport] = []
    common, source_only, _target_only = reconcile_tables(
        list(source.table_names), list(target.table_names)
    )
    # Warn about skipped tables before any reads: the current code used to
    # read_table() every source table first and only then discover it would
    # be skipped.
    for table_name in source_only:
        print(
            f"warning: skipping table {table_name!r}: not present in "
            f"target backend {target.name!r}",
            file=sys.stderr,
        )
    common_set = set(common)
    try:
        for table_name in source.table_names:
            if table_name not in common_set:
                report.append(
                    TableReport(
                        table=table_name,
                        source_rows=await source.count_rows(table_name),
                        copied_rows=0,
                        skipped=True,
                    )
                )
                continue
            rows = await source.read_table(table_name)
            copied = 0
            if not dry_run:
                copied = await target.write_rows(table_name, rows)
            report.append(
                TableReport(
                    table=table_name,
                    source_rows=len(rows),
                    copied_rows=copied,
                )
            )
    finally:
        await source.close()
        await target.close()
    return report


def print_report(report: list[TableReport], dry_run: bool) -> None:
    mode = "DRY RUN -- nothing written" if dry_run else "MIGRATED"
    print(f"{mode}")
    print(f"{'table':<28}{'source rows':>12}{'copied':>10}{'status':>10}")
    print("-" * 62)
    total_source = total_copied = 0
    for r in report:
        status = "skipped" if r.skipped else ""
        print(f"{r.table:<28}{r.source_rows:>12}{r.copied_rows:>10}{status:>10}")
        total_source += r.source_rows
        total_copied += r.copied_rows
    print("-" * 62)
    print(f"{'TOTAL':<28}{total_source:>12}{total_copied:>10}")


def main(argv: list[str] | None = None) -> int:
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
        # Show exactly what will be touched before any writes happen.
        # The operation stays upsert-only; this is visibility, not a prompt
        # (the CLI runs non-interactively).
        print(f"{source.identity} -> {target.identity}")
        report = asyncio.run(migrate(source, target, dry_run=args.dry_run))
    except (ValueError, SameBackendError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    print_report(report, args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
