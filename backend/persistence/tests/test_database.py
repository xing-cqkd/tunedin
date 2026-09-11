"""Tests for backend.persistence.database (Linear: XIN-133).

Covers ``init_db``'s alembic stamp-vs-upgrade decision, the shared
``configure_sqlite_fk`` helper wiring, and the ``get_db``/``session_scope``
alias. Alembic's stamp/upgrade commands are stubbed -- these tests assert
the *decision*, never run a real migration.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

import alembic.command
from backend.persistence import database
from backend.persistence._sqlite import configure_sqlite_fk
from backend.persistence.models import Base


@pytest.fixture()
def tmp_engine(tmp_path, monkeypatch):
    """Point database.init_db at a throwaway sqlite file."""
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/db.db")
    monkeypatch.setattr(database, "engine", engine)
    yield engine
    # NOTE: no dispose -- existing tests in this package create engines the
    # same way; connections close on garbage collection.


def _stub_alembic(monkeypatch):
    calls = []
    monkeypatch.setattr(
        alembic.command, "stamp", lambda cfg, rev: calls.append(("stamp", rev))
    )
    monkeypatch.setattr(
        alembic.command, "upgrade", lambda cfg, rev: calls.append(("upgrade", rev))
    )
    return calls


async def _create_app_schema(engine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def test_init_db_stamps_existing_schema_without_version_table(
    tmp_engine, monkeypatch
):
    # A database built by the old create_all path has app tables but no
    # alembic_version -- it must be stamped at head, not migrated (a wrong
    # decision here corrupts schema history).
    await _create_app_schema(tmp_engine)
    calls = _stub_alembic(monkeypatch)

    await database.init_db()

    assert calls == [("stamp", "head")]


async def test_init_db_upgrades_fresh_database(tmp_engine, monkeypatch):
    calls = _stub_alembic(monkeypatch)

    await database.init_db()

    assert calls == [("upgrade", "head")]


async def test_init_db_upgrades_when_version_table_present(tmp_engine, monkeypatch):
    await _create_app_schema(tmp_engine)
    async with tmp_engine.begin() as conn:
        await conn.execute(
            text("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
        )
    calls = _stub_alembic(monkeypatch)

    await database.init_db()

    assert calls == [("upgrade", "head")]


async def test_configure_sqlite_fk_enables_pragma(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/fk.db")
    configure_sqlite_fk(engine)
    try:
        async with engine.connect() as conn:
            pragma = await conn.exec_driver_sql("PRAGMA foreign_keys")
            assert pragma.scalar() == 1
    finally:
        await engine.dispose()


async def test_configure_sqlite_fk_enables_wal_and_busy_timeout(tmp_path):
    """XIN-58: the shared SQLite listener sets WAL mode + busy timeout so
    concurrent workers on one SQLite file don't hit 'database is locked'."""
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/wal.db")
    configure_sqlite_fk(engine)
    try:
        async with engine.connect() as conn:
            journal_mode = await conn.exec_driver_sql("PRAGMA journal_mode")
            assert journal_mode.scalar() == "wal"
            busy_timeout = await conn.exec_driver_sql("PRAGMA busy_timeout")
            assert busy_timeout.scalar() == 5000
    finally:
        await engine.dispose()


def test_get_db_is_session_scope_alias():
    # get_db is kept only as the conventional FastAPI dependency name;
    # behaviorally it is session_scope (XIN-133).
    assert database.get_db is database.session_scope
