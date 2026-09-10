"""Alembic migration chain tests (XIN-122).

Exercises upgrade AND downgrade of the full migration chain against a
scratch SQLite file, and pins the schema properties the cleanup issues
depend on:

* XIN-121: the two partial unique tag indexes replace
  ``uq_tag_name_category`` — duplicate ``('name', NULL)`` inserts raise.
* XIN-122: ``playlist_episodes.added_at`` and ``episodes.episode_type``
  are NOT NULL with server defaults, matching the models.

The downgrade run also pins the supported behavior noted in XIN-122: the
``f3a8c1d2e4b5`` downgrade calls ``op.drop_column`` outside batch mode,
which works on modern SQLite (this repo's) and would raise
``NotImplementedError`` on old SQLite.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import uuid
from pathlib import Path

import pytest
from alembic import command as alembic_command
from alembic.config import Config

REPO_ROOT = Path(__file__).resolve().parents[3]
ALEMBIC_INI = str(REPO_ROOT / "backend" / "alembic.ini")

HEAD = "742ddc0a7799"  # current chain head (XIN-122 not-null migration)
TAG_MIGRATION_DOWN = "f3a8c1d2e4b5"  # revision before the XIN-121 migration


def _db_url(tmp_path, name: str = "mig.db") -> str:
    return f"sqlite+aiosqlite:///{tmp_path}/{name}"


async def _migrate(monkeypatch, tmp_path, revision: str) -> Path:
    """Run `alembic upgrade/downgrade <revision>` against a scratch file DB."""
    db_path = tmp_path / "mig.db"
    monkeypatch.setenv("DATABASE_URL", _db_url(tmp_path))
    cfg = Config(ALEMBIC_INI)
    # env.py drives its own event loop via asyncio.run(), so run the
    # command in a thread (same pattern as database.init_db).
    if revision.startswith("-"):
        await asyncio.to_thread(
            alembic_command.downgrade, cfg, revision[1:]
        )
    else:
        await asyncio.to_thread(alembic_command.upgrade, cfg, revision)
    return db_path


def _connect(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(os.fspath(db_path))
    con.execute("PRAGMA foreign_keys=ON")
    return con


def _tables(con: sqlite3.Connection) -> set[str]:
    return {
        r[0]
        for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }


def _version(con: sqlite3.Connection) -> str | None:
    row = con.execute("SELECT version_num FROM alembic_version").fetchone()
    return row[0] if row else None


def _index_sql(con: sqlite3.Connection, name: str) -> str | None:
    row = con.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND name=?", (name,)
    ).fetchone()
    return row[0] if row else None


def _notnull(con: sqlite3.Connection, table: str, column: str) -> bool:
    row = next(
        r for r in con.execute(f"PRAGMA table_info({table})") if r[1] == column
    )
    return bool(row[3])


def _server_default(con: sqlite3.Connection, table: str, column: str):
    row = next(
        r for r in con.execute(f"PRAGMA table_info({table})") if r[1] == column
    )
    return row[4]


async def test_upgrade_head_then_downgrade_base_round_trip(
    monkeypatch, tmp_path
):
    """The whole chain upgrades, downgrades to base, and upgrades again."""
    db_path = await _migrate(monkeypatch, tmp_path, "head")
    with _connect(db_path) as con:
        assert _version(con) == HEAD
        assert {"feeds", "episodes", "tags", "curated_playlists"}.issubset(
            _tables(con)
        )

    await _migrate(monkeypatch, tmp_path, "-base")
    with _connect(db_path) as con:
        # All app tables are gone; only the version table remains.
        assert "feeds" not in _tables(con)
        assert "tags" not in _tables(con)

    await _migrate(monkeypatch, tmp_path, "head")
    with _connect(db_path) as con:
        assert _version(con) == HEAD
        assert "feeds" in _tables(con)


async def test_tag_partial_unique_indexes_replace_constraint(
    monkeypatch, tmp_path
):
    """XIN-121: duplicate ('name', NULL) inserts raise after upgrade."""
    db_path = await _migrate(monkeypatch, tmp_path, "head")
    with _connect(db_path) as con:
        null_idx = _index_sql(con, "uq_tag_name_null_category")
        assert null_idx is not None
        assert "WHERE category IS NULL" in null_idx
        cat_idx = _index_sql(con, "uq_tag_name_category")
        assert cat_idx is not None
        assert "WHERE category IS NOT NULL" in cat_idx

        def add_tag(name, category):
            con.execute(
                "INSERT INTO tags (tag_id, name, category) VALUES (?, ?, ?)",
                (str(uuid.uuid4()), name, category),
            )

        # The NULL-category case the old constraint never enforced.
        add_tag("rust", None)
        with pytest.raises(sqlite3.IntegrityError):
            add_tag("rust", None)
        con.rollback()

        # Distinct categories still coexist; same category still dedupes.
        add_tag("rust", "topic")
        add_tag("rust", "mood")
        with pytest.raises(sqlite3.IntegrityError):
            add_tag("rust", "topic")
        con.rollback()


async def test_tag_migration_downgrade_restores_old_constraint(
    monkeypatch, tmp_path
):
    """XIN-121 downgrade: partial indexes go away, the old constraint returns."""
    await _migrate(monkeypatch, tmp_path, "head")
    db_path = await _migrate(monkeypatch, tmp_path, f"-{TAG_MIGRATION_DOWN}")
    with _connect(db_path) as con:
        assert _index_sql(con, "uq_tag_name_null_category") is None
        assert _index_sql(con, "uq_tag_name_category") is None
        # The old behavior is back: NULL categories are not deduplicated.
        con.execute(
            "INSERT INTO tags (tag_id, name, category) VALUES (?, 'go', NULL)",
            (str(uuid.uuid4()),),
        )
        con.execute(
            "INSERT INTO tags (tag_id, name, category) VALUES (?, 'go', NULL)",
            (str(uuid.uuid4()),),
        )
        con.rollback()


async def test_not_null_columns_match_models(monkeypatch, tmp_path):
    """XIN-122: added_at / episode_type are NOT NULL with server defaults."""
    db_path = await _migrate(monkeypatch, tmp_path, "head")
    with _connect(db_path) as con:
        assert _notnull(con, "playlist_episodes", "added_at")
        assert _server_default(con, "playlist_episodes", "added_at") == (
            "CURRENT_TIMESTAMP"
        )
        assert _notnull(con, "episodes", "episode_type")
        assert _server_default(con, "episodes", "episode_type") == "'full'"
        # Unrelated columns keep their nullability.
        assert not _notnull(con, "episodes", "guid")


async def test_not_null_migration_downgrade_restores_nullable(
    monkeypatch, tmp_path
):
    """XIN-122 downgrade: the columns become nullable again."""
    await _migrate(monkeypatch, tmp_path, "head")
    db_path = await _migrate(monkeypatch, tmp_path, f"-{HEAD}")
    with _connect(db_path) as con:
        assert _version(con) == "6ceb43cc6cf5"
        assert not _notnull(con, "playlist_episodes", "added_at")
        assert not _notnull(con, "episodes", "episode_type")
