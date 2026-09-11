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

HEAD = "0f3a4b5c6d7e"  # current chain head (XIN-68 guid backfill + NOT NULL)
GUID_MIGRATION_DOWN = "9e2f3a4b5c6d"  # revision before the XIN-68 migration
TEXT_MIGRATION_DOWN = "8d1e2f3a4b5c"  # revision before the XIN-47 migration
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
    """XIN-122: added_at / episode_type are NOT NULL with server defaults.

    XIN-68: episodes.guid is NOT NULL as well (the nullable guid defeated
    the (feed_id, guid) dedup). Unrelated columns keep their nullability.
    """
    db_path = await _migrate(monkeypatch, tmp_path, "head")
    with _connect(db_path) as con:
        assert _notnull(con, "playlist_episodes", "added_at")
        assert _server_default(con, "playlist_episodes", "added_at") == (
            "CURRENT_TIMESTAMP"
        )
        assert _notnull(con, "episodes", "episode_type")
        assert _server_default(con, "episodes", "episode_type") == "'full'"
        assert _notnull(con, "episodes", "guid")
        # Unrelated columns keep their nullability.
        assert not _notnull(con, "episodes", "summary")


async def test_not_null_migration_downgrade_restores_nullable(
    monkeypatch, tmp_path
):
    """XIN-122 downgrade: the columns become nullable again.

    Downgrades past the 742ddc0a7799 migration (the XIN-122 not-null one);
    the later migrations in the chain are undone along the way.
    """
    await _migrate(monkeypatch, tmp_path, "head")
    db_path = await _migrate(monkeypatch, tmp_path, "-6ceb43cc6cf5")
    with _connect(db_path) as con:
        assert _version(con) == "6ceb43cc6cf5"
        assert not _notnull(con, "playlist_episodes", "added_at")
        assert not _notnull(con, "episodes", "episode_type")


def _column_type(con: sqlite3.Connection, table: str, column: str) -> str:
    row = next(
        r for r in con.execute(f"PRAGMA table_info({table})") if r[1] == column
    )
    return row[2]


async def test_hot_path_indexes_exist(monkeypatch, tmp_path):
    """XIN-46: the ingestion hot paths are indexed after upgrade."""
    db_path = await _migrate(monkeypatch, tmp_path, "head")
    with _connect(db_path) as con:
        status_idx = _index_sql(con, "ix_feeds_sync_status")
        assert status_idx is not None
        assert "sync_status" in status_idx

        composite_idx = _index_sql(con, "ix_episodes_feed_processed")
        assert composite_idx is not None
        # Composite (feed_id, processed): feed_id must come first so the
        # (feed_id, processed) filter and the bare feed_id prefix both use it.
        # (Search the parenthesized column list — the index name itself
        # contains "processed".)
        columns = composite_idx[composite_idx.index("("):]
        assert columns.index("feed_id") < columns.index("processed")


async def test_title_and_url_columns_are_unbounded_text(monkeypatch, tmp_path):
    """XIN-47: title/URL columns are TEXT (unbounded) after upgrade."""
    db_path = await _migrate(monkeypatch, tmp_path, "head")
    with _connect(db_path) as con:
        assert _column_type(con, "episodes", "title") == "TEXT"
        assert _column_type(con, "episodes", "audio_url") == "TEXT"
        assert _column_type(con, "feeds", "rss_url") == "TEXT"
        assert _column_type(con, "feeds", "title") == "TEXT"


async def test_guid_backfill_assigns_deterministic_guids(monkeypatch, tmp_path):
    """XIN-68: NULL guids are backfilled deterministically, then NOT NULL."""
    import hashlib

    # Migrate only to the revision before the XIN-68 migration, where
    # guid is still nullable.
    db_path = await _migrate(monkeypatch, tmp_path, GUID_MIGRATION_DOWN)
    feed_id = str(uuid.uuid4())
    ep_by_audio = str(uuid.uuid4())
    ep_by_title = str(uuid.uuid4())
    ep_bare = str(uuid.uuid4())
    with _connect(db_path) as con:
        con.execute(
            "INSERT INTO feeds (feed_id, rss_url, title, sync_status,"
            " error_count, created_at)"
            " VALUES (?, ?, ?, 'pending', 0, CURRENT_TIMESTAMP)",
            (feed_id, "https://example.com/backfill.xml", "Backfill Feed"),
        )
        con.execute(
            "INSERT INTO episodes (episode_id, feed_id, guid, title,"
            " audio_url, episode_type, processed, created_at)"
            " VALUES (?, ?, NULL, ?, ?, 'full', 0, CURRENT_TIMESTAMP)",
            (ep_by_audio, feed_id, "Ep One", "https://example.com/audio1.mp3"),
        )
        con.execute(
            "INSERT INTO episodes (episode_id, feed_id, guid, title,"
            " audio_url, published_at, episode_type, processed, created_at)"
            " VALUES (?, ?, NULL, ?, '', ?, 'full', 0, CURRENT_TIMESTAMP)",
            (ep_by_title, feed_id, "Title Two", "2026-01-01 00:00:00"),
        )
        con.execute(
            "INSERT INTO episodes (episode_id, feed_id, guid, title,"
            " audio_url, episode_type, processed, created_at)"
            " VALUES (?, ?, NULL, '', '', 'full', 0, CURRENT_TIMESTAMP)",
            (ep_bare, feed_id),
        )
        con.commit()

    db_path = await _migrate(monkeypatch, tmp_path, "head")
    with _connect(db_path) as con:
        assert _notnull(con, "episodes", "guid")
        rows = {
            r[0]: r[1]
            for r in con.execute(
                "SELECT episode_id, guid FROM episodes WHERE feed_id = ?",
                (feed_id,),
            )
        }
        assert len(rows) == 3
        # No NULLs survived, and every fallback is marked as synthesized.
        assert all(g is not None for g in rows.values())
        assert all(g.startswith("tunedin-fallback-") for g in rows.values())
        # Deterministic: sha1(feed_id | seed).
        def expected(seed: str) -> str:
            digest = hashlib.sha1(
                f"{feed_id}|{seed}".encode("utf-8")
            ).hexdigest()
            return f"tunedin-fallback-{digest}"

        assert rows[ep_by_audio] == expected("https://example.com/audio1.mp3")
        assert rows[ep_by_title] == expected("Title Two|2026-01-01 00:00:00")
        assert rows[ep_bare] == expected(ep_bare)
        assert len(set(rows.values())) == 3

        # The (feed_id, guid) dedup now actually bites: a duplicate guid
        # for the same feed raises.
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "INSERT INTO episodes (episode_id, feed_id, guid, title,"
                " audio_url, episode_type, processed, created_at)"
                " VALUES (?, ?, ?, 'Dup', 'https://example.com/dup.mp3',"
                " 'full', 0, CURRENT_TIMESTAMP)",
                (str(uuid.uuid4()), feed_id, rows[ep_by_audio]),
            )
        con.rollback()

        # And inserting a NULL guid is now rejected outright.
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "INSERT INTO episodes (episode_id, feed_id, guid, title,"
                " audio_url, episode_type, processed, created_at)"
                " VALUES (?, ?, NULL, 'No Guid',"
                " 'https://example.com/noguid.mp3', 'full', 0,"
                " CURRENT_TIMESTAMP)",
                (str(uuid.uuid4()), feed_id),
            )
        con.rollback()
