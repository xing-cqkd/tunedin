"""Tests for the parity_check script (Linear: XIN-95).

Seeds a SQL backend, migrates to a moto-backed DynamoDB backend with the
real ``migrate()`` orchestration, then asserts :func:`compare` reports
parity — and that it catches row-count, missing-row, and payload
differences. No test touches real AWS.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List
from uuid import uuid4

import boto3
import pytest
from moto import mock_aws

from backend.migrate_data import Backend, SqlAlchemyBackend, migrate
from backend import parity_check
from backend.parity_check import _run, compare, main
from backend.persistence.dynamodb.migrate_adapter import DynamoDBBackend
from backend.persistence.dynamodb.table import DEFAULT_TABLE_NAME, ensure_table
from backend.persistence.dynamodb.testing import AsyncBoto3Client

UTC = timezone.utc


def _seed_rows() -> Dict[str, List[Dict[str, Any]]]:
    f1, f2 = uuid4(), uuid4()
    e1, e2, e3 = uuid4(), uuid4(), uuid4()
    t1 = uuid4()
    now = datetime.now(UTC)
    return {
        "feeds": [
            {
                "feed_id": f1,
                "rss_url": "https://example.com/feed1.xml",
                "title": "Feed One",
                "sync_status": "pending",
                "error_count": 0,
                "created_at": now,
            },
            {
                "feed_id": f2,
                "rss_url": "https://example.com/feed2.xml",
                "title": "Feed Two",
                "sync_status": "active",
                "error_count": 0,
                "created_at": now,
            },
        ],
        "episodes": [
            {
                "episode_id": e1,
                "feed_id": f1,
                "guid": "guid-1",
                "title": "Episode 1",
                "audio_url": "https://example.com/e1.mp3",
                "published_at": datetime(2026, 1, 5, 12, 0, tzinfo=UTC),
                "processed": False,
                "created_at": now,
            },
            {
                "episode_id": e2,
                "feed_id": f1,
                "guid": "guid-2",
                "title": "Episode 2",
                "audio_url": "https://example.com/e2.mp3",
                "published_at": datetime(2026, 1, 6, 12, 0),  # naive -> UTC
                "processed": True,
                "created_at": now,
            },
            {
                "episode_id": e3,
                "feed_id": f2,
                "guid": None,
                "title": "Episode 3",
                "audio_url": "https://example.com/e3.mp3",
                "published_at": None,
                "processed": False,
                "created_at": now,
            },
        ],
        "tags": [{"tag_id": t1, "name": "tech", "category": "topic"}],
        "episode_tags": [{"episode_id": e1, "tag_id": t1}],
    }


@pytest.fixture()
def backends(tmp_path):
    """(sql_backend, ddb_backend) with the SQL side seeded; moto, no network."""
    with mock_aws():
        sync = boto3.client(
            "dynamodb",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )
        client = AsyncBoto3Client(sync)

        async def _setup():
            await ensure_table(client, table_name=DEFAULT_TABLE_NAME)

        asyncio.run(_setup())
        ddb = DynamoDBBackend(client=client, table_name=DEFAULT_TABLE_NAME)
        sql = SqlAlchemyBackend.from_url(
            "s", f"sqlite+aiosqlite:///{tmp_path}/parity.db"
        )

        async def _seed():
            await sql.init()
            seed = _seed_rows()
            for table in sql.table_names:
                if table in seed:
                    await sql.write_rows(table, seed[table])
            return seed

        seed = asyncio.run(_seed())
        yield sql, ddb, seed
        asyncio.run(sql.close())
        asyncio.run(ddb.close())


def _table(report, name):
    return next(t for t in report.tables if t.table == name)


async def test_compare_clean_after_migrate_both_directions(backends):
    sql, ddb, seed = backends
    await migrate(sql, ddb)

    fwd = await compare(sql, ddb)
    assert fwd.ok, [ (t.table, t.sample_mismatches) for t in fwd.tables if not t.ok ]

    rev = await compare(ddb, sql)
    assert rev.ok


async def test_compare_reports_status_and_unprocessed_counts(backends):
    sql, ddb, seed = backends
    await migrate(sql, ddb)
    report = await compare(sql, ddb)

    feeds = _table(report, "feeds")
    assert feeds.source_by_status == feeds.target_by_status == {
        "pending": 1,
        "active": 1,
    }
    episodes = _table(report, "episodes")
    assert episodes.source_unprocessed == episodes.target_unprocessed == 2
    assert episodes.source_rows == episodes.target_rows == 3


async def test_compare_detects_missing_row(backends):
    sql, ddb, seed = backends
    await migrate(sql, ddb)
    # A row added to the source after migration must show up as missing.
    extra = {
        "episode_id": uuid4(),
        "feed_id": seed["feeds"][0]["feed_id"],
        "guid": "guid-4",
        "title": "Episode 4",
        "audio_url": "https://example.com/e4.mp3",
        "published_at": None,
        "processed": False,
        "created_at": datetime.now(UTC),
    }
    await sql.write_rows("episodes", [extra])

    report = await compare(sql, ddb)
    assert not report.ok
    episodes = _table(report, "episodes")
    assert episodes.source_rows == 4 and episodes.target_rows == 3
    kinds = [k for k, _ in episodes.sample_mismatches]
    assert "missing-row" in kinds


async def test_compare_detects_payload_difference(backends):
    sql, ddb, seed = backends
    await migrate(sql, ddb)
    # Change one column on an existing row (full row dict: merge upserts).
    changed = dict(seed["feeds"][0])
    changed["title"] = "Feed One (renamed)"
    await sql.write_rows("feeds", [changed])

    report = await compare(sql, ddb)
    assert not report.ok
    feeds = _table(report, "feeds")
    assert feeds.source_rows == feeds.target_rows == 2  # counts still match
    payload_diffs = [d for k, d in feeds.sample_mismatches if k == "payload-diff"]
    assert payload_diffs and "title" in payload_diffs[0]


async def test_compare_detects_status_count_mismatch(backends):
    sql, ddb, seed = backends
    await migrate(sql, ddb)
    changed = dict(seed["feeds"][0])
    changed["sync_status"] = "error"
    await sql.write_rows("feeds", [changed])

    report = await compare(sql, ddb)
    assert not report.ok
    feeds = _table(report, "feeds")
    assert feeds.source_by_status != feeds.target_by_status


class _ExtraTableBackend:
    """Wraps a Backend, pretending it holds one extra (target-only) table."""

    def __init__(
        self, inner: Backend, extra_table: str, extra_rows: List[Dict[str, Any]]
    ):
        self._inner = inner
        self.name = inner.name
        self._extra_table = extra_table
        self._extra_rows = extra_rows

    @property
    def identity(self) -> str:
        return self._inner.identity

    @property
    def table_names(self) -> List[str]:
        return [*self._inner.table_names, self._extra_table]

    async def init(self) -> None:
        await self._inner.init()

    async def read_table(self, table_name: str) -> List[Dict[str, Any]]:
        if table_name == self._extra_table:
            return [dict(r) for r in self._extra_rows]
        return await self._inner.read_table(table_name)

    async def count_rows(self, table_name: str) -> int:
        if table_name == self._extra_table:
            return len(self._extra_rows)
        return await self._inner.count_rows(table_name)

    async def write_rows(self, table_name: str, rows: List[Dict[str, Any]]) -> int:
        return await self._inner.write_rows(table_name, rows)

    async def close(self) -> None:
        await self._inner.close()


async def test_compare_flags_target_only_tables(backends):
    sql, ddb, seed = backends
    await migrate(sql, ddb)
    # Simulate a stale table left in the target by an earlier partial backfill.
    target = _ExtraTableBackend(ddb, "leftover_table", [{"id": 1}, {"id": 2}])

    report = await compare(sql, target)
    assert not report.ok
    leftover = _table(report, "leftover_table")
    assert leftover.source_rows == -1 and leftover.target_rows == 2
    kinds = [k for k, _ in leftover.sample_mismatches]
    assert "target-only-table" in kinds
    # Source tables keep their original order and still pass.
    assert [t.table for t in report.tables][: len(sql.table_names)] == list(
        sql.table_names
    )
    assert all(_table(report, name).ok for name in sql.table_names)


class _SourceOnlyTableBackend:
    """Wraps a Backend, pretending the source holds one extra table."""

    def __init__(self, inner: Backend, extra_table: str):
        self._inner = inner
        self.name = inner.name
        self._extra_table = extra_table

    @property
    def identity(self) -> str:
        return self._inner.identity

    @property
    def table_names(self) -> List[str]:
        return [*self._inner.table_names, self._extra_table]

    async def init(self) -> None:
        await self._inner.init()

    async def read_table(self, table_name: str) -> List[Dict[str, Any]]:
        return await self._inner.read_table(table_name)

    async def count_rows(self, table_name: str) -> int:
        return await self._inner.count_rows(table_name)

    async def write_rows(self, table_name: str, rows: List[Dict[str, Any]]) -> int:
        return await self._inner.write_rows(table_name, rows)

    async def close(self) -> None:
        await self._inner.close()


async def test_compare_reports_source_side_missing_table(backends):
    sql, ddb, seed = backends
    await migrate(sql, ddb)
    # A table present in the source but absent from the target is a
    # "missing-table" mismatch (only the target-only branch was tested).
    source = _SourceOnlyTableBackend(sql, "ghost_table")

    report = await compare(source, ddb)
    assert not report.ok
    ghost = _table(report, "ghost_table")
    assert ghost.source_rows == -1 and ghost.target_rows == -1
    kinds = [k for k, _ in ghost.sample_mismatches]
    assert "missing-table" in kinds


async def test_compare_empty_string_treated_as_none_after_migrate(backends):
    sql, ddb, seed = backends
    # feedparser yields "" for missing text fields; the DynamoDB codec maps
    # "" -> attribute-omitted -> None on read BY DESIGN, so compare() must
    # treat them as equivalent (XIN-131) -- not a payload-diff.
    await sql.write_rows(
        "feeds",
        [
            {
                "feed_id": uuid4(),
                "rss_url": "https://example.com/empty-title.xml",
                "title": "",
                "sync_status": "pending",
                "error_count": 0,
                "created_at": datetime.now(UTC),
            }
        ],
    )
    await migrate(sql, ddb)

    fwd = await compare(sql, ddb)
    assert fwd.ok, [(t.table, t.sample_mismatches) for t in fwd.tables if not t.ok]
    rev = await compare(ddb, sql)
    assert rev.ok, [(t.table, t.sample_mismatches) for t in rev.tables if not t.ok]


async def test_compare_missing_row_detected_with_sample_zero(backends):
    sql, ddb, seed = backends
    await migrate(sql, ddb)
    extra = {
        "episode_id": uuid4(),
        "feed_id": seed["feeds"][0]["feed_id"],
        "guid": "guid-9",
        "title": "Episode 9",
        "audio_url": "https://example.com/e9.mp3",
        "published_at": None,
        "processed": False,
        "created_at": datetime.now(UTC),
    }
    await sql.write_rows("episodes", [extra])
    # Existence is checked over the FULL pk sets, so a missing row is
    # still caught when payload sampling is disabled (XIN-131).
    report = await compare(sql, ddb, sample_size=0)
    assert not report.ok
    episodes = _table(report, "episodes")
    assert [k for k, _ in episodes.sample_mismatches] == ["missing-row"]


# ---------------------------------------------------------------------------
# CLI tests: main() / _run() exit codes (XIN-131 coverage)
# ---------------------------------------------------------------------------


class _MemoryBackend(Backend):
    """In-memory Backend for CLI tests (no sqlite, no moto)."""

    def __init__(self, name: str, identity: str, rows: Dict[str, List[Dict[str, Any]]]):
        self.name = name
        self._identity = identity
        self._rows = {t: [dict(r) for r in rs] for t, rs in rows.items()}

    @property
    def identity(self) -> str:
        return self._identity

    @property
    def table_names(self) -> List[str]:
        return list(self._rows)

    async def init(self) -> None:
        pass

    async def read_table(self, table_name: str) -> List[Dict[str, Any]]:
        return [dict(r) for r in self._rows[table_name]]

    async def write_rows(self, table_name: str, rows: List[Dict[str, Any]]) -> int:
        self._rows.setdefault(table_name, []).extend(dict(r) for r in rows)
        return len(rows)

    async def close(self) -> None:
        pass


def _feed_rows(title: str = "Feed", feed_id=None):
    return [
        {
            "feed_id": feed_id if feed_id is not None else uuid4(),
            "rss_url": "https://example.com/f.xml",
            "title": title,
            "sync_status": "active",
        }
    ]


def _payload_pair(title_a: str, title_b: str):
    """Two backends holding the SAME feed row except for ``title``.

    Same pk on both sides, so the only mismatch is a payload-diff (no
    missing/extra rows to confuse the sampling tests).
    """
    feed_id = uuid4()
    return (
        _MemoryBackend("simple", "mem:one", {"feeds": _feed_rows(title_a, feed_id)}),
        _MemoryBackend("app", "mem:two", {"feeds": _feed_rows(title_b, feed_id)}),
    )


def _cli_backends(monkeypatch, source: _MemoryBackend, target: _MemoryBackend):
    """Route the CLI's get_backend() at the two in-memory backends."""
    mapping = {"simple": source, "app": target}
    monkeypatch.setattr(parity_check, "get_backend", lambda name: mapping[name])


def test_main_exits_zero_on_parity(monkeypatch, capsys):
    source, target = _payload_pair("Feed", "Feed")
    _cli_backends(monkeypatch, source, target)
    assert main(["--source", "simple", "--target", "app"]) == 0
    assert "PARITY OK" in capsys.readouterr().out


def test_main_exits_one_on_mismatch(monkeypatch, capsys):
    source, target = _payload_pair("Feed", "Renamed")
    _cli_backends(monkeypatch, source, target)
    assert main(["--source", "simple", "--target", "app"]) == 1
    assert "PARITY MISMATCH" in capsys.readouterr().out


def test_main_exits_two_on_same_backend(monkeypatch, capsys):
    rows = {"feeds": _feed_rows()}
    _cli_backends(
        monkeypatch,
        _MemoryBackend("simple", "mem:same", rows),
        _MemoryBackend("app", "mem:same", rows),
    )
    # Safety-critical refusal path: never compare a database with itself.
    assert main(["--source", "simple", "--target", "app"]) == 2
    assert "refusing" in capsys.readouterr().out


def test_main_sample_zero_skips_payload_diff(monkeypatch):
    source, target = _payload_pair("Feed", "Renamed")
    _cli_backends(monkeypatch, source, target)
    # Counts match; --sample 0 disables payload spot-checks only.
    assert main(["--source", "simple", "--target", "app", "--sample", "0"]) == 0


def test_main_negative_sample_skips_payload_diff(monkeypatch):
    source, target = _payload_pair("Feed", "Renamed")
    _cli_backends(monkeypatch, source, target)
    assert main(["--source", "simple", "--target", "app", "--sample", "-5"]) == 0


def test_main_sample_zero_still_catches_missing_row(monkeypatch):
    _cli_backends(
        monkeypatch,
        _MemoryBackend("simple", "mem:one", {"feeds": _feed_rows()}),
        _MemoryBackend("app", "mem:two", {"feeds": []}),
    )
    assert main(["--source", "simple", "--target", "app", "--sample", "0"]) == 1


async def test_run_refuses_same_backend(monkeypatch, capsys):
    rows = {"feeds": _feed_rows()}
    _cli_backends(
        monkeypatch,
        _MemoryBackend("simple", "mem:same", rows),
        _MemoryBackend("app", "mem:same", rows),
    )
    assert await _run("simple", "app", 25) == 2
    assert "refusing" in capsys.readouterr().out
