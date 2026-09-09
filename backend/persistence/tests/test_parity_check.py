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

from backend.migrate_data import SqlAlchemyBackend, migrate
from backend.parity_check import compare
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
