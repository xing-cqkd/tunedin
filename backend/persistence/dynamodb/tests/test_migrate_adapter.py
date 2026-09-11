"""Migration-adapter tests (Linear: XIN-94).

Exercises ``backend.migrate_data.migrate`` end to end in both directions
(``simple -> dynamodb`` and ``dynamodb -> simple``) with the DynamoDB side
backed by moto. No test makes a real network call: the DynamoDB client is
a sync boto3 client under ``mock_aws`` with fake credentials, wrapped in
the async test adapter.

The SQL side uses throwaway SQLite files through ``SqlAlchemyBackend`` —
the same backend the CLI wires to ``simple``/``app``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List
from uuid import UUID, uuid4

import boto3
import pytest
from moto import mock_aws

from backend.migrate_data import SqlAlchemyBackend, migrate
from backend.persistence.dynamodb import codec
from backend.persistence.dynamodb import migrate_adapter as ma
from backend.persistence.dynamodb.migrate_adapter import DynamoDBBackend
from backend.persistence.dynamodb.table import DEFAULT_TABLE_NAME, ensure_table
from backend.persistence.dynamodb.testing import AsyncBoto3Client

UTC = timezone.utc

# Primary-key columns per table, for deterministic row ordering in parity
# comparisons.
_PK_COLUMNS = {
    "feeds": ("feed_id",),
    "episodes": ("episode_id",),
    "insights": ("insight_id",),
    "tags": ("tag_id",),
    "episode_tags": ("episode_id", "tag_id"),
    "users": ("user_id",),
    "curated_playlists": ("playlist_id",),
    "playlist_episodes": ("playlist_id", "episode_id"),
    "user_episode_progress": ("user_id", "episode_id"),
    "task_logs": ("task_log_id",),
}


def _norm(value: Any) -> Any:
    """Normalize a column value for cross-backend comparison."""
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            # SQLite returns naive datetimes; the codec normalizes naive
            # to UTC on the DynamoDB write path, so assume UTC here too.
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat()
    return value


def _norm_row(row: Dict[str, Any]) -> tuple:
    return tuple((k, _norm(v)) for k, v in sorted(row.items()))


def _norm_rows(table: str, rows: List[Dict[str, Any]]) -> List[tuple]:
    pk = _PK_COLUMNS[table]
    return sorted(
        (_norm_row(r) for r in rows),
        key=lambda r: tuple(v for k, v in r if k in pk),
    )


def _seed_rows() -> Dict[str, List[Dict[str, Any]]]:
    f1, f2 = uuid4(), uuid4()
    e1, e2, e3 = uuid4(), uuid4(), uuid4()
    t1, t2 = uuid4(), uuid4()
    u1 = uuid4()
    p1 = uuid4()
    now = datetime.now(UTC)
    return {
        "feeds": [
            {
                "feed_id": f1,
                "rss_url": "https://example.com/feed1.xml",
                "title": "Feed One",
                "author": "Author A",
                "description": None,
                "sync_status": "pending",
                "error_count": 0,
                "last_fetched_at": None,
                "created_at": now,
            },
            {
                "feed_id": f2,
                "rss_url": "https://example.com/feed2.xml",
                "title": "Feed Two",
                "sync_status": "error",
                "error_count": 3,
                "last_fetched_at": now,
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
                # Naive datetime: exercises the UTC-normalization path.
                "published_at": datetime(2026, 1, 6, 12, 0),
                "processed": True,
                "created_at": now,
            },
            {
                "episode_id": e3,
                "feed_id": f2,
                "guid": "guid-3",
                "title": "Episode 3",
                "audio_url": "https://example.com/e3.mp3",
                "published_at": None,
                "processed": False,
                "created_at": now,
            },
        ],
        "insights": [
            {
                "insight_id": uuid4(),
                "episode_id": e2,
                "title": "Insight 1",
                "created_at": now,
            },
        ],
        "tags": [
            {"tag_id": t1, "name": "tech", "category": "topic"},
            {"tag_id": t2, "name": "news", "category": None},
        ],
        "episode_tags": [
            {"episode_id": e1, "tag_id": t1},
            {"episode_id": e2, "tag_id": t2},
        ],
        "users": [
            {"user_id": u1, "email": "user@example.com", "created_at": now},
        ],
        "curated_playlists": [
            {
                "playlist_id": p1,
                "user_id": u1,
                "title": "My Playlist",
                "created_at": now,
            },
        ],
        "playlist_episodes": [
            {"playlist_id": p1, "episode_id": e1, "position": 2},
        ],
        "user_episode_progress": [
            {
                "user_id": u1,
                "episode_id": e1,
                "position_seconds": 120,
                "completed": False,
                "last_played_at": now,
            },
        ],
        "task_logs": [
            {
                "task_log_id": uuid4(),
                "task_type": "sync",
                "status": "done",
                "created_at": now,
            },
        ],
    }


@pytest.fixture()
def ddb_backend():
    """A DynamoDBBackend over a moto-backed table (no network)."""
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

        import asyncio

        asyncio.run(_setup())
        backend = DynamoDBBackend(client=client, table_name=DEFAULT_TABLE_NAME)
        yield backend, sync


async def _make_sql_backend(tmp_path, name: str) -> SqlAlchemyBackend:
    backend = SqlAlchemyBackend.from_url(
        name, f"sqlite+aiosqlite:///{tmp_path}/{name}.db"
    )
    await backend.init()
    return backend


async def _seed_sql(backend: SqlAlchemyBackend) -> Dict[str, List[Dict[str, Any]]]:
    seed = _seed_rows()
    for table in backend.table_names:
        if table in seed:
            await backend.write_rows(table, seed[table])
    return seed


async def _read_all(backend) -> Dict[str, List[Dict[str, Any]]]:
    out = {}
    for table in backend.table_names:
        out[table] = await backend.read_table(table)
    return out


def _assert_parity(
    expected: Dict[str, List[Dict[str, Any]]],
    actual: Dict[str, List[Dict[str, Any]]],
    label: str,
) -> None:
    assert set(expected) == set(actual) == set(_PK_COLUMNS), f"{label}: tables"
    for table in _PK_COLUMNS:
        assert _norm_rows(table, expected[table]) == _norm_rows(
            table, actual[table]
        ), f"{label}: {table} rows differ"



async def test_table_names_match_sqlalchemy(ddb_backend, tmp_path):
    backend, _ = ddb_backend
    sql = SqlAlchemyBackend.from_url("s", f"sqlite+aiosqlite:///{tmp_path}/s.db")
    try:
        assert backend.table_names == sql.table_names
        # FK-safe: parents before children.
        names = backend.table_names
        assert names.index("feeds") < names.index("episodes")
        assert names.index("episodes") < names.index("insights")
        assert names.index("tags") < names.index("episode_tags")
    finally:
        await sql.close()



async def test_migrate_simple_to_dynamodb_parity(ddb_backend, tmp_path):
    backend, sync = ddb_backend
    sql = await _make_sql_backend(tmp_path, "src")
    try:
        seed = await _seed_sql(sql)
        source_snapshot = await _read_all(sql)

        report = await migrate(sql, backend)
        assert sum(r.copied_rows for r in report) == sum(
            len(v) for v in seed.values()
        )

        migrated = await _read_all(
            DynamoDBBackend(client=backend._client, table_name=DEFAULT_TABLE_NAME)
        )
        _assert_parity(source_snapshot, migrated, "simple->dynamodb")

        # Status / unprocessed counts parity.
        feeds = migrated["feeds"]
        assert sum(1 for f in feeds if f["sync_status"] == "pending") == 1
        assert sum(1 for f in feeds if f["sync_status"] == "error") == 1
        episodes = migrated["episodes"]
        assert sum(1 for e in episodes if not e["processed"]) == 2

        # Spot-checked payload: episode 3 kept its guid / NULL timestamp.
        e3 = next(e for e in episodes if e["guid"] == "guid-3")
        assert e3["title"] == "Episode 3"
        assert e3["published_at"] is None

        # Link tables round-tripped with their payloads.
        pl_ep = migrated["playlist_episodes"]
        assert len(pl_ep) == 1 and pl_ep[0]["position"] == 2
        assert len(migrated["episode_tags"]) == 2

        # Guid markers were rebuilt (3 episodes have guids), never migrated.
        markers = sync.scan(
            TableName=DEFAULT_TABLE_NAME,
            FilterExpression="#t = :t",
            ExpressionAttributeNames={"#t": "type"},
            ExpressionAttributeValues={":t": {"S": codec.TYPE_GUID_MARKER}},
        )["Items"]
        assert len(markers) == 3
        claims = sync.scan(
            TableName=DEFAULT_TABLE_NAME,
            FilterExpression="#t = :t",
            ExpressionAttributeNames={"#t": "type"},
            ExpressionAttributeValues={":t": {"S": codec.TYPE_TAG_CLAIM}},
        )["Items"]
        assert claims == []
    finally:
        await sql.close()



async def test_migrate_dynamodb_to_simple_parity(ddb_backend, tmp_path):
    backend, _ = ddb_backend
    src = await _make_sql_backend(tmp_path, "src")
    dst = await _make_sql_backend(tmp_path, "dst")
    try:
        seed = await _seed_sql(src)
        source_snapshot = await _read_all(src)
        await migrate(src, backend)

        fresh_dynamo = DynamoDBBackend(
            client=backend._client, table_name=DEFAULT_TABLE_NAME
        )
        report = await migrate(fresh_dynamo, dst)
        assert sum(r.copied_rows for r in report) == sum(
            len(v) for v in seed.values()
        )

        round_tripped = await _read_all(dst)
        _assert_parity(source_snapshot, round_tripped, "dynamodb->simple")

        # Spot check: guid survived the round trip; link payloads intact.
        episodes = {str(e["episode_id"]): e for e in round_tripped["episodes"]}
        guids = sorted(e["guid"] for e in episodes.values() if e["guid"])
        assert guids == ["guid-1", "guid-2", "guid-3"]
        assert round_tripped["playlist_episodes"][0]["position"] == 2
    finally:
        await src.close()
        await dst.close()



async def test_migrate_is_idempotent(ddb_backend, tmp_path):
    backend, _ = ddb_backend
    url = f"sqlite+aiosqlite:///{tmp_path}/src.db"
    sql = SqlAlchemyBackend.from_url("src", url)
    await sql.init()
    try:
        await _seed_sql(sql)
        first = await migrate(
            sql, DynamoDBBackend(client=backend._client, table_name=DEFAULT_TABLE_NAME)
        )
        # Re-migrate the SAME database file: identical rows must overwrite,
        # never duplicate.
        sql_again = SqlAlchemyBackend.from_url("src", url)
        await sql_again.init()
        try:
            second = await migrate(
                sql_again,
                DynamoDBBackend(client=backend._client, table_name=DEFAULT_TABLE_NAME),
            )
        finally:
            await sql_again.close()
        assert [r.copied_rows for r in first] == [r.copied_rows for r in second]
        after = await _read_all(
            DynamoDBBackend(client=backend._client, table_name=DEFAULT_TABLE_NAME)
        )
        assert sum(len(v) for v in after.values()) == sum(
            r.copied_rows for r in first
        )
    finally:
        await sql.close()



async def test_write_rows_empty_is_noop(ddb_backend):
    backend, _ = ddb_backend
    assert await backend.write_rows("feeds", []) == 0


async def test_owned_client_entered_lazily_and_close_idempotent():
    # No injected client: construction alone must not touch aioboto3's
    # un-entered ClientCreatorContext, and close() before any use (the
    # dry-run path, where migrate() never calls init()) is a safe no-op.
    backend = DynamoDBBackend(table_name="never-used", region_name="us-east-1")
    assert backend._client is None
    await backend.close()
    await backend.close()

    # Entering the context is lazy but offline-safe: aioboto3 resolves
    # credentials only on the first real API call, so no network happens.
    client = await backend._ensure_client()
    assert hasattr(client, "meta")  # entered client, not the raw context
    await backend.close()
    await backend.close()  # idempotent
    assert backend._client is None


def test_migration_table_spec_coverage():
    # Explicit pin of the import-time coverage assertion: every SQL table
    # must have both a write builder and a read spec, and nothing extra —
    # so the next added model fails fast instead of breaking migration
    # with a cryptic KeyError/ValueError mid-run.
    assert set(ma._MIGRATION_TABLES) == set(ma._ITEM_BUILDERS) == set(ma._READ_SPECS)
    backend = DynamoDBBackend(client=object())  # injected; never touched here
    assert backend.table_names == list(ma._MIGRATION_TABLES)


class _FlakyBatchClient:
    """Fake DynamoDB client: batch_write_item returns UnprocessedItems
    until `succeed_after` calls, then succeeds. No network."""

    def __init__(self, succeed_after: int = 0):
        self.calls = 0
        self.succeed_after = succeed_after

    async def batch_write_item(self, RequestItems):
        self.calls += 1
        if self.calls <= self.succeed_after:
            return {"UnprocessedItems": RequestItems}
        return {"UnprocessedItems": {}}


async def test_batch_write_retries_unprocessed_items_with_backoff():
    sleeps: List[float] = []

    async def no_sleep(delay: float) -> None:
        sleeps.append(delay)

    client = _FlakyBatchClient(succeed_after=2)
    await ma._batch_write_chunk(
        client, "ddb-table", "feeds", [{"pk": {"S": "x"}}], sleep=no_sleep
    )
    assert client.calls == 3
    # Exponential backoff + jitter: [base, 2*base] then [2*base, 4*base].
    assert len(sleeps) == 2
    assert 0.1 <= sleeps[0] <= 0.2
    assert 0.2 <= sleeps[1] <= 0.4


async def test_batch_write_raises_after_max_attempts(monkeypatch):
    monkeypatch.setattr(ma, "_BATCH_WRITE_MAX_ATTEMPTS", 3)

    async def no_sleep(delay: float) -> None:
        pass

    client = _FlakyBatchClient(succeed_after=999)  # never succeeds
    with pytest.raises(RuntimeError, match="feeds"):
        await ma._batch_write_chunk(
            client, "ddb-table", "feeds", [{"pk": {"S": "x"}}], sleep=no_sleep
        )
    # Bounded: exactly the cap, then a loud failure naming the table —
    # no infinite hot loop against a throttled table.
    assert client.calls == 3


async def test_playlist_episode_link_added_at_round_trip(ddb_backend, tmp_path):
    """XIN-124 §4: added_at round-trips through the adapter both ways.

    Before the fix the write path dropped added_at, so DynamoDB
    list_entries synthesized _now_utc() per read and every migrated
    entry got a nondeterministic RSS <pubDate>.
    """
    backend, _ = ddb_backend
    u1, f1, e1, p1 = uuid4(), uuid4(), uuid4(), uuid4()
    added = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
    now = datetime.now(UTC)
    # FK-safe seed: the reverse migration lands in a pragma-enforced DB.
    await backend.write_rows(
        "users", [{"user_id": u1, "email": "u@example.com", "created_at": now}]
    )
    await backend.write_rows(
        "feeds",
        [
            {
                "feed_id": f1,
                "rss_url": "https://example.com/f.xml",
                "title": "F",
                "sync_status": "pending",
                "error_count": 0,
                "created_at": now,
            }
        ],
    )
    await backend.write_rows(
        "episodes",
        [
            {
                "episode_id": e1,
                "feed_id": f1,
                "guid": "e-guid",
                "title": "E",
                "audio_url": "https://example.com/e.mp3",
                "processed": False,
                "created_at": now,
            }
        ],
    )
    await backend.write_rows(
        "curated_playlists",
        [
            {
                "playlist_id": p1,
                "user_id": u1,
                "title": "P",
                "created_at": now,
            }
        ],
    )
    await backend.write_rows(
        "playlist_episodes",
        [
            {
                "playlist_id": p1,
                "episode_id": e1,
                "position": 3,
                "added_at": added,
            }
        ],
    )

    rows = await backend.read_table("playlist_episodes")
    assert len(rows) == 1
    assert rows[0]["position"] == 3
    assert _norm(rows[0]["added_at"]) == _norm(added)

    # Reverse direction: DynamoDB -> SQL preserves the instant too.
    sql = await _make_sql_backend(tmp_path, "dst")
    try:
        await migrate(backend, sql)
        back = await sql.read_table("playlist_episodes")
        assert len(back) == 1
        assert back[0]["position"] == 3
        assert _norm(back[0]["added_at"]) == _norm(added)
    finally:
        await sql.close()
