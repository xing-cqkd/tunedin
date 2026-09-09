"""DynamoDB Local integration tests (Linear: XIN-91, XIN-92).

Runs the transactional guid-dedup path from ``EpisodeRepository.save_many``
and ``ensure_table()`` against a REAL DynamoDB engine instead of moto.
moto has known fidelity gaps around ``TransactWriteItems`` conditional
checks, so this module is the only proof the ``attribute_not_exists(pk)``
marker condition actually rejects duplicates outside moto.

How to run (slow tests are opt-in via ``-m slow``)::

    docker run -d -p 8000:8000 amazon/dynamodb-local
    .venv/bin/pytest backend/ -q -m slow
    docker stop <container>

When DynamoDB Local is not reachable on ``localhost:8000`` (override with
``DYNAMODB_LOCAL_HOST`` / ``DYNAMODB_LOCAL_PORT``) every test in this module
SKIPS — it never fails for a missing emulator. No AWS credentials are
needed; fake ones are set because botocore requires *something* in the
chain (DynamoDB Local ignores them). Nothing here touches real AWS.

Note: ``ensure_table`` is called with ``enable_pitr=False`` here because
DynamoDB Local rejects ``update_continuous_backups`` with
``UnsupportedOperationException``
(awslabs/amazon-dynamodb-local-samples#17); PITR stays enabled by default
on the real-AWS path.
"""

from __future__ import annotations

import os
import socket
import time
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from backend.persistence.dynamodb import keys
from backend.persistence.dynamodb.client import create_client
from backend.persistence.dynamodb.store import DynamoDBStore
from backend.persistence.dynamodb.table import GSI_DEFS, ensure_table
from backend.persistence.models import Episode, Feed

pytestmark = pytest.mark.slow

LOCAL_HOST = os.environ.get("DYNAMODB_LOCAL_HOST", "localhost")
LOCAL_PORT = int(os.environ.get("DYNAMODB_LOCAL_PORT", "8000"))
LOCAL_ENDPOINT = f"http://{LOCAL_HOST}:{LOCAL_PORT}"


_LOCAL_STATUS: bool | None = None  # cached emulator reachability


def _wait_for_local(timeout: float = 10.0) -> bool:
    """True once something accepts TCP on the DynamoDB Local port.

    Polls instead of probing once: DynamoDB Local takes a few seconds to
    warm up its JVM, and a single probe would race a slow-starting
    emulator and cause silent skips. The result is cached for the module
    so a missing emulator costs the timeout only once.
    """
    global _LOCAL_STATUS
    if _LOCAL_STATUS is not None:
        return _LOCAL_STATUS
    deadline = time.monotonic() + timeout
    while True:
        try:
            with socket.create_connection((LOCAL_HOST, LOCAL_PORT), timeout=1):
                _LOCAL_STATUS = True
                return True
        except OSError:
            if time.monotonic() >= deadline:
                _LOCAL_STATUS = False
                return False
            time.sleep(0.5)


@pytest.fixture()
async def local_store(monkeypatch):
    """A ``DynamoDBStore`` on a fresh table in DynamoDB Local.

    Uses the owned-client path (``endpoint_url=...``) so the real aioboto3
    client lifecycle is exercised, exactly as production does. Skips when
    the emulator is not running.
    """
    if not _wait_for_local():
        pytest.skip(
            f"DynamoDB Local not reachable at {LOCAL_ENDPOINT} "
            "(docker run -d -p 8000:8000 amazon/dynamodb-local)"
        )
    # botocore needs *some* credentials in the chain; Local ignores them.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    table_name = f"xin92-{uuid4().hex}"
    async with DynamoDBStore(
        endpoint_url=LOCAL_ENDPOINT, table_name=table_name
    ) as store:
        # Reach into the store for provisioning: ensure_table needs the
        # raw client, which only exists after the store is entered.
        # enable_pitr=False: DynamoDB Local rejects update_continuous_backups
        # (UnsupportedOperationException; awslabs/amazon-dynamodb-local-samples#17).
        created = await ensure_table(
            store._client, table_name=table_name, enable_pitr=False
        )
        assert created == "created"
        yield store
        try:
            await store._client.delete_table(TableName=table_name)
        except Exception:  # best-effort cleanup of the scratch table
            pass


def _feed() -> Feed:
    return Feed(
        rss_url=f"https://example.com/{uuid4().hex}.xml",
        title="Feed",
        sync_status="pending",
        created_at=datetime.now(timezone.utc),
    )


def _episode(feed_id: UUID, title: str, guid: str | None = None) -> Episode:
    return Episode(
        feed_id=feed_id,
        title=title,
        guid=guid,
        audio_url=f"https://example.com/{uuid4().hex}.mp3",
        created_at=datetime.now(timezone.utc),
    )


class TestEnsureTableOnLocal:
    async def test_table_created_with_gsis(self, local_store):
        desc = await local_store._client.describe_table(
            TableName=local_store.table_name
        )
        gsis = {
            g["IndexName"]: g
            for g in desc["Table"].get("GlobalSecondaryIndexes", [])
        }
        assert set(gsis) == {g["IndexName"] for g in GSI_DEFS}
        # Names alone aren't enough: assert key schemas and projections too.
        for expected in GSI_DEFS:
            actual = gsis[expected["IndexName"]]
            assert actual["KeySchema"] == expected["KeySchema"]
            assert (
                actual["Projection"]["ProjectionType"]
                == expected["Projection"]["ProjectionType"]
            )
        # Second call is a no-op.
        assert (
            await ensure_table(
                local_store._client,
                table_name=local_store.table_name,
                enable_pitr=False,
            )
            == "exists"
        )


class TestTransactionalGuidDedupOnLocal:
    async def test_duplicate_guid_rejected_by_real_transaction(
        self, local_store
    ):
        """The core XIN-91/XIN-92 proof: a real engine must cancel the
        transaction when the marker condition fails, so the duplicate
        episode is dropped AND the original row is untouched."""
        feed = await local_store.feeds.save(_feed())
        original = await local_store.episodes.save(
            _episode(feed.feed_id, "Original", guid="taken")
        )

        saved = await local_store.episodes.save_many(
            [_episode(feed.feed_id, "Impostor", guid="taken")]
        )

        assert saved == []
        kept = await local_store.episodes.get_by_id(original.episode_id)
        assert kept is not None
        assert kept.title == "Original"  # not overwritten by the impostor
        assert await local_store.episodes.list_guids_by_feed(feed.feed_id) == {
            "taken"
        }

    async def test_marker_condition_fails_directly_on_local(self, local_store):
        """Raw engine semantics: a ``Put`` of a guid marker with
        ``attribute_not_exists(pk)`` must raise
        ``TransactionCanceledException`` with a ``ConditionalCheckFailed``
        reason when the marker already exists. This is the exact condition
        moto is suspected of not enforcing faithfully."""
        feed = await local_store.feeds.save(_feed())
        await local_store.episodes.save(_episode(feed.feed_id, "Seed", guid="taken"))

        key_attrs = keys.guid_marker_keys(feed.feed_id, "taken")
        marker_item = {name: {"S": value} for name, value in key_attrs.items()}
        marker_item["probe"] = {"S": "duplicate-marker-write"}

        ctx = create_client(endpoint_url=LOCAL_ENDPOINT)
        client = await ctx.__aenter__()
        try:
            with pytest.raises(client.exceptions.TransactionCanceledException) as exc:
                await client.transact_write_items(
                    TransactItems=[
                        {
                            "Put": {
                                "TableName": local_store.table_name,
                                "Item": marker_item,
                                "ConditionExpression": "attribute_not_exists(pk)",
                            }
                        }
                    ]
                )
        finally:
            await ctx.__aexit__(None, None, None)

        reasons = exc.value.response.get("CancellationReasons", [])
        assert reasons and reasons[0]["Code"] == "ConditionalCheckFailed"
