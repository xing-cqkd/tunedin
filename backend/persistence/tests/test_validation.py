"""Backend-agnostic item-size guard tests (Linear: XIN-95).

DynamoDB rejects items over 400 KiB. The guard lives in the shared
:mod:`backend.persistence.validation` module so that every backend raises
the *same* error type — these tests assert exactly that, against a real
SQLite store and a moto-backed DynamoDB store. No test touches real AWS.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone

import boto3
import pytest
import pytest_asyncio
from moto import mock_aws
from sqlalchemy.ext.asyncio import create_async_engine

from backend.persistence import validation
from backend.persistence.dynamodb.store import DynamoDBStore
from backend.persistence.dynamodb.table import ensure_table
from backend.persistence.dynamodb.testing import AsyncBoto3Client
from backend.persistence.models import Base, Episode, Feed
from backend.persistence.sqlalchemy_store import SQLAlchemyStore
from backend.persistence.validation import ItemTooLargeError, MAX_ITEM_BYTES

NOW = datetime.now(timezone.utc)
BIG_TEXT = "x" * (500 * 1024)  # 500 KiB — over the 400 KiB limit


# ---------------------------------------------------------------------------
# Unit tests for the shared validator
# ---------------------------------------------------------------------------


def test_under_limit_passes():
    validation.check_item_size({"a": "b" * 1000}, what="small")


def test_over_limit_raises_item_too_large():
    with pytest.raises(ItemTooLargeError, match="exceeds"):
        validation.check_item_size({"blob": "x" * (MAX_ITEM_BYTES + 1)}, what="big")


def test_item_too_large_is_a_value_error():
    # Callers can catch either the specific type or ValueError.
    assert issubclass(ItemTooLargeError, ValueError)


def test_entity_fields_extracts_columns():
    feed = Feed(rss_url="https://example.com/f.xml", title="T")
    fields = validation.entity_fields(feed)
    assert fields["rss_url"] == "https://example.com/f.xml"
    assert fields["title"] == "T"
    assert "_sa_instance_state" not in fields


# ---------------------------------------------------------------------------
# Fixtures: one SQL store, one moto-backed DynamoDB store
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def sql_store(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path}/guard_test.db"
    engine = create_async_engine(url, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()
    s = SQLAlchemyStore.from_url(url)
    yield s
    await s.rollback()
    await s.close()


@pytest.fixture()
def ddb_store():
    with mock_aws():
        sync = boto3.client(
            "dynamodb",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )
        client = AsyncBoto3Client(sync)

        async def _setup():
            await ensure_table(client, table_name="guard-test")

        asyncio.run(_setup())
        yield DynamoDBStore(client=client, table_name="guard-test")


async def _feed_and_episode_kwargs(store, big: bool):
    feed = await store.feeds.save(
        Feed(
            rss_url=f"https://example.com/{uuid.uuid4().hex}.xml",
            title="Feed",
            sync_status="active",
        )
    )
    await store.commit()
    return {
        "feed_id": feed.feed_id,
        "guid": uuid.uuid4().hex,
        "title": "Episode",
        "audio_url": "https://example.com/audio.mp3",
        "transcript": BIG_TEXT if big else "short transcript",
    }


# ---------------------------------------------------------------------------
# Backend-agnostic behavior: same error type on both backends
# ---------------------------------------------------------------------------


async def test_sql_rejects_oversized_episode(sql_store):
    kwargs = await _feed_and_episode_kwargs(sql_store, big=True)
    with pytest.raises(ItemTooLargeError):
        await sql_store.episodes.save(Episode(**kwargs))


async def test_dynamodb_rejects_oversized_episode(ddb_store):
    kwargs = await _feed_and_episode_kwargs(ddb_store, big=True)
    with pytest.raises(ItemTooLargeError):
        await ddb_store.episodes.save(Episode(**kwargs))


async def test_both_backends_raise_identical_error_type(sql_store, ddb_store):
    """The core XIN-95 acceptance: the error type does not depend on backend."""
    sql_kwargs = await _feed_and_episode_kwargs(sql_store, big=True)
    ddb_kwargs = await _feed_and_episode_kwargs(ddb_store, big=True)
    with pytest.raises(ItemTooLargeError) as sql_exc:
        await sql_store.episodes.save(Episode(**sql_kwargs))
    with pytest.raises(ItemTooLargeError) as ddb_exc:
        await ddb_store.episodes.save(Episode(**ddb_kwargs))
    assert type(sql_exc.value) is type(ddb_exc.value) is ItemTooLargeError


async def test_normal_sized_episode_saves_on_both(sql_store, ddb_store):
    """The guard must not reject ordinary writes."""
    sql_kwargs = await _feed_and_episode_kwargs(sql_store, big=False)
    saved_sql = await sql_store.episodes.save(Episode(**sql_kwargs))
    assert saved_sql.episode_id is not None

    ddb_kwargs = await _feed_and_episode_kwargs(ddb_store, big=False)
    saved_ddb = await ddb_store.episodes.save(Episode(**ddb_kwargs))
    assert saved_ddb.episode_id is not None
