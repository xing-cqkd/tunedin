"""Focused tests for DynamoDB repository edge cases (Linear: XIN-90).

Covers the transactional guid-dedup path in ``EpisodeRepository.save_many``
— the correctness-critical write the conformance suite only exercises for
order preservation — plus guid-union reads and the tag claim path.
"""

from datetime import datetime, timezone
from uuid import uuid4

import boto3
import pytest
from moto import mock_aws

from backend.persistence.dynamodb.store import DynamoDBStore
from backend.persistence.dynamodb.table import ensure_table
from backend.persistence.dynamodb.testing import AsyncBoto3Client
from backend.persistence.models import Episode, Feed


@pytest.fixture()
async def store():
    from backend.persistence.dynamodb.table import DEFAULT_TABLE_NAME

    table_name = f"xin90-{uuid4().hex}"
    with mock_aws():
        sync = boto3.client(
            "dynamodb",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )
        client = AsyncBoto3Client(sync)
        await ensure_table(client, table_name=table_name)
        s = DynamoDBStore(client=client, table_name=table_name)
        yield s
        await s.close()


def _feed() -> Feed:
    return Feed(
        rss_url=f"https://example.com/{uuid4().hex}.xml",
        title="Feed",
        sync_status="pending",
        created_at=datetime.now(timezone.utc),
    )


def _episode(feed_id, title, guid=None, processed=False):
    return Episode(
        feed_id=feed_id,
        title=title,
        guid=guid,
        audio_url=f"https://example.com/{uuid4().hex}.mp3",
        processed=processed,
        created_at=datetime.now(timezone.utc),
    )


class TestSaveManyDedup:
    async def test_duplicate_guid_within_batch_drops_later_copy(self, store):
        feed = await store.feeds.save(_feed())
        first = _episode(feed.feed_id, "First", guid="dup-1")
        second = _episode(feed.feed_id, "Second", guid="dup-1")

        saved = await store.episodes.save_many([first, second])

        assert [e.episode_id for e in saved] == [first.episode_id]
        assert await store.episodes.get_by_id(first.episode_id) is not None
        assert await store.episodes.get_by_id(second.episode_id) is None
        assert await store.episodes.list_guids_by_feed(feed.feed_id) == {"dup-1"}

    async def test_duplicate_guid_across_calls_is_dropped(self, store):
        feed = await store.feeds.save(_feed())
        original = _episode(feed.feed_id, "Original", guid="dup-2")
        await store.episodes.save(original)

        newcomer = _episode(feed.feed_id, "Newcomer", guid="dup-2")
        saved = await store.episodes.save_many([newcomer])

        assert saved == []
        assert await store.episodes.get_by_id(newcomer.episode_id) is None
        # The original is untouched.
        fetched = await store.episodes.get_by_id(original.episode_id)
        assert fetched is not None and fetched.title == "Original"

    async def test_null_guid_episodes_all_persist_without_markers(self, store):
        feed = await store.feeds.save(_feed())
        eps = [_episode(feed.feed_id, f"E{i}", guid=None) for i in range(3)]

        saved = await store.episodes.save_many(eps)

        assert [e.episode_id for e in saved] == [e.episode_id for e in eps]
        assert await store.episodes.list_guids_by_feed(feed.feed_id) == set()
        assert await store.episodes.count_all() == 3

    async def test_mixed_batch_reports_only_persisted_in_input_order(self, store):
        feed = await store.feeds.save(_feed())
        await store.episodes.save(_episode(feed.feed_id, "Seed", guid="taken"))
        a = _episode(feed.feed_id, "A", guid="g-a")
        b = _episode(feed.feed_id, "B", guid="taken")  # conflicts with seed
        c = _episode(feed.feed_id, "C", guid=None)
        d = _episode(feed.feed_id, "D", guid="g-a")  # within-batch dup of A

        saved = await store.episodes.save_many([a, b, c, d])

        assert [e.episode_id for e in saved] == [
            a.episode_id,
            c.episode_id,
        ]

    async def test_save_many_empty_list(self, store):
        assert await store.episodes.save_many([]) == []


class TestGuidUnionReads:
    async def test_list_guids_unions_save_and_save_many(self, store):
        feed = await store.feeds.save(_feed())
        await store.episodes.save(_episode(feed.feed_id, "ViaSave", guid="s-1"))
        await store.episodes.save_many(
            [_episode(feed.feed_id, "ViaSaveMany", guid="m-1")]
        )
        assert await store.episodes.list_guids_by_feed(feed.feed_id) == {
            "s-1",
            "m-1",
        }


class TestTagClaim:
    async def test_get_or_create_sequential_idempotent(self, store):
        a = await store.tags.get_or_create("python", "topic")
        b = await store.tags.get_or_create("python", "topic")
        assert a.tag_id == b.tag_id

    async def test_claim_loser_reads_winner(self, store):
        # Simulate the race loser path: a claim item exists without the tag
        # row yet (winner between its two writes). get_or_create must wait
        # for and return the winner's tag rather than creating a duplicate.
        from backend.persistence.dynamodb import keys

        tag_id = uuid4()
        claim_pk = keys.tag_keys(tag_id, name="race", category=None)[
            "gsi1pk"
        ].replace("TAGNAME#", "TAGCLAIM#", 1)
        await store._tags._c.put_item(
            TableName=store.table_name,
            Item={
                "pk": {"S": claim_pk},
                "sk": {"S": "META"},
                "type": {"S": "tag_claim"},
                "tag_id": {"S": str(tag_id)},
            },
        )
        # Winner's tag put lands just after the loser starts waiting.
        async def _winner_put():
            import asyncio

            await asyncio.sleep(0.05)
            from backend.persistence import models
            from backend.persistence.dynamodb import codec

            tag = models.Tag(tag_id=tag_id, name="race", category=None)
            item = codec.model_to_item(
                tag,
                keys.tag_keys(tag_id, name="race", category=None),
                codec.TYPE_TAG,
            )
            await store._tags._c.put_item(
                TableName=store.table_name, Item=item
            )

        import asyncio

        winner = asyncio.ensure_future(_winner_put())
        try:
            tag = await store.tags.get_or_create("race", None)
        finally:
            await winner
        assert tag.tag_id == tag_id

    async def test_stale_claim_recovered(self, store):
        # Simulate a winner crash: a claim item exists whose tag will
        # never materialize. get_or_create must delete the stale claim
        # and recover (once, bounded) instead of raising forever.
        from backend.persistence.dynamodb import keys

        dead_tag_id = uuid4()
        claim_pk = keys.tag_keys(dead_tag_id, name="stale", category="topic")[
            "gsi1pk"
        ].replace("TAGNAME#", "TAGCLAIM#", 1)
        await store._tags._c.put_item(
            TableName=store.table_name,
            Item={
                "pk": {"S": claim_pk},
                "sk": {"S": "META"},
                "type": {"S": "tag_claim"},
                "tag_id": {"S": str(dead_tag_id)},
            },
        )
        tag = await store.tags.get_or_create("stale", "topic")
        assert tag.name == "stale"
        assert tag.category == "topic"
        # Recovered with a fresh tag — not the dead claim's tag_id.
        assert tag.tag_id != dead_tag_id
        # The recovered state is stable: the natural key now resolves to
        # the real tag.
        again = await store.tags.get_or_create("stale", "topic")
        assert again.tag_id == tag.tag_id
