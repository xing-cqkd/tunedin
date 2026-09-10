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

from backend.persistence.dynamodb import keys
from backend.persistence.dynamodb.repositories import (
    _save_many_op_chunks,
    _tag_claim_pk,
)
from backend.persistence.dynamodb.store import DynamoDBStore
from backend.persistence.dynamodb.table import ensure_table
from backend.persistence.dynamodb.testing import AsyncBoto3Client
from backend.persistence.models import CuratedPlaylist, Episode, Feed, User
from backend.persistence.repositories import MissingParentError, SlugConflictError


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
        claim_pk = _tag_claim_pk("race", None)
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
        claim_pk = _tag_claim_pk("stale", "topic")
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


class _RecordingClient:
    """Wraps the async test client, recording transact_write_items calls.

    Lets tests assert that a stale-claim/marker delete rode in the SAME
    transaction as the put (Linear: XIN-123), not just that the end
    state is right.
    """

    def __init__(self, client):
        self._client = client
        self.transact_calls: list = []
        self.exceptions = client.exceptions

    def __getattr__(self, name):
        target = getattr(self._client, name)
        if name == "transact_write_items":

            async def _record(*args, **kwargs):
                items = kwargs.get("TransactItems")
                if items is None and args:
                    items = args[0]
                self.transact_calls.append(items)
                return await target(*args, **kwargs)

            return _record
        return target


def _playlist(user_id) -> CuratedPlaylist:
    return CuratedPlaylist(
        user_id=user_id,
        title="Playlist",
        created_at=datetime.now(timezone.utc),
    )


async def _raw_item(client, table_name, pk, sk="META"):
    resp = await client.get_item(
        TableName=table_name, Key={"pk": {"S": pk}, "sk": {"S": sk}}
    )
    return resp.get("Item")


class TestPlaylistSlugClaimCleanup:
    """XIN-123 §1+§4: slug changes release the old claim transactionally."""

    async def test_slug_change_releases_old_slug(self, store):
        user = await store.users.save(User(email=f"{uuid4().hex}@example.com"))
        pl = _playlist(user.user_id)
        pl.slug = "slug-old"
        pl = await store.playlists.save(pl)

        pl.slug = "slug-new"
        await store.playlists.save(pl)

        # The old claim item is gone (not just shadowed).
        assert (
            await _raw_item(store._playlists._c, store.table_name, "SLUG#slug-old")
            is None
        )
        # Old slug resolves to nothing; new slug resolves to the playlist.
        assert await store.playlists.get_by_slug("slug-old") is None
        fetched = await store.playlists.get_by_slug("slug-new")
        assert fetched is not None and fetched.playlist_id == pl.playlist_id

        # The released slug is claimable again by a different playlist.
        other = _playlist(user.user_id)
        other.slug = "slug-old"
        other = await store.playlists.save(other)
        assert (await store.playlists.get_by_slug("slug-old")).playlist_id == (
            other.playlist_id
        )

    async def test_sequential_slug_conflict_still_raises(self, store):
        user = await store.users.save(User(email=f"{uuid4().hex}@example.com"))
        a = _playlist(user.user_id)
        a.slug = "taken"
        await store.playlists.save(a)

        b = _playlist(user.user_id)
        b.slug = "taken"
        with pytest.raises(SlugConflictError):
            await store.playlists.save(b)

    async def test_stale_claim_delete_in_same_transaction(self, store):
        user = await store.users.save(User(email=f"{uuid4().hex}@example.com"))
        rec = _RecordingClient(store._playlists._c)
        rec_store = DynamoDBStore(client=rec, table_name=store.table_name)

        pl = _playlist(user.user_id)
        pl.slug = "tx-old"
        pl = await rec_store.playlists.save(pl)
        rec.transact_calls.clear()

        pl.slug = "tx-new"
        await rec_store.playlists.save(pl)

        assert len(rec.transact_calls) == 1
        items = rec.transact_calls[0]
        puts = [op["Put"]["Item"] for op in items if "Put" in op]
        deletes = [op["Delete"]["Key"] for op in items if "Delete" in op]
        # Playlist put + new-claim put + stale-claim delete, one call.
        assert len(puts) == 2
        assert any(i["sk"] == {"S": "META"} and i["pk"] == {"S": "SLUG#tx-new"} for i in puts)
        assert {"pk": {"S": "SLUG#tx-old"}, "sk": {"S": "META"}} in deletes

    async def test_get_by_slug_rejects_skewed_claim(self, store):
        # XIN-123 §4 defense-in-depth: a claim that resolves to a playlist
        # whose slug differs (claim/row skew) returns None.
        user = await store.users.save(User(email=f"{uuid4().hex}@example.com"))
        pl = _playlist(user.user_id)
        pl.slug = "real-slug"
        pl = await store.playlists.save(pl)
        await store._playlists._c.put_item(
            TableName=store.table_name,
            Item={
                "pk": {"S": "SLUG#skewed"},
                "sk": {"S": "META"},
                "type": {"S": "slug_claim"},
                "playlist_id": {"S": str(pl.playlist_id)},
            },
        )
        assert await store.playlists.get_by_slug("skewed") is None
        assert await store.playlists.get_by_slug("real-slug") is not None

    async def test_get_by_slug_malformed_claim_returns_none(self, store):
        # XIN-124 §5: a claim item without playlist_id returns None
        # instead of raising KeyError.
        await store._playlists._c.put_item(
            TableName=store.table_name,
            Item={
                "pk": {"S": "SLUG#malformed"},
                "sk": {"S": "META"},
                "type": {"S": "slug_claim"},
            },
        )
        assert await store.playlists.get_by_slug("malformed") is None


class TestEpisodeGuidMarkerTransactions:
    """XIN-123 §2+§3: guid-marker cleanup rides in the transactions."""

    async def test_save_guid_change_deletes_stale_marker_in_transaction(
        self, store
    ):
        feed = await store.feeds.save(_feed())
        rec = _RecordingClient(store._episodes._c)
        rec_store = DynamoDBStore(client=rec, table_name=store.table_name)

        ep = _episode(feed.feed_id, "E", guid="g-a")
        ep = await rec_store.episodes.save(ep)
        rec.transact_calls.clear()

        ep.guid = "g-b"
        await rec_store.episodes.save(ep)

        assert len(rec.transact_calls) == 1
        items = rec.transact_calls[0]
        puts = [op["Put"]["Item"] for op in items if "Put" in op]
        deletes = [op["Delete"]["Key"] for op in items if "Delete" in op]
        # Episode put + new-marker put + stale-marker delete, one call.
        assert len(puts) == 2
        assert any(i["sk"] == {"S": "GUID#g-b"} for i in puts)
        assert {
            "pk": {"S": f"FEED#{feed.feed_id}"},
            "sk": {"S": "GUID#g-a"},
        } in deletes
        assert (
            await _raw_item(
                store._episodes._c, store.table_name, f"FEED#{feed.feed_id}", "GUID#g-a"
            )
            is None
        )
        assert await store.episodes.list_guids_by_feed(feed.feed_id) == {"g-b"}

    async def test_save_many_guid_change_releases_old_guid(self, store):
        feed = await store.feeds.save(_feed())
        ep = _episode(feed.feed_id, "E", guid="g-old")
        saved = await store.episodes.save_many([ep])
        assert [e.episode_id for e in saved] == [ep.episode_id]

        changed = Episode(
            feed_id=feed.feed_id,
            episode_id=ep.episode_id,
            title="E",
            guid="g-new",
            audio_url=ep.audio_url,
            created_at=ep.created_at,
        )
        resaved = await store.episodes.save_many([changed])
        assert [e.episode_id for e in resaved] == [ep.episode_id]

        # Old marker released in the same transaction; new marker owned.
        assert (
            await _raw_item(
                store._episodes._c,
                store.table_name,
                f"FEED#{feed.feed_id}",
                "GUID#g-old",
            )
            is None
        )
        marker = await _raw_item(
            store._episodes._c, store.table_name, f"FEED#{feed.feed_id}", "GUID#g-new"
        )
        assert marker is not None
        assert marker["episode_id"]["S"] == str(ep.episode_id)
        assert await store.episodes.list_guids_by_feed(feed.feed_id) == {"g-new"}

        # The released guid is dedup-clean: a fresh episode may claim it.
        fresh = _episode(feed.feed_id, "Fresh", guid="g-old")
        assert [e.episode_id for e in await store.episodes.save_many([fresh])] == [
            fresh.episode_id
        ]


class TestSaveManyMarkerId:
    """XIN-124 §1: markers must carry the real episode_id, not "None"."""

    async def test_markers_carry_assigned_ids(self, store):
        feed = await store.feeds.save(_feed())
        eps = [
            _episode(feed.feed_id, "A", guid="id-a"),
            _episode(feed.feed_id, "B", guid="id-b"),
        ]
        assert all(e.episode_id is None for e in eps)

        saved = await store.episodes.save_many(eps)

        assert [e.episode_id for e in saved] == [e.episode_id for e in eps]
        for ep, guid in zip(eps, ("id-a", "id-b")):
            marker = await _raw_item(
                store._episodes._c,
                store.table_name,
                f"FEED#{feed.feed_id}",
                f"GUID#{guid}",
            )
            assert marker is not None
            assert marker["episode_id"]["S"] == str(ep.episode_id) != "None"

    async def test_idempotent_rewrite_of_id_less_batch(self, store):
        # The real sync-pipeline path (ingestion/service.py builds
        # Episode(...) without episode_id): a retried batch must resolve
        # its own markers and re-write, not be dropped as duplicates.
        feed = await store.feeds.save(_feed())
        eps = [
            _episode(feed.feed_id, "A", guid="rw-a"),
            _episode(feed.feed_id, "B", guid="rw-b"),
        ]
        first = await store.episodes.save_many(eps)
        assert len(first) == 2

        second = await store.episodes.save_many(eps)
        assert [e.episode_id for e in second] == [e.episode_id for e in first]


class TestTagCaseSensitivity:
    """XIN-124 §2: tag claims hash the exact-case (name, category)."""

    async def test_name_case_variants_are_distinct_tags(self, store):
        foo = await store.tags.get_or_create("Foo", "topic")
        FOO = await store.tags.get_or_create("FOO", "topic")

        assert foo.tag_id != FOO.tag_id
        assert FOO.name == "FOO"
        assert (await store.tags.get_by_name_category("FOO")).tag_id == FOO.tag_id
        assert (await store.tags.get_by_name_category("Foo")).tag_id == foo.tag_id
        # No exact-case "foo" tag exists.
        assert await store.tags.get_by_name_category("foo") is None

    async def test_category_case_variants_are_distinct_tags(self, store):
        lower = await store.tags.get_or_create("x", "topic")
        upper = await store.tags.get_or_create("x", "Topic")

        assert lower.tag_id != upper.tag_id
        assert (await store.tags.get_by_name_category("x", "Topic")).tag_id == (
            upper.tag_id
        )


class TestNaturalKeyUniqueness:
    """XIN-124 §3: write-time uniqueness for Feed.rss_url and User.email."""

    async def test_duplicate_rss_url_raises(self, store):
        url = f"https://example.com/{uuid4().hex}.xml"
        first = await store.feeds.save(_feed_with_url(url))

        with pytest.raises(ValueError, match="already taken"):
            await store.feeds.save(_feed_with_url(url))

        # Idempotent re-save of the owning feed is fine.
        first.title = "Renamed"
        await store.feeds.save(first)
        assert (await store.feeds.get_by_rss_url(url)).feed_id == first.feed_id

        # Changing the URL releases the old claim in the same transaction.
        first.rss_url = f"https://example.com/{uuid4().hex}.xml"
        await store.feeds.save(first)
        assert await store.feeds.get_by_rss_url(url) is None
        second = await store.feeds.save(_feed_with_url(url))
        assert (await store.feeds.get_by_rss_url(url)).feed_id == second.feed_id

    async def test_duplicate_email_raises(self, store):
        email = f"{uuid4().hex}@example.com"
        first = await store.users.save(User(email=email))

        with pytest.raises(ValueError, match="already taken"):
            await store.users.save(User(email=email))

        # Idempotent re-save of the owning user is fine.
        await store.users.save(first)
        assert (await store.users.get_by_email(email)).user_id == first.user_id

        # Changing the email releases the old claim in the same transaction.
        first.email = f"{uuid4().hex}@example.com"
        await store.users.save(first)
        assert await store.users.get_by_email(email) is None
        second = await store.users.save(User(email=email))
        assert (await store.users.get_by_email(email)).user_id == second.user_id


def _feed_with_url(url: str) -> Feed:
    return Feed(
        rss_url=url,
        title="Feed",
        sync_status="pending",
        created_at=datetime.now(timezone.utc),
    )


class TestAddEpisodeFkParity:
    """XIN-124 — Chester's call: enforce FK parity across backends.

    ``add_episode`` with a nonexistent playlist or episode raises
    ``MissingParentError`` on ALL backends (SQLAlchemy maps the FK
    ``IntegrityError`` — covered in
    ``backend/persistence/tests/test_sqlalchemy_store.py``; DynamoDB
    checks parent existence before writing), so callers catch one type
    regardless of backend. No orphaned link is created on either backend.
    """

    async def test_dynamodb_add_episode_without_parents_raises(self, store):
        missing_pl, missing_ep = uuid4(), uuid4()
        with pytest.raises(MissingParentError):
            await store.playlists.add_episode(missing_pl, missing_ep, 0)
        # No orphaned link was created.
        assert (
            await _raw_item(
                store._playlists._c,
                store.table_name,
                f"PL#{missing_pl}",
                f"PLEP#{missing_ep}",
            )
            is None
        )

    async def test_dynamodb_add_episode_missing_episode_only_raises(
        self, store
    ):
        user = await store.users.save(User(email=f"{uuid4().hex}@x.com"))
        pl = await store.playlists.save(
            CuratedPlaylist(user_id=user.user_id, title="P")
        )
        with pytest.raises(MissingParentError):
            await store.playlists.add_episode(pl.playlist_id, uuid4(), 0)

    async def test_dynamodb_add_episode_missing_playlist_only_raises(
        self, store
    ):
        feed = await store.feeds.save(_feed())
        ep = await store.episodes.save(
            Episode(
                feed_id=feed.feed_id,
                title="E",
                audio_url="https://example.com/e.mp3",
            )
        )
        with pytest.raises(MissingParentError):
            await store.playlists.add_episode(uuid4(), ep.episode_id, 0)


class TestSaveManyOpChunks:
    """XIN-123: save_many chunks are bounded by transact-item budget.

    A guid episode costs 2 transact items (marker put + episode put),
    3 when its guid changed (stale-marker delete). Fixed 50-episode
    chunks could reach 150 items — over DynamoDB's 100-item limit.
    """

    def _candidates(self, feed_id, specs):
        out = []
        old_guids = {}
        for i, (guid, prev_guid) in enumerate(specs):
            ep = _episode(feed_id, f"E{i}", guid=guid)
            ep.episode_id = uuid4()
            out.append((i, ep))
            if prev_guid is not None:
                old_guids[(str(feed_id), str(ep.episode_id))] = prev_guid
        return out, old_guids

    def test_guid_changing_episodes_cost_three_ops(self):
        # 40 guid-changing episodes = 120 ops -> must split into 2 chunks.
        feed_id = uuid4()
        candidates, old_guids = self._candidates(
            feed_id, [(f"new-{i}", f"old-{i}") for i in range(40)]
        )
        chunks = list(_save_many_op_chunks(candidates, old_guids))
        assert len(chunks) == 2
        assert [len(c) for c in chunks] == [33, 7]
        assert sum(len(c) for c in chunks) == 40
        # No chunk exceeds the 100-item transaction limit.
        for chunk in chunks:
            assert sum(3 for _ in chunk) <= 100

    def test_guid_episodes_without_change_cost_two_ops(self):
        # 50 stable-guid episodes = 100 ops -> exactly one chunk.
        feed_id = uuid4()
        candidates, old_guids = self._candidates(
            feed_id, [(f"g-{i}", None) for i in range(50)]
        )
        chunks = list(_save_many_op_chunks(candidates, old_guids))
        assert len(chunks) == 1
        assert len(chunks[0]) == 50

    def test_null_guid_episodes_cost_one_op(self):
        # 60 guid-less episodes = 60 ops -> one chunk.
        feed_id = uuid4()
        candidates, old_guids = self._candidates(
            feed_id, [(None, None) for _ in range(60)]
        )
        chunks = list(_save_many_op_chunks(candidates, old_guids))
        assert len(chunks) == 1
        assert len(chunks[0]) == 60

    def test_mixed_costs_pack_greedily(self):
        # 30 changing (3 ops) + 30 guid-less (1 op) = 120 ops -> 2 chunks,
        # and input order is preserved across the split.
        feed_id = uuid4()
        specs = [(f"new-{i}", f"old-{i}") for i in range(30)]
        specs += [(None, None) for _ in range(30)]
        candidates, old_guids = self._candidates(feed_id, specs)
        chunks = list(_save_many_op_chunks(candidates, old_guids))
        assert len(chunks) == 2
        # First chunk: 30 changing (90 ops) + 10 guid-less (10 ops) = 100.
        assert [len(c) for c in chunks] == [40, 20]
        order = [i for chunk in chunks for i, _ in chunk]
        assert order == list(range(60))

    def test_empty_candidates(self):
        assert list(_save_many_op_chunks([], {})) == []
