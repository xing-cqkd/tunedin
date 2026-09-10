"""Repository conformance suite (XIN-88).

A SINGLE test class exercising EVERY method of the repository ABC protocol
defined in :mod:`backend.persistence.repositories`. The suite is
backend-agnostic: tests only use the ``Store``/repository ABC interface and
the model classes as attribute bags — never SQLAlchemy internals or SQL
expressions.

Backend parametrization
-----------------------
``_BACKENDS`` maps a backend name to a backend-context class exposing
``setup()`` / ``new_store()`` / ``teardown()``. The ``store`` fixture yields
a fresh, isolated store per test. Today only ``"sqlalchemy"`` (an in-memory
SQLite ``SQLAlchemyStore``) is registered; the DynamoDB task adds a second
entry — no test changes needed.

Ordering discipline
-------------------
Per the protocol docstrings, rows with equal sort keys may come back in ANY
order on ALL backends. Tie-sensitive tests therefore assert set membership
(or the relative order of non-tied rows) and never an exact sequence across
tied values.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import re

import boto3
import pytest
from moto import mock_aws
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from backend.persistence.models import (
    Base,
    CuratedPlaylist,
    Episode,
    Feed,
    Insight,
    Tag,
    TaskLog,
    User,
    UserEpisodeProgress,
)
from backend.persistence.repositories import SlugConflictError, Store
from backend.persistence.sqlalchemy_store import SQLAlchemyStore
from backend.persistence.dynamodb.store import DynamoDBStore


# ---------------------------------------------------------------------------
# Backend contexts: the seam a future DynamoDB task plugs into.
# ---------------------------------------------------------------------------


class _SqliteBackend:
    """In-memory SQLite backend context for ``SQLAlchemyStore``."""

    async def setup(self) -> None:
        self._engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    def new_store(self) -> Store:
        factory = async_sessionmaker(
            bind=self._engine, class_=AsyncSession, expire_on_commit=False
        )
        return SQLAlchemyStore(factory)

    async def teardown(self) -> None:
        await self._engine.dispose()


class _DynamoDBBackend:
    """Moto-backed DynamoDB backend context for ``DynamoDBStore``.

    moto 5.2.3 cannot intercept aioboto3's async HTTP layer, so the store
    is given the test-only async adapter over a sync boto3 client
    (``backend.persistence.dynamodb.testing``) — production code stays on
    aioboto3. Each test gets a freshly provisioned table for isolation.
    """

    async def setup(self) -> None:
        from backend.persistence.dynamodb.table import ensure_table
        from backend.persistence.dynamodb.testing import AsyncBoto3Client

        self._mock = mock_aws()
        self._mock.start()
        sync = boto3.client(
            "dynamodb",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )
        self._client = AsyncBoto3Client(sync)
        self._table_name = f"conformance-{uuid4().hex}"
        await ensure_table(self._client, table_name=self._table_name)

    def new_store(self) -> Store:
        return DynamoDBStore(client=self._client, table_name=self._table_name)

    async def teardown(self) -> None:
        await self._client.close()
        self._mock.stop()


_BACKENDS = {"sqlalchemy": _SqliteBackend, "dynamodb": _DynamoDBBackend}


@pytest.fixture(params=sorted(_BACKENDS))
async def backend(request):
    ctx = _BACKENDS[request.param]()
    await ctx.setup()
    yield ctx
    await ctx.teardown()


@pytest.fixture
async def store(backend):
    s = backend.new_store()
    yield s
    await s.close()


# ---------------------------------------------------------------------------
# Entity builders (model classes as attribute bags, per the protocol).
# ---------------------------------------------------------------------------

_T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _feed(rss_url: str, title: str = "Feed", **kw) -> Feed:
    kw.setdefault("sync_status", "pending")
    kw.setdefault("created_at", datetime.now(timezone.utc))
    return Feed(rss_url=rss_url, title=title, **kw)


def _episode(feed_id: UUID, title: str = "Episode", **kw) -> Episode:
    kw.setdefault("audio_url", f"https://example.com/{uuid4().hex}.mp3")
    return Episode(feed_id=feed_id, title=title, **kw)


def _insight(episode_id: UUID, title: str = "Insight", **kw) -> Insight:
    kw.setdefault("created_at", datetime.now(timezone.utc))
    return Insight(episode_id=episode_id, title=title, **kw)


def _wall(dt: datetime) -> datetime:
    """Wall-clock time of a datetime, ignoring tz-awareness.

    SQLite returns naive datetimes; DynamoDB returns tz-aware ISO-8601.
    The protocol-level guarantee is that the instant is preserved.
    """
    return dt.replace(tzinfo=None)


class TestRepositoryConformance:
    # ------------------------------------------------------------------
    # Feed repository
    # ------------------------------------------------------------------

    async def _seed_feed(self, store: Store, url: str | None = None) -> Feed:
        return await store.feeds.save(_feed(url or f"https://e.com/{uuid4().hex}.xml"))

    async def test_feed_save_get_by_id_round_trip(self, store: Store):
        feed = _feed("https://example.com/a.xml", "Alpha", author="Ann")
        saved = await store.feeds.save(feed)
        assert saved.feed_id is not None

        fetched = await store.feeds.get_by_id(saved.feed_id)
        assert fetched is not None
        assert fetched.rss_url == "https://example.com/a.xml"
        assert fetched.title == "Alpha"
        assert fetched.author == "Ann"

        assert await store.feeds.get_by_id(uuid4()) is None

    async def test_feed_get_by_rss_url(self, store: Store):
        await store.feeds.save(_feed("https://example.com/b.xml", "Beta"))
        fetched = await store.feeds.get_by_rss_url("https://example.com/b.xml")
        assert fetched is not None
        assert fetched.title == "Beta"
        assert await store.feeds.get_by_rss_url("https://nope.invalid/x") is None

    async def test_feed_list_by_statuses_order_and_limit(self, store: Store):
        t1, t2, t3 = _T0, _T0 + timedelta(hours=1), _T0 + timedelta(hours=2)
        p1 = await store.feeds.save(_feed("https://e.com/p1.xml", "P1", created_at=t1))
        p2 = await store.feeds.save(_feed("https://e.com/p2.xml", "P2", created_at=t2))
        e1 = await store.feeds.save(
            _feed("https://e.com/e1.xml", "E1", sync_status="error", created_at=t3)
        )
        await store.feeds.save(
            _feed("https://e.com/s1.xml", "S1", sync_status="success")
        )

        rows = await store.feeds.list_by_statuses(["pending", "error"])
        assert [f.feed_id for f in rows] == [p1.feed_id, p2.feed_id, e1.feed_id]

        rows = await store.feeds.list_by_statuses(["pending", "error"], limit=2)
        assert [f.feed_id for f in rows] == [p1.feed_id, p2.feed_id]

        pending = await store.feeds.list_by_statuses(["pending"])
        assert [f.feed_id for f in pending] == [p1.feed_id, p2.feed_id]

    async def test_feed_list_by_statuses_empty_list(self, store: Store):
        await store.feeds.save(_feed("https://e.com/x.xml"))
        assert await store.feeds.list_by_statuses([]) == []

    async def test_feed_list_by_statuses_ties_are_unordered(self, store: Store):
        tie = _T0 + timedelta(days=9)
        a = await store.feeds.save(_feed("https://e.com/ta.xml", "TA", created_at=tie))
        b = await store.feeds.save(_feed("https://e.com/tb.xml", "TB", created_at=tie))
        rows = await store.feeds.list_by_statuses(["pending"])
        # Weak assertion: same created_at may come back in any order.
        assert {f.feed_id for f in rows} == {a.feed_id, b.feed_id}

    async def test_feed_list_error_due_retry(self, store: Store):
        cutoff = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        due = await store.feeds.save(
            _feed(
                "https://e.com/due.xml",
                "Due",
                sync_status="error",
                error_count=1,
                last_fetched_at=cutoff - timedelta(days=2),
                created_at=_T0,
            )
        )
        # error_count exhausted -> not due
        await store.feeds.save(
            _feed(
                "https://e.com/max.xml",
                "Max",
                sync_status="error",
                error_count=3,
                last_fetched_at=cutoff - timedelta(days=10),
            )
        )
        # fetched too recently -> not due
        await store.feeds.save(
            _feed(
                "https://e.com/fresh.xml",
                "Fresh",
                sync_status="error",
                error_count=1,
                last_fetched_at=cutoff + timedelta(hours=1),
            )
        )
        # wrong status -> not due
        await store.feeds.save(
            _feed(
                "https://e.com/ok.xml",
                "Ok",
                sync_status="success",
                error_count=9,
                last_fetched_at=cutoff - timedelta(days=30),
            )
        )
        # NULL last_fetched_at error row -> due (XIN-128: must not be stranded)
        null_ts = await store.feeds.save(
            _feed(
                "https://e.com/nullts.xml",
                "NullTs",
                sync_status="error",
                error_count=1,
                last_fetched_at=None,
                created_at=_T0 + timedelta(hours=1),
            )
        )

        rows = await store.feeds.list_error_due_retry(cutoff, max_attempts=3)
        assert [f.feed_id for f in rows] == [due.feed_id, null_ts.feed_id]

    async def test_feed_counts(self, store: Store):
        assert await store.feeds.count_all() == 0
        assert await store.feeds.count_by_status("pending") == 0
        assert await store.feeds.count_by_statuses(["pending", "error"]) == 0
        assert await store.feeds.count_by_statuses([]) == 0

        await store.feeds.save(_feed("https://e.com/c1.xml"))
        await store.feeds.save(_feed("https://e.com/c2.xml"))
        await store.feeds.save(
            _feed("https://e.com/c3.xml", sync_status="error")
        )

        assert await store.feeds.count_all() == 3
        assert await store.feeds.count_by_status("pending") == 2
        assert await store.feeds.count_by_status("error") == 1
        assert await store.feeds.count_by_statuses(["pending", "error"]) == 3
        assert await store.feeds.count_by_statuses(["success"]) == 0

    async def test_feed_list_all_order_and_limit(self, store: Store):
        t1, t2 = _T0, _T0 + timedelta(hours=1)
        f1 = await store.feeds.save(_feed("https://e.com/l1.xml", created_at=t2))
        f2 = await store.feeds.save(_feed("https://e.com/l2.xml", created_at=t1))
        rows = await store.feeds.list_all()
        assert [f.feed_id for f in rows] == [f2.feed_id, f1.feed_id]
        rows = await store.feeds.list_all(limit=1)
        assert [f.feed_id for f in rows] == [f2.feed_id]

    async def test_feed_empty_string_round_trip(self, store: Store):
        saved = await store.feeds.save(
            _feed("https://e.com/empty.xml", author="", description="")
        )
        fetched = await store.feeds.get_by_id(saved.feed_id)
        assert fetched is not None
        # DynamoDB rejects "" and maps it to omitted/None on read; SQL
        # preserves "". The protocol-level guarantee is no corruption.
        assert fetched.author in ("", None)
        assert fetched.description in ("", None)

    async def test_feed_naive_datetime_round_trip(self, store: Store):
        naive = datetime(2026, 3, 4, 5, 6, 7)  # no tzinfo
        saved = await store.feeds.save(
            _feed("https://e.com/naive.xml", last_fetched_at=naive)
        )
        fetched = await store.feeds.get_by_id(saved.feed_id)
        assert fetched is not None
        assert fetched.last_fetched_at is not None
        # The instant must survive regardless of tz-awareness on read.
        assert _wall(fetched.last_fetched_at) == _wall(naive)

    # ------------------------------------------------------------------
    # Episode repository
    # ------------------------------------------------------------------

    async def test_episode_save_get_by_id_round_trip(self, store: Store):
        feed = await self._seed_feed(store)
        ep = _episode(feed.feed_id, "Hello", guid="g-1")
        saved = await store.episodes.save(ep)
        assert saved.episode_id is not None

        fetched = await store.episodes.get_by_id(saved.episode_id)
        assert fetched is not None
        assert fetched.title == "Hello"
        assert fetched.guid == "g-1"
        assert fetched.feed_id == feed.feed_id

        assert await store.episodes.get_by_id(uuid4()) is None

    async def test_episode_list_guids_by_feed(self, store: Store):
        feed = await self._seed_feed(store)
        other = await self._seed_feed(store)
        await store.episodes.save(_episode(feed.feed_id, "A", guid="g1"))
        await store.episodes.save(_episode(feed.feed_id, "B", guid="g2"))
        await store.episodes.save(_episode(feed.feed_id, "C", guid=None))
        # Same guid in a different feed is a different dedup scope.
        await store.episodes.save(_episode(other.feed_id, "D", guid="g1"))

        guids = await store.episodes.list_guids_by_feed(feed.feed_id)
        assert guids == {"g1", "g2"}
        assert await store.episodes.list_guids_by_feed(other.feed_id) == {"g1"}
        assert await store.episodes.list_guids_by_feed(uuid4()) == set()

    async def test_episode_list_episodes_by_feed_ordering(self, store: Store):
        feed = await self._seed_feed(store)
        other = await self._seed_feed(store)
        t1 = datetime(2026, 2, 1, tzinfo=timezone.utc)
        t2 = datetime(2026, 2, 2, tzinfo=timezone.utc)
        t3 = datetime(2026, 2, 3, tzinfo=timezone.utc)
        e_old = await store.episodes.save(_episode(feed.feed_id, "Old", published_at=t1))
        e_new = await store.episodes.save(_episode(feed.feed_id, "New", published_at=t3))
        e_mid = await store.episodes.save(_episode(feed.feed_id, "Mid", published_at=t2))
        e_nodate = await store.episodes.save(
            _episode(feed.feed_id, "NoDate", published_at=None)
        )
        await store.episodes.save(_episode(other.feed_id, "Other", published_at=t3))

        rows = await store.episodes.list_episodes_by_feed(feed.feed_id)
        assert [e.episode_id for e in rows] == [
            e_new.episode_id,
            e_mid.episode_id,
            e_old.episode_id,
            e_nodate.episode_id,
        ]

    async def test_episode_list_episodes_by_feed_ties_unordered(self, store: Store):
        feed = await self._seed_feed(store)
        t = datetime(2026, 2, 5, tzinfo=timezone.utc)
        a = await store.episodes.save(_episode(feed.feed_id, "A", published_at=t))
        b = await store.episodes.save(_episode(feed.feed_id, "B", published_at=t))
        rows = await store.episodes.list_episodes_by_feed(feed.feed_id)
        assert {e.episode_id for e in rows} == {a.episode_id, b.episode_id}

    async def test_episode_list_unprocessed_order_and_limit(self, store: Store):
        feed = await self._seed_feed(store)
        t1 = datetime(2026, 2, 1, tzinfo=timezone.utc)
        t2 = datetime(2026, 2, 2, tzinfo=timezone.utc)
        e1 = await store.episodes.save(
            _episode(feed.feed_id, "E1", published_at=t1, processed=False)
        )
        e2 = await store.episodes.save(
            _episode(feed.feed_id, "E2", published_at=t2, processed=False)
        )
        await store.episodes.save(
            _episode(feed.feed_id, "Done", published_at=t2, processed=True)
        )
        e_null = await store.episodes.save(
            _episode(feed.feed_id, "NoDate", published_at=None, processed=False)
        )

        rows = await store.episodes.list_unprocessed()
        assert [e.episode_id for e in rows] == [
            e2.episode_id,
            e1.episode_id,
            e_null.episode_id,
        ]

        rows = await store.episodes.list_unprocessed(limit=2)
        assert [e.episode_id for e in rows] == [e2.episode_id, e1.episode_id]

    async def test_episode_list_unprocessed_feed_filter_pagination_contract(
        self, store: Store
    ):
        # More unprocessed episodes per feed than the requested limit, spread
        # across feeds. The limit is a hard cap on MATCHING rows: a backend
        # that applies Limit before its feed filter would silently
        # under-return here (the DynamoDB Limit-before-FilterExpression trap).
        feed_a = await self._seed_feed(store)
        feed_b = await self._seed_feed(store)
        base = datetime(2026, 3, 1, tzinfo=timezone.utc)
        for i in range(8):
            await store.episodes.save(
                _episode(
                    feed_a.feed_id,
                    f"A{i}",
                    guid=f"a-{i}",
                    published_at=base + timedelta(hours=i),
                    processed=False,
                )
            )
            await store.episodes.save(
                _episode(
                    feed_b.feed_id,
                    f"B{i}",
                    guid=f"b-{i}",
                    published_at=base + timedelta(hours=i),
                    processed=False,
                )
            )

        rows = await store.episodes.list_unprocessed(feed_id=feed_a.feed_id, limit=5)
        assert len(rows) == 5
        assert all(e.feed_id == feed_a.feed_id for e in rows)
        assert all(e.processed is False for e in rows)

        rows = await store.episodes.list_unprocessed(feed_id=feed_b.feed_id, limit=20)
        assert len(rows) == 8  # fewer matches than the limit: return them all

    async def test_episode_list_unprocessed_default_limit(self, store: Store):
        feed = await self._seed_feed(store)
        base = datetime(2026, 3, 1, tzinfo=timezone.utc)
        for i in range(60):
            await store.episodes.save(
                _episode(
                    feed.feed_id,
                    f"E{i}",
                    guid=f"d-{i}",
                    published_at=base + timedelta(minutes=i),
                    processed=False,
                )
            )
        rows = await store.episodes.list_unprocessed()
        assert len(rows) == 50  # the documented default limit

    async def test_episode_mark_processed(self, store: Store):
        feed = await self._seed_feed(store)
        ep = await store.episodes.save(_episode(feed.feed_id, "E", processed=False))
        assert await store.episodes.count_unprocessed() == 1

        updated = await store.episodes.mark_processed(ep.episode_id)
        assert updated is not None
        assert updated.processed is True
        assert await store.episodes.count_unprocessed() == 0
        assert await store.episodes.list_unprocessed() == []

        updated = await store.episodes.mark_processed(ep.episode_id, processed=False)
        assert updated is not None and updated.processed is False
        assert await store.episodes.count_unprocessed() == 1

        assert await store.episodes.mark_processed(uuid4()) is None

    async def test_episode_save_many_preserves_input_order(self, store: Store):
        feed = await self._seed_feed(store)
        eps = [_episode(feed.feed_id, f"E{i}", guid=f"m-{i}") for i in range(4)]
        saved = await store.episodes.save_many(eps)
        assert [e.episode_id for e in saved] == [e.episode_id for e in eps]
        for e in eps:
            assert await store.episodes.get_by_id(e.episode_id) is not None

    async def test_episode_counts(self, store: Store):
        feed = await self._seed_feed(store)
        assert await store.episodes.count_all() == 0
        assert await store.episodes.count_unprocessed() == 0
        await store.episodes.save(_episode(feed.feed_id, "A", processed=False))
        await store.episodes.save(_episode(feed.feed_id, "B", processed=True))
        assert await store.episodes.count_all() == 2
        assert await store.episodes.count_unprocessed() == 1

    # ------------------------------------------------------------------
    # Insight repository
    # ------------------------------------------------------------------

    async def _seed_episode(self, store: Store) -> Episode:
        feed = await self._seed_feed(store)
        return await store.episodes.save(_episode(feed.feed_id))

    async def test_insight_save_and_list_by_episode(self, store: Store):
        ep = await self._seed_episode(store)
        other = await self._seed_episode(store)
        i1 = await store.insights.save(_insight(ep.episode_id, "I1", created_at=_T0))
        i2 = await store.insights.save(
            _insight(ep.episode_id, "I2", created_at=_T0 + timedelta(hours=1))
        )
        await store.insights.save(_insight(other.episode_id, "Other"))

        rows = await store.insights.list_by_episode(ep.episode_id)
        assert [i.insight_id for i in rows] == [i1.insight_id, i2.insight_id]
        assert await store.insights.list_by_episode(uuid4()) == []

    async def test_insight_list_by_episode_ties_unordered(self, store: Store):
        ep = await self._seed_episode(store)
        a = await store.insights.save(_insight(ep.episode_id, "A", created_at=_T0))
        b = await store.insights.save(_insight(ep.episode_id, "B", created_at=_T0))
        rows = await store.insights.list_by_episode(ep.episode_id)
        assert {i.insight_id for i in rows} == {a.insight_id, b.insight_id}

    async def test_insight_save_many_preserves_input_order(self, store: Store):
        ep = await self._seed_episode(store)
        items = [_insight(ep.episode_id, f"I{i}") for i in range(3)]
        saved = await store.insights.save_many(items)
        assert [i.insight_id for i in saved] == [i.insight_id for i in items]
        rows = await store.insights.list_by_episode(ep.episode_id)
        assert len(rows) == 3

    # ------------------------------------------------------------------
    # Tag repository
    # ------------------------------------------------------------------

    async def test_tag_get_by_name_category(self, store: Store):
        tag = await store.tags.get_or_create("python", "topic")
        fetched = await store.tags.get_by_name_category("python", "topic")
        assert fetched is not None
        assert fetched.tag_id == tag.tag_id
        assert await store.tags.get_by_name_category("python", "other") is None
        assert await store.tags.get_by_name_category("nope", "topic") is None

    async def test_tag_get_or_create_is_idempotent(self, store: Store):
        a = await store.tags.get_or_create("rust", "topic")
        b = await store.tags.get_or_create("rust", "topic")
        assert a.tag_id == b.tag_id
        # A null category is a distinct key from a named one.
        c = await store.tags.get_or_create("rust", None)
        assert c.tag_id != a.tag_id

    async def test_tag_add_episode_tag_idempotent_and_ordered(self, store: Store):
        ep = await self._seed_episode(store)
        t_zebra = await store.tags.get_or_create("zebra", None)
        t_apple = await store.tags.get_or_create("apple", None)

        await store.tags.add_episode_tag(ep.episode_id, t_zebra.tag_id)
        await store.tags.add_episode_tag(ep.episode_id, t_apple.tag_id)
        await store.tags.add_episode_tag(ep.episode_id, t_apple.tag_id)  # no-op

        tags = await store.tags.list_tags_for_episode(ep.episode_id)
        assert [t.name for t in tags] == ["apple", "zebra"]
        assert await store.tags.list_tags_for_episode(uuid4()) == []

    # ------------------------------------------------------------------
    # User repository
    # ------------------------------------------------------------------

    async def test_user_save_get_round_trip(self, store: Store):
        saved = await store.users.save(User(email="ada@example.com"))
        assert saved.user_id is not None

        by_id = await store.users.get_by_id(saved.user_id)
        assert by_id is not None and by_id.email == "ada@example.com"

        by_email = await store.users.get_by_email("ada@example.com")
        assert by_email is not None and by_email.user_id == saved.user_id

        assert await store.users.get_by_id(uuid4()) is None
        assert await store.users.get_by_email("nobody@example.com") is None

    # ------------------------------------------------------------------
    # Playlist repository
    # ------------------------------------------------------------------

    async def _seed_user(self, store: Store, email: str | None = None) -> User:
        return await store.users.save(User(email=email or f"{uuid4().hex}@example.com"))

    async def test_playlist_save_list_by_user(self, store: Store):
        user = await self._seed_user(store)
        other = await self._seed_user(store)
        p1 = await store.playlists.save(
            CuratedPlaylist(user_id=user.user_id, title="P1", created_at=_T0)
        )
        p2 = await store.playlists.save(
            CuratedPlaylist(
                user_id=user.user_id, title="P2", created_at=_T0 + timedelta(hours=1)
            )
        )
        await store.playlists.save(CuratedPlaylist(user_id=other.user_id, title="Q"))

        rows = await store.playlists.list_by_user(user.user_id)
        assert [p.playlist_id for p in rows] == [p1.playlist_id, p2.playlist_id]
        assert await store.playlists.list_by_user(uuid4()) == []

        fetched = await store.playlists.get_by_id(p1.playlist_id)
        assert fetched is not None and fetched.title == "P1"
        assert await store.playlists.get_by_id(uuid4()) is None

    async def test_playlist_list_by_user_ties_unordered(self, store: Store):
        user = await self._seed_user(store)
        a = await store.playlists.save(
            CuratedPlaylist(user_id=user.user_id, title="A", created_at=_T0)
        )
        b = await store.playlists.save(
            CuratedPlaylist(user_id=user.user_id, title="B", created_at=_T0)
        )
        rows = await store.playlists.list_by_user(user.user_id)
        assert {p.playlist_id for p in rows} == {a.playlist_id, b.playlist_id}

    async def test_playlist_episodes_order_and_upsert(self, store: Store):
        user = await self._seed_user(store)
        pl = await store.playlists.save(
            CuratedPlaylist(user_id=user.user_id, title="Mix")
        )
        feed = await self._seed_feed(store)
        e1 = await store.episodes.save(_episode(feed.feed_id, "E1"))
        e2 = await store.episodes.save(_episode(feed.feed_id, "E2"))
        e3 = await store.episodes.save(_episode(feed.feed_id, "E3"))

        await store.playlists.add_episode(pl.playlist_id, e1.episode_id, 2)
        await store.playlists.add_episode(pl.playlist_id, e2.episode_id, 0)
        await store.playlists.add_episode(pl.playlist_id, e3.episode_id, 1)

        rows = await store.playlists.list_episodes(pl.playlist_id)
        assert [e.episode_id for e in rows] == [
            e2.episode_id,
            e3.episode_id,
            e1.episode_id,
        ]

        # Re-adding updates the position instead of duplicating the link.
        await store.playlists.add_episode(pl.playlist_id, e1.episode_id, 5)
        rows = await store.playlists.list_episodes(pl.playlist_id)
        assert [e.episode_id for e in rows] == [
            e2.episode_id,
            e3.episode_id,
            e1.episode_id,
        ]
        assert len(rows) == 3

    async def test_playlist_episodes_position_ties_broken_by_id(self, store: Store):
        # The protocol guarantees a fully deterministic order: ties on
        # position are broken by episode_id ascending on ALL backends.
        user = await self._seed_user(store)
        pl = await store.playlists.save(
            CuratedPlaylist(user_id=user.user_id, title="Ties")
        )
        feed = await self._seed_feed(store)
        ids = sorted([uuid4(), uuid4(), uuid4()])
        eps = [
            await store.episodes.save(
                _episode(feed.feed_id, f"E{i}", episode_id=ids[i])
            )
            for i in range(3)
        ]
        # Add in reverse id order, all at position 0.
        for e in reversed(eps):
            await store.playlists.add_episode(pl.playlist_id, e.episode_id, 0)

        rows = await store.playlists.list_episodes(pl.playlist_id)
        assert [e.episode_id for e in rows] == ids

    async def test_playlist_list_entries_carries_position_and_added_at(
        self, store: Store
    ):
        # XIN-98: the RSS endpoint needs curator position order plus the
        # added-to-playlist date per episode. added_at is set on first add
        # and preserved when an existing link is re-added at a new position.
        user = await self._seed_user(store)
        pl = await store.playlists.save(
            CuratedPlaylist(user_id=user.user_id, title="Entries")
        )
        feed = await self._seed_feed(store)
        e1 = await store.episodes.save(_episode(feed.feed_id, "E1"))
        e2 = await store.episodes.save(_episode(feed.feed_id, "E2"))

        await store.playlists.add_episode(pl.playlist_id, e1.episode_id, 1)
        await store.playlists.add_episode(pl.playlist_id, e2.episode_id, 0)

        entries = await store.playlists.list_entries(pl.playlist_id)
        assert [e.episode.episode_id for e in entries] == [
            e2.episode_id,
            e1.episode_id,
        ]
        assert [e.position for e in entries] == [0, 1]
        assert all(e.added_at is not None for e in entries)
        first_added = {
            e.episode.episode_id: e.added_at for e in entries
        }

        # Re-adding updates the position but keeps the original added_at.
        await store.playlists.add_episode(pl.playlist_id, e2.episode_id, 5)
        entries = await store.playlists.list_entries(pl.playlist_id)
        assert [e.episode.episode_id for e in entries] == [
            e1.episode_id,
            e2.episode_id,
        ]
        assert [e.position for e in entries] == [1, 5]
        assert (
            entries[1].added_at.replace(tzinfo=None)
            == first_added[e2.episode_id].replace(tzinfo=None)
        )

    # ------------------------------------------------------------------
    # Playlist publish state (XIN-97)
    # ------------------------------------------------------------------

    async def test_playlist_publish_assigns_slug_and_token(self, store: Store):
        user = await self._seed_user(store)
        pl = await store.playlists.save(
            CuratedPlaylist(user_id=user.user_id, title="My Mix")
        )
        assert pl.visibility == "unlisted"
        assert pl.slug is None
        assert pl.token is None

        published = await store.playlists.publish(pl.playlist_id, "public")
        assert published is not None
        assert published.visibility == "public"
        assert published.slug
        assert re.fullmatch(r"[A-Za-z0-9_-]+", published.slug)
        assert published.token
        assert re.fullmatch(r"[A-Za-z0-9_-]+", published.token)
        assert len(published.token) == 43  # secrets.token_urlsafe(32)

        by_slug = await store.playlists.get_by_slug(published.slug)
        assert by_slug is not None and by_slug.playlist_id == pl.playlist_id
        assert await store.playlists.get_by_slug("no-such-slug") is None

        # Re-publishing keeps the assigned slug and token (stable URLs).
        again = await store.playlists.publish(pl.playlist_id, "unlisted")
        assert again.slug == published.slug
        assert again.token == published.token
        assert again.visibility == "unlisted"

    async def test_playlist_publish_validation_and_missing(self, store: Store):
        user = await self._seed_user(store)
        pl = await store.playlists.save(
            CuratedPlaylist(user_id=user.user_id, title="V")
        )
        with pytest.raises(ValueError):
            await store.playlists.publish(pl.playlist_id, "bogus")
        # Unknown ids resolve to None, like get_by_id.
        assert await store.playlists.publish(uuid4(), "public") is None
        assert await store.playlists.unpublish(uuid4()) is None
        assert await store.playlists.rotate_token(uuid4()) is None

    async def test_playlist_unpublish_keeps_slug_and_token(self, store: Store):
        user = await self._seed_user(store)
        pl = await store.playlists.save(
            CuratedPlaylist(user_id=user.user_id, title="Mix")
        )
        published = await store.playlists.publish(pl.playlist_id, "public")
        unpublished = await store.playlists.unpublish(pl.playlist_id)
        assert unpublished is not None
        assert unpublished.visibility == "unlisted"
        assert unpublished.slug == published.slug
        assert unpublished.token == published.token

    async def test_playlist_rotate_token(self, store: Store):
        user = await self._seed_user(store)
        pl = await store.playlists.save(
            CuratedPlaylist(user_id=user.user_id, title="Mix")
        )
        published = await store.playlists.publish(pl.playlist_id, "unlisted")
        old_token = published.token

        new_token = await store.playlists.rotate_token(pl.playlist_id)
        assert new_token
        assert new_token != old_token

        fetched = await store.playlists.get_by_id(pl.playlist_id)
        assert fetched.token == new_token
        assert fetched.token_revoked_at is not None

        # The slug still resolves after rotation (single-field change).
        assert (
            await store.playlists.get_by_slug(fetched.slug)
        ).playlist_id == pl.playlist_id

    async def test_playlist_slug_uniqueness(self, store: Store):
        user = await self._seed_user(store)
        a = await store.playlists.save(
            CuratedPlaylist(user_id=user.user_id, title="A", slug="taken")
        )
        b = await store.playlists.save(
            CuratedPlaylist(user_id=user.user_id, title="B")
        )
        b.slug = "taken"
        with pytest.raises(SlugConflictError):
            await store.playlists.save(b)

        # Idempotent re-save of the owning playlist is fine.
        a.title = "A2"
        await store.playlists.save(a)
        assert (await store.playlists.get_by_slug("taken")).playlist_id == (
            a.playlist_id
        )

    # ------------------------------------------------------------------
    # Progress repository
    # ------------------------------------------------------------------

    async def test_progress_get_save_upsert(self, store: Store):
        user = await self._seed_user(store)
        ep = await self._seed_episode(store)

        assert await store.progress.get(user.user_id, ep.episode_id) is None

        saved = await store.progress.save(
            UserEpisodeProgress(
                user_id=user.user_id, episode_id=ep.episode_id, position_seconds=42
            )
        )
        fetched = await store.progress.get(user.user_id, ep.episode_id)
        assert fetched is not None
        assert fetched.position_seconds == 42
        assert fetched.user_id == saved.user_id

        # Upsert by (user_id, episode_id): the second save updates the row.
        await store.progress.save(
            UserEpisodeProgress(
                user_id=user.user_id,
                episode_id=ep.episode_id,
                position_seconds=100,
                completed=True,
            )
        )
        fetched = await store.progress.get(user.user_id, ep.episode_id)
        assert fetched is not None
        assert fetched.position_seconds == 100
        assert fetched.completed is True

        assert await store.progress.get(uuid4(), ep.episode_id) is None

    # ------------------------------------------------------------------
    # TaskLog repository
    # ------------------------------------------------------------------

    async def test_tasklog_save_and_list_by_type_status(self, store: Store):
        t1 = await store.task_logs.save(
            TaskLog(task_type="sync", status="pending", created_at=_T0)
        )
        t2 = await store.task_logs.save(
            TaskLog(
                task_type="sync", status="pending", created_at=_T0 + timedelta(hours=1)
            )
        )
        await store.task_logs.save(
            TaskLog(task_type="sync", status="done", created_at=_T0)
        )
        await store.task_logs.save(
            TaskLog(task_type="enrich", status="pending", created_at=_T0)
        )

        rows = await store.task_logs.list_by_type_status("sync", "pending")
        # Newest first.
        assert [t.task_log_id for t in rows] == [t2.task_log_id, t1.task_log_id]

        rows = await store.task_logs.list_by_type_status(
            "sync", "pending", limit=1
        )
        assert [t.task_log_id for t in rows] == [t2.task_log_id]

        assert await store.task_logs.list_by_type_status("sync", "nope") == []

    async def test_tasklog_list_by_type_status_ties_unordered(self, store: Store):
        a = await store.task_logs.save(
            TaskLog(task_type="sync", status="pending", created_at=_T0)
        )
        b = await store.task_logs.save(
            TaskLog(task_type="sync", status="pending", created_at=_T0)
        )
        rows = await store.task_logs.list_by_type_status("sync", "pending")
        assert {t.task_log_id for t in rows} == {a.task_log_id, b.task_log_id}

    async def test_tasklog_update_status(self, store: Store):
        t = await store.task_logs.save(TaskLog(task_type="sync", status="pending"))
        updated = await store.task_logs.update_status(
            t.task_log_id, "failed", error_message="boom"
        )
        assert updated is not None
        assert updated.status == "failed"
        assert updated.error_message == "boom"

        updated = await store.task_logs.update_status(t.task_log_id, "done")
        assert updated is not None and updated.status == "done"

        assert await store.task_logs.update_status(uuid4(), "done") is None

    # ------------------------------------------------------------------
    # Store unit-of-work contract
    # ------------------------------------------------------------------

    async def test_store_context_manager_commits_on_clean_exit(self, backend):
        async with backend.new_store() as s:
            await s.feeds.save(_feed("https://e.com/ctx.xml", "Ctx"))

        reader = backend.new_store()
        try:
            fetched = await reader.feeds.get_by_rss_url("https://e.com/ctx.xml")
            assert fetched is not None and fetched.title == "Ctx"
        finally:
            await reader.close()

    async def test_store_commit_is_explicit_unit_of_work(self, store: Store):
        # The documented write pattern: mutate -> repo.save -> store.commit.
        feed = await store.feeds.save(_feed("https://e.com/uow.xml"))
        await store.commit()
        assert await store.feeds.get_by_id(feed.feed_id) is not None
