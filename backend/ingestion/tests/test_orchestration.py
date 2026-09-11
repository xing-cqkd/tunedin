"""XIN-38: coverage for the single feed-sync orchestrator.

Covers the required behaviors:
- concurrency=1 matches the sequential ``sync_all_pending_feeds`` behavior
- max_feeds honored after the due/backoff filter
- delay_between_feeds honored
- isolated per-feed sessions
- one FeedSyncError does not poison subsequent feeds (rollback + repair)
- progress callbacks receive useful events/results
"""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from backend.ingestion.errors import FeedSyncError
from backend.ingestion.orchestration import (
    FeedSyncOrchestrator,
    SyncPolicy,
    error_retry_due,
)
from backend.ingestion.service import FeedSyncService
from backend.persistence.models.base import Base
from backend.persistence.models.episode import Episode
from backend.persistence.models.feed import Feed
from backend.persistence.sqlalchemy_store import SQLAlchemyStore

NOW = datetime(2026, 9, 11, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
async def session_factory():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    yield factory
    await engine.dispose()


def _orchestrator_session_factory(session_factory, created):
    """Session factory for the orchestrator: a fresh SQLAlchemyStore (fresh
    AsyncSession) per call, recording every store it creates."""

    @asynccontextmanager
    async def factory():
        store = SQLAlchemyStore(session_factory)
        created.append(store)
        try:
            yield store
        finally:
            await store.close()

    return factory


async def _seed_feeds(session_factory, specs):
    """Create Feed rows; each spec is (title, sync_status, error_count,
    last_fetched_ago_seconds|None). Returns the feeds in creation order."""
    now = datetime.now(timezone.utc)
    async with session_factory() as session:
        feeds = []
        for title, sync_status, error_count, ago in specs:
            feed = Feed(
                rss_url=f"https://{title}.example.com/feed.xml",
                title=title,
                sync_status=sync_status,
                error_count=error_count,
                last_fetched_at=(
                    now - timedelta(seconds=ago) if ago is not None else None
                ),
            )
            session.add(feed)
            feeds.append(feed)
        await session.commit()
        for f in feeds:
            await session.refresh(f)
        return feeds


async def _clear_feeds(session_factory):
    async with session_factory() as session:
        for f in (await session.execute(select(Feed))).scalars():
            await session.delete(f)
        await session.commit()


class _FakeSyncService:
    """Stands in for FeedSyncService: configurable per-feed behavior."""

    def __init__(self, fail_titles=(), dirty_titles=()):
        self.fail_titles = set(fail_titles)
        self.dirty_titles = set(dirty_titles)
        self.seen_stores = []
        self.seen_feed_ids = []

    async def sync_podcast_episodes_by_id(
        self, store, feed_id, client=None, auto_queue_episodes=0
    ):
        self.seen_stores.append(store)
        self.seen_feed_ids.append(feed_id)
        feed = await store.feeds.get_by_id(feed_id)
        if feed.title in self.dirty_titles:
            # Simulate a persistence-step failure AFTER a partial write:
            # the orchestrator must roll this back.
            await store.episodes.save(
                Episode(
                    feed_id=feed.feed_id,
                    guid="partial-write",
                    title="partial",
                    audio_url="https://x.example.com/a.mp3",
                )
            )
            raise FeedSyncError("persistence boom")
        if feed.title in self.fail_titles:
            raise FeedSyncError("sync boom")
        return feed, [f"ep-{feed.title}-1", f"ep-{feed.title}-2"]


class TestSyncPolicy:
    def test_concurrency_zero_rejected(self):
        """XIN-76: concurrency=0 deadlocks the worker pool — reject it."""
        with pytest.raises(ValueError, match="concurrency must be >= 1"):
            SyncPolicy(concurrency=0)


class TestFeedSelection:
    @pytest.mark.asyncio
    async def test_max_feeds_after_backoff_filter(self, session_factory):
        """max_feeds applies AFTER the due/backoff filter: not-yet-due error
        feeds never consume limit slots, and skipped_backoff counts them."""
        await _seed_feeds(
            session_factory,
            [
                ("p1", "pending", 0, None),
                ("p2", "pending", 0, None),
                ("p3", "pending", 0, None),
                # error_count=2 -> 600s exact backoff, fetched 400s ago:
                # passes the coarse 300s SQL pre-filter but fails the exact
                # per-row check -> counted in skipped_backoff, never attempted
                ("err-waiting", "error", 2, 400),
                # error_count=1, fetched long ago -> due for retry
                ("err-due", "error", 1, 3600),
            ],
        )
        created = []
        svc = _FakeSyncService()
        orch = FeedSyncOrchestrator(
            sync_service=svc,
            policy=SyncPolicy(concurrency=1, max_feeds=2),
            session_factory=_orchestrator_session_factory(session_factory, created),
        )
        summary = await orch.run()

        assert summary["total_feeds_processed"] == 2
        assert summary["skipped_backoff"] == 1
        # The not-due error feed was never attempted
        assert len(svc.seen_feed_ids) == 2

    @pytest.mark.asyncio
    async def test_error_retry_due_boundary(self):
        """The moved backoff helper keeps the XIN-34 semantics."""
        feed = Feed(
            rss_url="https://x.example.com/f.xml",
            title="T",
            sync_status="error",
            error_count=1,
            last_fetched_at=NOW - timedelta(seconds=301),
        )
        assert error_retry_due(feed, NOW) is True
        feed.last_fetched_at = NOW - timedelta(seconds=299)
        assert error_retry_due(feed, NOW) is False


class TestConcurrencyEquivalence:
    @pytest.mark.asyncio
    async def test_concurrency_1_matches_sequential(self, session_factory):
        """Two identical concurrency=1 runs produce identical summaries
        (deterministic sequential behavior, like the old sequential path)."""

        async def run_once():
            await _seed_feeds(
                session_factory,
                [("a", "pending", 0, None), ("b", "discovered", 0, None)],
            )
            created = []
            orch = FeedSyncOrchestrator(
                sync_service=_FakeSyncService(),
                policy=SyncPolicy(concurrency=1),
                session_factory=_orchestrator_session_factory(
                    session_factory, created
                ),
            )
            summary = await orch.run()
            await _clear_feeds(session_factory)
            return summary

        first = await run_once()
        second = await run_once()
        assert first == second
        assert first["total_feeds_processed"] == 2
        assert first["total_synced"] == 2
        assert first["total_episodes_saved"] == 4
        assert first["failed_count"] == 0


class TestDelay:
    @pytest.mark.asyncio
    async def test_delay_between_feeds_honored(self, session_factory, monkeypatch):
        await _seed_feeds(
            session_factory,
            [("a", "pending", 0, None), ("b", "pending", 0, None)],
        )
        import backend.ingestion.orchestration as orch_module

        sleep_mock = AsyncMock()
        monkeypatch.setattr(orch_module.asyncio, "sleep", sleep_mock)

        created = []
        orch = FeedSyncOrchestrator(
            sync_service=_FakeSyncService(),
            policy=SyncPolicy(concurrency=1, delay_between_feeds=0.25),
            session_factory=_orchestrator_session_factory(session_factory, created),
        )
        await orch.run()

        delays = [c.args[0] for c in sleep_mock.call_args_list]
        assert delays == [0.25, 0.25]


class TestSessionIsolation:
    @pytest.mark.asyncio
    async def test_per_feed_sessions_isolated(self, session_factory):
        """Each worker gets its own session; the selection session is never
        reused for a feed sync."""
        await _seed_feeds(
            session_factory,
            [("a", "pending", 0, None), ("b", "pending", 0, None), ("c", "pending", 0, None)],
        )
        created = []
        svc = _FakeSyncService()
        orch = FeedSyncOrchestrator(
            sync_service=svc,
            policy=SyncPolicy(concurrency=2),
            session_factory=_orchestrator_session_factory(session_factory, created),
        )
        await orch.run()

        # 1 selection session + 3 worker sessions, all distinct objects
        assert len(created) == 4
        assert len({id(s) for s in created}) == 4
        selection_session = created[0]
        worker_stores = svc.seen_stores
        assert len(worker_stores) == 3
        assert selection_session not in worker_stores
        assert len({id(s) for s in worker_stores}) == 3


class TestErrorIsolation:
    @pytest.mark.asyncio
    async def test_feed_sync_error_does_not_poison_subsequent(self, session_factory):
        """A FeedSyncError on one feed: the run continues, the failed feed is
        marked ERROR (rollback before repair), later feeds still sync."""
        feeds = await _seed_feeds(
            session_factory,
            [("good1", "pending", 0, None), ("bad", "pending", 0, None), ("good2", "pending", 0, None)],
        )
        bad_id = feeds[1].feed_id
        created = []
        orch = FeedSyncOrchestrator(
            sync_service=_FakeSyncService(fail_titles={"bad"}),
            policy=SyncPolicy(concurrency=1),
            session_factory=_orchestrator_session_factory(session_factory, created),
        )
        summary = await orch.run()

        assert summary["total_feeds_processed"] == 3
        assert summary["total_synced"] == 2
        assert summary["failed_count"] == 1
        assert summary["failed_feed_ids"] == [str(bad_id)]

        async with session_factory() as session:
            store = SQLAlchemyStore(lambda: session)
            bad = await store.feeds.get_by_id(bad_id)
            assert bad.sync_status == "error"
            assert bad.error_count == 1

    @pytest.mark.asyncio
    async def test_partial_write_rolled_back(self, session_factory):
        """A persistence-step failure after a partial write rolls the
        worker's session back: the partial episode must not survive."""
        feeds = await _seed_feeds(
            session_factory, [("dirty", "pending", 0, None)]
        )
        created = []
        orch = FeedSyncOrchestrator(
            sync_service=_FakeSyncService(dirty_titles={"dirty"}),
            policy=SyncPolicy(concurrency=1),
            session_factory=_orchestrator_session_factory(session_factory, created),
        )
        summary = await orch.run()

        assert summary["failed_count"] == 1
        async with session_factory() as session:
            store = SQLAlchemyStore(lambda: session)
            eps = await store.episodes.list_episodes_by_feed(feeds[0].feed_id)
            assert eps == []
            dirty = await store.feeds.get_by_id(feeds[0].feed_id)
            assert dirty.sync_status == "error"


class TestProgressCallbacks:
    @pytest.mark.asyncio
    async def test_callbacks_receive_useful_events(self, session_factory):
        await _seed_feeds(
            session_factory,
            [("good", "pending", 0, None), ("bad", "pending", 0, None)],
        )
        synced, failed, summaries = [], [], []
        created = []
        orch = FeedSyncOrchestrator(
            sync_service=_FakeSyncService(fail_titles={"bad"}),
            policy=SyncPolicy(
                concurrency=1,
                on_feed_synced=lambda title, n: synced.append((title, n)),
                on_feed_failed=lambda label, err: failed.append((label, err)),
                on_progress=summaries.append,
            ),
            session_factory=_orchestrator_session_factory(session_factory, created),
        )
        summary = await orch.run()

        assert synced == [("good", 2)]
        assert len(failed) == 1
        label, err = failed[0]
        assert "bad" in label
        assert isinstance(err, FeedSyncError)
        assert summaries == [summary]
        assert summary["total_feeds_processed"] == 2
        assert summary["total_synced"] == 1
        assert summary["failed_count"] == 1


class TestServiceDelegate:
    @pytest.mark.asyncio
    async def test_sync_all_pending_feeds_delegates(self, session_factory, monkeypatch):
        """FeedSyncService.sync_all_pending_feeds is the thin sequential
        delegate: same summary shape, caller-owned store reused."""
        await _seed_feeds(session_factory, [("a", "pending", 0, None)])
        svc = FeedSyncService()

        async def fake_sync(store, feed_id, client=None, auto_queue_episodes=0):
            feed = await store.feeds.get_by_id(feed_id)
            return feed, ["ep1"]

        monkeypatch.setattr(svc, "sync_podcast_episodes_by_id", fake_sync)

        seen = []

        @asynccontextmanager
        async def factory():
            store = SQLAlchemyStore(session_factory)
            seen.append(store)
            try:
                yield store
            finally:
                await store.close()

        # Drive the delegate with a caller-owned store, as the old
        # sequential path did.
        async with factory() as store:
            summary = await svc.sync_all_pending_feeds(store)

        assert summary["total_feeds_processed"] == 1
        assert summary["total_synced"] == 1
        assert summary["total_episodes_saved"] == 1
        assert summary["failed_count"] == 0
        assert summary["skipped_backoff"] == 0
        # The delegate reused the caller's store: only one session opened.
        assert len(seen) == 1
