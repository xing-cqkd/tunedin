"""Tests for SQLAlchemyStore (XIN-85).

Exercises the repository implementations against a real SQLite database via
SQLAlchemyStore.from_url(), covering the main read/write paths plus the
unit-of-work (commit/rollback) semantics.
"""

import asyncio
import threading
import uuid
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import create_async_engine

from backend.persistence import sqlalchemy_store as sa_module
from backend.persistence.models import (
    Base,
    CuratedPlaylist,
    Episode,
    EpisodeTag,
    Feed,
    Insight,
    Tag,
    TaskLog,
    User,
    UserEpisodeProgress,
)
from backend.persistence.repositories import MissingParentError, SlugConflictError
from backend.persistence.sqlalchemy_store import SQLAlchemyStore
from backend.persistence.validation import ItemTooLargeError

NOW = datetime.now(timezone.utc)


@pytest_asyncio.fixture
async def store(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path}/store_test.db"
    engine = create_async_engine(url, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()

    s = SQLAlchemyStore.from_url(url)
    yield s
    await s.rollback()
    await s.close()


async def make_feed(store, **kw):
    defaults = {
        "rss_url": f"https://example.com/{uuid.uuid4().hex}.xml",
        "title": "Feed",
        "sync_status": "pending",
    }
    defaults.update(kw)
    return await store.feeds.save(Feed(**defaults))


async def make_episode(store, feed, **kw):
    defaults = {
        "feed_id": feed.feed_id,
        "guid": uuid.uuid4().hex,
        "title": "Episode",
        "audio_url": "https://example.com/audio.mp3",
        "processed": False,
    }
    defaults.update(kw)
    return await store.episodes.save(Episode(**defaults))


# ---------------------------------------------------------------------------
# Feed repository
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_feed_save_get_roundtrip(store):
    feed = await make_feed(store, rss_url="https://example.com/a.xml", title="A")
    await store.commit()

    fetched = await store.feeds.get_by_id(feed.feed_id)
    assert fetched is not None
    assert fetched.rss_url == "https://example.com/a.xml"

    by_url = await store.feeds.get_by_rss_url("https://example.com/a.xml")
    assert by_url is not None
    assert by_url.feed_id == feed.feed_id

    assert await store.feeds.get_by_id(uuid.uuid4()) is None
    assert await store.feeds.get_by_rss_url("https://nope.example/x.xml") is None


@pytest.mark.asyncio
async def test_feed_save_populates_pk_without_commit(store):
    feed = await make_feed(store)
    # save() flushes: PK is populated before commit
    assert isinstance(feed.feed_id, uuid.UUID)
    assert await store.feeds.get_by_id(feed.feed_id) is not None
    await store.rollback()
    assert await store.feeds.get_by_id(feed.feed_id) is None


@pytest.mark.asyncio
async def test_feed_save_updates_existing(store):
    feed = await make_feed(store, title="Old")
    feed.title = "New"
    await store.feeds.save(feed)
    await store.commit()
    assert (await store.feeds.get_by_id(feed.feed_id)).title == "New"


@pytest.mark.asyncio
async def test_feed_list_by_statuses_ordering_and_limit(store):
    f1 = await make_feed(store, sync_status="discovered")
    f2 = await make_feed(store, sync_status="pending")
    f3 = await make_feed(store, sync_status="active")
    await store.commit()

    rows = await store.feeds.list_by_statuses(["discovered", "pending"])
    assert [f.feed_id for f in rows] == [f1.feed_id, f2.feed_id]  # created_at asc

    limited = await store.feeds.list_by_statuses(
        ["discovered", "pending"], limit=1
    )
    assert [f.feed_id for f in limited] == [f1.feed_id]

    assert await store.feeds.list_by_statuses([]) == []
    assert await store.feeds.list_by_statuses(["nope"]) == []


@pytest.mark.asyncio
async def test_feed_counts_and_list_all(store):
    await make_feed(store, sync_status="discovered")
    await make_feed(store, sync_status="discovered")
    await make_feed(store, sync_status="error")
    await store.commit()

    assert await store.feeds.count_all() == 3
    assert await store.feeds.count_by_status("discovered") == 2
    assert await store.feeds.count_by_status("active") == 0
    assert await store.feeds.count_by_statuses(["discovered", "pending"]) == 2
    assert await store.feeds.count_by_statuses(["discovered", "pending", "error"]) == 3
    assert await store.feeds.count_by_statuses([]) == 0
    assert await store.feeds.count_by_statuses(["nope"]) == 0

    all_feeds = await store.feeds.list_all()
    assert len(all_feeds) == 3
    assert len(await store.feeds.list_all(limit=2)) == 2


@pytest.mark.asyncio
async def test_feed_list_error_due_retry(store):
    cutoff = NOW - timedelta(seconds=300)
    due = await make_feed(
        store,
        sync_status="error",
        error_count=2,
        last_fetched_at=NOW - timedelta(hours=1),
    )
    not_yet_due = await make_feed(
        store,
        sync_status="error",
        error_count=1,
        last_fetched_at=NOW - timedelta(seconds=60),
    )
    abandoned = await make_feed(
        store,
        sync_status="error",
        error_count=10,
        last_fetched_at=NOW - timedelta(days=2),
    )
    pending = await make_feed(store, sync_status="pending")
    await store.commit()

    rows = await store.feeds.list_error_due_retry(cutoff, max_attempts=10)
    ids = {f.feed_id for f in rows}
    assert due.feed_id in ids
    assert not_yet_due.feed_id not in ids
    assert abandoned.feed_id not in ids
    assert pending.feed_id not in ids


# ---------------------------------------------------------------------------
# Episode repository
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_episode_save_get_roundtrip(store):
    feed = await make_feed(store)
    ep = await make_episode(store, feed, guid="g1", title="E1")
    await store.commit()

    fetched = await store.episodes.get_by_id(ep.episode_id)
    assert fetched is not None
    assert fetched.title == "E1"
    assert await store.episodes.get_by_id(uuid.uuid4()) is None


@pytest.mark.asyncio
async def test_episode_save_many_returns_in_order(store):
    feed = await make_feed(store)
    eps = [
        Episode(
            feed_id=feed.feed_id,
            guid=f"g{i}",
            title=f"E{i}",
            audio_url="https://example.com/a.mp3",
        )
        for i in range(3)
    ]
    saved = await store.episodes.save_many(eps)
    assert [e.title for e in saved] == ["E0", "E1", "E2"]
    assert all(isinstance(e.episode_id, uuid.UUID) for e in saved)
    await store.commit()
    assert await store.episodes.count_all() == 3


@pytest.mark.asyncio
async def test_episode_list_guids_by_feed_returns_all_guids(store):
    # XIN-68: guid is NOT NULL, so every stored episode contributes its
    # guid; list_guids_by_feed returns the full per-feed set.
    feed = await make_feed(store)
    other = await make_feed(store)
    await make_episode(store, feed, guid="keep")
    await make_episode(store, feed, guid="keep-2")
    await make_episode(store, other, guid="other")
    await store.commit()

    assert await store.episodes.list_guids_by_feed(feed.feed_id) == {"keep", "keep-2"}


@pytest.mark.asyncio
async def test_episode_list_unprocessed_ordering(store):
    feed = await make_feed(store)
    e_old = await make_episode(
        store, feed, published_at=NOW - timedelta(days=2), processed=False
    )
    e_new = await make_episode(
        store, feed, published_at=NOW - timedelta(days=1), processed=False
    )
    e_null = await make_episode(store, feed, published_at=None, processed=False)
    e_done = await make_episode(store, feed, published_at=NOW, processed=True)
    await store.commit()

    rows = await store.episodes.list_unprocessed()
    # published_at desc, nulls last; processed one excluded
    assert [e.episode_id for e in rows] == [
        e_new.episode_id,
        e_old.episode_id,
        e_null.episode_id,
    ]

    limited = await store.episodes.list_unprocessed(limit=2)
    assert [e.episode_id for e in limited] == [e_new.episode_id, e_old.episode_id]

    by_feed = await store.episodes.list_unprocessed(feed_id=feed.feed_id)
    assert len(by_feed) == 3
    assert await store.episodes.list_unprocessed(feed_id=uuid.uuid4()) == []


@pytest.mark.asyncio
async def test_episode_mark_processed(store):
    feed = await make_feed(store)
    ep = await make_episode(store, feed, processed=False)
    await store.commit()

    updated = await store.episodes.mark_processed(ep.episode_id)
    assert updated is not None
    assert updated.processed is True
    await store.commit()
    assert await store.episodes.count_unprocessed() == 0

    assert await store.episodes.mark_processed(uuid.uuid4()) is None


@pytest.mark.asyncio
async def test_episode_counts(store):
    feed = await make_feed(store)
    await make_episode(store, feed, processed=False)
    await make_episode(store, feed, processed=True)
    await store.commit()

    assert await store.episodes.count_all() == 2
    assert await store.episodes.count_unprocessed() == 1


# ---------------------------------------------------------------------------
# Insight repository
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_insight_crud(store):
    feed = await make_feed(store)
    ep = await make_episode(store, feed)
    other = await make_episode(store, feed)

    i1 = await store.insights.save(
        Insight(episode_id=ep.episode_id, title="First", detail="d1")
    )
    await store.insights.save_many(
        [
            Insight(episode_id=ep.episode_id, title="Second", detail="d2"),
            Insight(episode_id=other.episode_id, title="Other", detail="d3"),
        ]
    )
    await store.commit()
    assert isinstance(i1.insight_id, uuid.UUID)

    rows = await store.insights.list_by_episode(ep.episode_id)
    assert [i.title for i in rows] == ["First", "Second"]  # created_at asc
    assert await store.insights.list_by_episode(uuid.uuid4()) == []


# ---------------------------------------------------------------------------
# Tag repository
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tag_get_or_create_and_links(store):
    tag = await store.tags.get_or_create("tech", "topic")
    same = await store.tags.get_or_create("tech", "topic")
    assert tag.tag_id == same.tag_id
    assert isinstance(tag.tag_id, uuid.UUID)

    # NULL category is distinct from a string category
    null_cat = await store.tags.get_or_create("tech", None)
    assert null_cat.tag_id != tag.tag_id
    assert await store.tags.get_by_name_category("tech", "topic") is not None
    assert await store.tags.get_by_name_category("nope", None) is None
    await store.commit()

    feed = await make_feed(store)
    ep = await make_episode(store, feed)
    other_tag = await store.tags.get_or_create("news", None)

    await store.tags.add_episode_tag(ep.episode_id, tag.tag_id)
    await store.tags.add_episode_tag(ep.episode_id, other_tag.tag_id)
    # idempotent: second add is a no-op
    await store.tags.add_episode_tag(ep.episode_id, tag.tag_id)
    await store.commit()

    tags = await store.tags.list_tags_for_episode(ep.episode_id)
    assert [t.name for t in tags] == ["news", "tech"]  # name asc


# ---------------------------------------------------------------------------
# User repository
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_user_crud(store):
    user = await store.users.save(User(email="u@example.com"))
    await store.commit()

    assert isinstance(user.user_id, uuid.UUID)
    assert (await store.users.get_by_id(user.user_id)).email == "u@example.com"
    assert (await store.users.get_by_email("u@example.com")).user_id == user.user_id
    assert await store.users.get_by_email("missing@example.com") is None


# ---------------------------------------------------------------------------
# Playlist repository
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_playlist_crud_and_episode_links(store):
    user = await store.users.save(User(email="pl@example.com"))
    feed = await make_feed(store)
    e1 = await make_episode(store, feed)
    e2 = await make_episode(store, feed)
    e3 = await make_episode(store, feed)

    pl = await store.playlists.save(
        CuratedPlaylist(user_id=user.user_id, title="My list")
    )
    await store.commit()

    assert (await store.playlists.get_by_id(pl.playlist_id)).title == "My list"
    assert await store.playlists.get_by_id(uuid.uuid4()) is None
    user_lists = await store.playlists.list_by_user(user.user_id)
    assert [p.playlist_id for p in user_lists] == [pl.playlist_id]

    await store.playlists.add_episode(pl.playlist_id, e2.episode_id, position=1)
    await store.playlists.add_episode(pl.playlist_id, e1.episode_id, position=0)
    # re-adding the same link updates the position instead of duplicating
    await store.playlists.add_episode(pl.playlist_id, e2.episode_id, position=2)
    await store.commit()

    eps = await store.playlists.list_episodes(pl.playlist_id)
    assert [e.episode_id for e in eps] == [e1.episode_id, e2.episode_id]
    assert e3.episode_id not in {e.episode_id for e in eps}


@pytest.mark.asyncio
async def test_add_episode_missing_parents_raises_missing_parent_error(store):
    """XIN-124 (Chester's call): FK parity — the FK IntegrityError maps to
    the backend-agnostic MissingParentError (DynamoDB raises the same
    type by checking parent existence before writing)."""
    user = await store.users.save(User(email="fk@example.com"))
    feed = await make_feed(store)
    ep = await make_episode(store, feed)
    pl = await store.playlists.save(
        CuratedPlaylist(user_id=user.user_id, title="FK list")
    )
    await store.commit()

    # Missing playlist.
    with pytest.raises(MissingParentError):
        await store.playlists.add_episode(uuid.uuid4(), ep.episode_id, 0)
    await store.rollback()
    # Missing episode.
    with pytest.raises(MissingParentError):
        await store.playlists.add_episode(pl.playlist_id, uuid.uuid4(), 0)
    await store.rollback()
    # Both missing.
    with pytest.raises(MissingParentError):
        await store.playlists.add_episode(uuid.uuid4(), uuid.uuid4(), 0)
    await store.rollback()


# ---------------------------------------------------------------------------
# Progress repository
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_progress_crud(store):
    user = await store.users.save(User(email="pr@example.com"))
    feed = await make_feed(store)
    ep = await make_episode(store, feed)

    prog = await store.progress.save(
        UserEpisodeProgress(
            user_id=user.user_id, episode_id=ep.episode_id, position_seconds=42
        )
    )
    await store.commit()

    fetched = await store.progress.get(user.user_id, ep.episode_id)
    assert fetched is not None
    assert fetched.position_seconds == 42
    assert await store.progress.get(user.user_id, uuid.uuid4()) is None

    # upsert: save again with new position
    prog.position_seconds = 100
    await store.progress.save(prog)
    await store.commit()
    assert (await store.progress.get(user.user_id, ep.episode_id)).position_seconds == 100


# ---------------------------------------------------------------------------
# TaskLog repository
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_log_crud(store):
    t1 = await store.task_logs.save(
        TaskLog(task_type="SYNC", payload_json="{}", status="pending")
    )
    await store.task_logs.save(
        TaskLog(task_type="SYNC", payload_json="{}", status="done")
    )
    await store.task_logs.save(
        TaskLog(task_type="OTHER", payload_json="{}", status="pending")
    )
    await store.commit()

    rows = await store.task_logs.list_by_type_status("SYNC", "pending")
    assert [t.task_log_id for t in rows] == [t1.task_log_id]

    updated = await store.task_logs.update_status(
        t1.task_log_id, "failed", error_message="boom"
    )
    assert updated is not None
    assert updated.status == "failed"
    assert updated.error_message == "boom"
    await store.commit()

    # newest-first ordering honored on a fresh read
    assert await store.task_logs.update_status(uuid.uuid4(), "done") is None
    assert len(await store.task_logs.list_by_type_status("SYNC", "failed")) == 1


# ---------------------------------------------------------------------------
# Store unit-of-work semantics
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_store_commit_persists_and_rollback_discards(store):
    await make_feed(store)
    await store.commit()
    assert await store.feeds.count_all() == 1

    await make_feed(store)
    await store.rollback()
    assert await store.feeds.count_all() == 1


@pytest.mark.asyncio
async def test_store_context_manager(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path}/ctx_test.db"
    engine = create_async_engine(url, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()

    async with SQLAlchemyStore.from_url(url) as s:
        await s.feeds.save(
            Feed(rss_url="https://example.com/ctx.xml", title="Ctx")
        )
    # clean exit committed
    async with SQLAlchemyStore.from_url(url) as s2:
        assert await s2.feeds.count_all() == 1

    try:
        async with SQLAlchemyStore.from_url(url) as s3:
            await s3.feeds.save(
                Feed(rss_url="https://example.com/ctx2.xml", title="Ctx2")
            )
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    # exception exit rolled back
    async with SQLAlchemyStore.from_url(url) as s4:
        assert await s4.feeds.count_all() == 1


# ---------------------------------------------------------------------------
# XIN-119: SAVEPOINT-scoped transaction handling
# ---------------------------------------------------------------------------


async def test_playlist_slug_conflict_preserves_unit_of_work(store):
    """A slug conflict rolls back only its SAVEPOINT (XIN-119).

    The caller's other flushed-but-uncommitted writes (feed, episode)
    survive the conflict, and the session stays usable afterwards.
    """
    user = await store.users.save(User(email=f"u-{uuid.uuid4().hex}@e.com"))
    taken = f"slug-{uuid.uuid4().hex[:8]}"
    await store.playlists.save(
        CuratedPlaylist(user_id=user.user_id, title="First", slug=taken)
    )
    # Other writes staged in the same unit of work, flushed but uncommitted.
    feed = await make_feed(store)
    episode = await make_episode(store, feed)

    with pytest.raises(SlugConflictError) as excinfo:
        await store.playlists.save(
            CuratedPlaylist(user_id=user.user_id, title="Clash", slug=taken)
        )
    # The slug was captured before any rollback expired the instance.
    assert taken in str(excinfo.value)

    # The rest of the unit of work survived the savepoint rollback ...
    assert await store.feeds.get_by_id(feed.feed_id) is not None
    assert await store.episodes.get_by_id(episode.episode_id) is not None
    # ... and the session is still usable: everything commits cleanly.
    await store.commit()
    assert await store.feeds.get_by_id(feed.feed_id) is not None
    assert await store.episodes.get_by_id(episode.episode_id) is not None


def _run_race(winner_target, loser_target):
    """Run two racy threads; fail loudly on deadlock or thread errors."""
    failures: list[BaseException] = []
    def _wrap(target):
        def _run():
            try:
                target()
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                failures.append(exc)
        return _run

    threads = [
        threading.Thread(target=_wrap(winner_target)),
        threading.Thread(target=_wrap(loser_target)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert not any(t.is_alive() for t in threads), "race threads deadlocked"
    assert not failures, f"race thread failures: {failures!r}"


async def _row_count(url: str, model, *filters) -> int:
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            res = await conn.execute(
                select(func.count()).select_from(model).where(*filters)
            )
            return res.scalar_one()
    finally:
        await engine.dispose()


def test_tag_get_or_create_concurrent_race_returns_winner(tmp_path):
    """Two threads, two sessions, one file DB (XIN-119).

    Thread B SELECT-misses the tag while A's insert is uncommitted, then
    flushes after A commits: B's flush hits the partial unique index, B
    rolls back to its savepoint and returns A's row instead of raising.
    Afterwards exactly one tag is visible.
    """
    url = f"sqlite+aiosqlite:///{tmp_path}/tag_race.db"

    async def _create_tables():
        engine = create_async_engine(url)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await engine.dispose()

    asyncio.run(_create_tables())

    b_selected = threading.Event()
    a_committed = threading.Event()
    outcome: dict[str, uuid.UUID] = {}

    def _winner():
        async def _go():
            store = SQLAlchemyStore.from_url(url)
            try:
                tag = await store.tags.get_or_create("race-tag", None)
                outcome["a_tag_id"] = tag.tag_id
                assert b_selected.wait(timeout=30), "B never SELECT-missed"
                await store.commit()
            finally:
                await store.close()
            a_committed.set()

        asyncio.run(_go())

    def _loser():
        async def _go():
            store = SQLAlchemyStore.from_url(url)
            try:
                # Pause between the SELECT-miss and the insert, forcing B
                # deterministically down the loser's race path.
                orig = store.tags.get_by_name_category
                first = True

                async def _select_then_wait(name, category):
                    nonlocal first
                    tag = await orig(name, category)
                    if first and tag is None:
                        first = False
                        b_selected.set()
                        assert a_committed.wait(timeout=30), (
                            "A never committed"
                        )
                    return tag

                store.tags.get_by_name_category = _select_then_wait  # type: ignore[method-assign]
                tag = await store.tags.get_or_create("race-tag", None)
                outcome["b_tag_id"] = tag.tag_id
            finally:
                await store.close()

        asyncio.run(_go())

    _run_race(_winner, _loser)
    # Both threads converged on the same row; only one tag exists.
    assert outcome["a_tag_id"] == outcome["b_tag_id"]
    assert (
        asyncio.run(_row_count(url, Tag, Tag.name == "race-tag")) == 1
    )


def test_add_episode_tag_concurrent_race_is_noop(tmp_path):
    """Same setup as the tag race, but for episode-tag links (XIN-119).

    B's existence-check misses while A's link is uncommitted; after A
    commits, B's insert hits the link PK and B treats the duplicate as the
    protocol-promised no-op. Exactly one link row exists afterwards.
    """
    url = f"sqlite+aiosqlite:///{tmp_path}/link_race.db"

    async def _seed():
        engine = create_async_engine(url)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await engine.dispose()
        store = SQLAlchemyStore.from_url(url)
        try:
            feed = await store.feeds.save(
                Feed(
                    rss_url="https://example.com/race.xml",
                    title="Race",
                    sync_status="pending",
                )
            )
            episode = await store.episodes.save(
                Episode(
                    feed_id=feed.feed_id,
                    guid="race-guid",
                    title="Race episode",
                    audio_url="https://example.com/race.mp3",
                )
            )
            tag = await store.tags.get_or_create("link-race-tag", None)
            await store.commit()
            return episode.episode_id, tag.tag_id
        finally:
            await store.close()

    episode_id, tag_id = asyncio.run(_seed())

    a_flushed = threading.Event()
    b_checked = threading.Event()
    a_committed = threading.Event()

    def _winner():
        async def _go():
            store = SQLAlchemyStore.from_url(url)
            try:
                # Check misses (no link yet) -> insert+flush, uncommitted.
                await store.tags.add_episode_tag(episode_id, tag_id)
                a_flushed.set()
                assert b_checked.wait(timeout=30), "B never checked"
                await store.commit()
            finally:
                await store.close()
            a_committed.set()

        asyncio.run(_go())

    def _loser():
        async def _go():
            store = SQLAlchemyStore.from_url(url)
            try:
                assert a_flushed.wait(timeout=30), "A never flushed"
                # Pause between the existence-check (miss) and the insert,
                # forcing B deterministically down the loser's race path.
                orig_execute = store._session.execute
                first = True

                async def _execute_then_wait(statement, *args, **kwargs):
                    nonlocal first
                    result = await orig_execute(statement, *args, **kwargs)
                    if first:
                        first = False
                        b_checked.set()
                        assert a_committed.wait(timeout=30), (
                            "A never committed"
                        )
                    return result

                store._session.execute = _execute_then_wait  # type: ignore[method-assign]
                # Must not raise: the duplicate add is a no-op.
                await store.tags.add_episode_tag(episode_id, tag_id)
            finally:
                await store.close()

        asyncio.run(_go())

    _run_race(_winner, _loser)
    assert (
        asyncio.run(
            _row_count(
                url,
                EpisodeTag,
                EpisodeTag.episode_id == episode_id,
                EpisodeTag.tag_id == tag_id,
            )
        )
        == 1
    )


# ---------------------------------------------------------------------------
# XIN-120: upsert/dedup semantics
# ---------------------------------------------------------------------------


async def test_episode_save_upserts_by_primary_key(store):
    """save() is an upsert by PK: re-saving an existing id updates (XIN-120)."""
    feed = await make_feed(store)
    ep = await make_episode(store, feed, title="v1")
    resaved = await store.episodes.save(
        Episode(
            episode_id=ep.episode_id,
            feed_id=feed.feed_id,
            guid=ep.guid,
            title="v2",
            audio_url=ep.audio_url,
        )
    )
    assert resaved.episode_id == ep.episode_id
    fetched = await store.episodes.get_by_id(ep.episode_id)
    assert fetched is not None
    assert fetched.title == "v2"
    # Still exactly one row for the episode.
    assert await store.episodes.list_guids_by_feed(feed.feed_id) == {ep.guid}


# ---------------------------------------------------------------------------
# XIN-122: item-size guard coverage
# ---------------------------------------------------------------------------


async def test_episode_save_many_enforces_item_size_guard(store):
    """save_many guards every item before writing (XIN-122)."""
    feed = await make_feed(store)
    good = Episode(
        feed_id=feed.feed_id,
        guid="good",
        title="Good",
        audio_url="https://example.com/a.mp3",
    )
    bad = Episode(
        feed_id=feed.feed_id,
        guid="bad",
        title="Bad",
        audio_url="https://example.com/a.mp3",
        transcript="x" * (500 * 1024),  # over the 400 KiB per-item limit
    )
    with pytest.raises(ItemTooLargeError):
        await store.episodes.save_many([good, bad])
    # The guard fires before any write: nothing from the batch persisted.
    assert "good" not in await store.episodes.list_guids_by_feed(feed.feed_id)


async def test_feed_update_status_enforces_item_size_guard(store):
    """update_status guards the new status value (XIN-122)."""
    feed = await make_feed(store)
    with pytest.raises(ItemTooLargeError):
        await store.feeds.update_status(feed.feed_id, "x" * (500 * 1024))


async def test_publish_and_add_episode_tag_skip_item_size_guard(
    store, monkeypatch
):
    """progress.save() guards; publish/add_episode_tag never do (XIN-122)."""
    calls: list[str] = []
    orig = sa_module._guard_item_size

    def _spy(entity):
        calls.append(type(entity).__name__)
        return orig(entity)

    feed = await make_feed(store)
    episode = await make_episode(store, feed)
    user = await store.users.save(User(email=f"u-{uuid.uuid4().hex}@e.com"))
    playlist = await store.playlists.save(
        CuratedPlaylist(user_id=user.user_id, title="P")
    )
    tag = await store.tags.get_or_create("spy-tag", None)

    monkeypatch.setattr(sa_module, "_guard_item_size", _spy)

    # save() paths DO guard ...
    await store.progress.save(
        UserEpisodeProgress(user_id=user.user_id, episode_id=episode.episode_id)
    )
    assert calls == ["UserEpisodeProgress"]

    # ... but publish/unpublish/rotate_token/add_episode_tag never do.
    await store.playlists.publish(playlist.playlist_id, "public")
    await store.playlists.unpublish(playlist.playlist_id)
    await store.playlists.rotate_token(playlist.playlist_id)
    await store.tags.add_episode_tag(episode.episode_id, tag.tag_id)
    assert calls == ["UserEpisodeProgress"]
