import pytest
import pytest_asyncio
import uuid
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select

from backend.persistence.models import (
    Base, User, Feed, Episode, UserEpisodeProgress,
    Tag, EpisodeTag, Insight, CuratedPlaylist, PlaylistEpisode, TaskLog
)

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"

@pytest_asyncio.fixture
async def test_session():
    engine = create_async_engine(TEST_DB_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.mark.asyncio
async def test_user_creation(test_session: AsyncSession):
    user = User(email="test@example.com")
    test_session.add(user)
    await test_session.commit()

    result = await test_session.execute(select(User).where(User.email == "test@example.com"))
    fetched_user = result.scalar_one_or_none()

    assert fetched_user is not None
    assert isinstance(fetched_user.user_id, uuid.UUID)
    assert fetched_user.email == "test@example.com"


@pytest.mark.asyncio
async def test_feed_and_episode_creation(test_session: AsyncSession):
    feed = Feed(
        rss_url="https://example.com/podcast.xml",
        title="Tech Talks",
        author="Jane Doe",
        description="A podcast about tech",
        etag='"123456"',
        last_modified="Mon, 01 Jan 2026 00:00:00 GMT",
        sync_status="success"
    )
    test_session.add(feed)
    await test_session.commit()

    episode = Episode(
        feed_id=feed.feed_id,
        guid="ep-001",
        title="Episode 1: AI Future",
        audio_url="https://example.com/audio/ep1.mp3",
        duration=1800,
        published_at=datetime.now(timezone.utc),
        processed=True
    )
    test_session.add(episode)
    await test_session.commit()

    res = await test_session.execute(select(Episode).where(Episode.feed_id == feed.feed_id))
    episodes = res.scalars().all()
    assert len(episodes) == 1
    assert episodes[0].guid == "ep-001"
    assert episodes[0].feed.title == "Tech Talks"


@pytest.mark.asyncio
async def test_episode_unique_guid_per_feed(test_session: AsyncSession):
    feed = Feed(rss_url="https://example.com/feed2.xml", title="Podcast 2")
    test_session.add(feed)
    await test_session.commit()

    ep1 = Episode(feed_id=feed.feed_id, guid="unique-guid-1", title="Ep 1", audio_url="http://audio1.mp3")
    test_session.add(ep1)
    await test_session.commit()

    # Attempt inserting duplicate guid for same feed
    ep2 = Episode(feed_id=feed.feed_id, guid="unique-guid-1", title="Ep 1 Duplicate", audio_url="http://audio2.mp3")
    test_session.add(ep2)
    with pytest.raises(IntegrityError):
        await test_session.commit()


@pytest.mark.asyncio
async def test_overlength_title_and_audio_url_round_trip(test_session: AsyncSession):
    """XIN-47: titles/URLs past the old String caps persist without DataError."""
    feed = Feed(
        rss_url="https://example.com/" + "u" * 2000 + ".xml",
        title="F" * 2000,
    )
    test_session.add(feed)
    await test_session.commit()

    long_title = "T" * 2000
    long_url = "https://example.com/" + "a" * 3000 + ".mp3"
    ep = Episode(
        feed_id=feed.feed_id,
        guid="long-1",
        title=long_title,
        audio_url=long_url,
    )
    test_session.add(ep)
    await test_session.commit()

    res = await test_session.execute(
        select(Episode).where(Episode.episode_id == ep.episode_id)
    )
    fetched = res.scalar_one()
    assert fetched.title == long_title
    assert fetched.audio_url == long_url
    assert fetched.feed.title == "F" * 2000
    assert fetched.feed.rss_url == feed.rss_url


@pytest.mark.asyncio
async def test_guid_is_required(test_session: AsyncSession):
    """XIN-68: episodes.guid is NOT NULL — the (feed_id, guid) dedup holds."""
    feed = Feed(rss_url="https://example.com/guidreq.xml", title="Guid Req")
    test_session.add(feed)
    await test_session.commit()

    ep = Episode(
        feed_id=feed.feed_id,
        guid=None,
        title="No guid",
        audio_url="https://example.com/noguid.mp3",
    )
    test_session.add(ep)
    with pytest.raises(IntegrityError):
        await test_session.commit()
    await test_session.rollback()


@pytest.mark.asyncio
async def test_insights_and_tags(test_session: AsyncSession):
    feed = Feed(rss_url="https://example.com/insights_feed.xml", title="Insights Pod")
    test_session.add(feed)
    await test_session.commit()

    ep = Episode(feed_id=feed.feed_id, guid="ep-100", title="Ep 100", audio_url="http://audio.mp3")
    test_session.add(ep)
    await test_session.commit()

    insight = Insight(
        episode_id=ep.episode_id,
        timestamp_seconds=240,
        title="Quantum Computing breakthrough",
        detail="Detailed summary of takeaway",
        insight_type="takeaway"
    )
    tag = Tag(name="Quantum Computing", category="concept")
    test_session.add_all([insight, tag])
    await test_session.commit()

    ep_tag = EpisodeTag(episode_id=ep.episode_id, tag_id=tag.tag_id)
    test_session.add(ep_tag)
    await test_session.commit()

    res = await test_session.execute(select(Insight).where(Insight.episode_id == ep.episode_id))
    fetched_insights = res.scalars().all()
    assert len(fetched_insights) == 1
    assert fetched_insights[0].title == "Quantum Computing breakthrough"


@pytest.mark.asyncio
async def test_playlist_and_progress(test_session: AsyncSession):
    user = User(email="listener@example.com")
    feed = Feed(rss_url="https://example.com/playlist_feed.xml", title="Playlist Pod")
    test_session.add_all([user, feed])
    await test_session.commit()

    ep = Episode(feed_id=feed.feed_id, guid="ep-200", title="Ep 200", audio_url="http://audio.mp3")
    test_session.add(ep)
    await test_session.commit()

    progress = UserEpisodeProgress(
        user_id=user.user_id,
        episode_id=ep.episode_id,
        position_seconds=450,
        completed=False
    )

    playlist = CuratedPlaylist(
        user_id=user.user_id,
        title="AI Favorites",
        description="Top AI episodes",
        query_prompt="Find AI podcast episodes"
    )
    test_session.add_all([progress, playlist])
    await test_session.commit()

    playlist_ep = PlaylistEpisode(playlist_id=playlist.playlist_id, episode_id=ep.episode_id, position=1)
    test_session.add(playlist_ep)
    await test_session.commit()

    res_prog = await test_session.execute(
        select(UserEpisodeProgress).where(
            UserEpisodeProgress.user_id == user.user_id,
            UserEpisodeProgress.episode_id == ep.episode_id
        )
    )
    fetched_prog = res_prog.scalar_one_or_none()
    assert fetched_prog is not None
    assert fetched_prog.position_seconds == 450


@pytest.mark.asyncio
async def test_task_log(test_session: AsyncSession):
    log = TaskLog(
        task_type="PROCESS_EPISODE",
        payload_json='{"episode_id": "abc"}',
        status="completed"
    )
    test_session.add(log)
    await test_session.commit()

    res = await test_session.execute(select(TaskLog).where(TaskLog.task_log_id == log.task_log_id))
    fetched_log = res.scalar_one_or_none()
    assert fetched_log is not None
    assert fetched_log.task_type == "PROCESS_EPISODE"


@pytest.mark.asyncio
async def test_init_db_migrates_fresh_database(tmp_path, monkeypatch):
    """XIN-32: init_db() on an empty file creates the schema via Alembic.

    XIN-78: the engine is swapped via monkeypatch.setattr (auto-restored
    at teardown) instead of importlib.reload — reloading while
    monkeypatch's DATABASE_URL override is active re-binds the module to
    the tmp URL rather than the true default, and leaks an undisposed
    engine. monkeypatch restores the original binding for us.
    """
    import sqlite3

    import backend.persistence.database as db

    db_path = tmp_path / "fresh.db"
    # init_db() drives Alembic through env.py, which builds its own engine
    # from DATABASE_URL at migration time; the module-level engine is used
    # only for the pre-migration inspection step.
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setattr(db, "engine", engine)
    try:
        await db.init_db()
    finally:
        await engine.dispose()

    con = sqlite3.connect(db_path)
    tables = {r[0] for r in con.execute("select name from sqlite_master where type='table'")}
    assert {"feeds", "episodes", "alembic_version"} <= tables
    version = con.execute("select version_num from alembic_version").fetchone()[0]
    assert version  # stamped at head
    con.close()


@pytest.mark.asyncio
async def test_init_db_stamps_legacy_create_all_database(tmp_path, monkeypatch):
    """XIN-32: init_db() stamps (not migrates) a DB built by the old create_all path.

    XIN-78: same setattr-not-reload isolation as the test above.
    """
    import sqlite3

    import backend.persistence.database as db

    db_path = tmp_path / "legacy.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setattr(db, "engine", engine)
    try:
        await db.create_all_tables()
        await db.init_db()  # must not fail: stamp instead of migrate
        await db.init_db()  # idempotent re-run
    finally:
        await engine.dispose()

    con = sqlite3.connect(db_path)
    version = con.execute("select version_num from alembic_version").fetchone()[0]
    assert version
    con.close()
