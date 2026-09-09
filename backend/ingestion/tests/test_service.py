import json
from pathlib import Path
import pytest
import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from backend.ingestion.itunes import ITunesSearchClient
from backend.ingestion.models import Podcast
from backend.ingestion.parser import PodcastFeedParser
from backend.ingestion.service import FeedIngestionService
from backend.persistence.models.base import Base
from backend.persistence.models.episode import Episode
from backend.persistence.models.feed import Feed

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_feed_xml() -> str:
    with open(FIXTURES_DIR / "sample_feed.xml", "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def itunes_search_json() -> str:
    with open(FIXTURES_DIR / "itunes_search.json", "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
async def in_memory_session():
    """Async session with SQLite in-memory db."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session

    await engine.dispose()


class TestFeedIngestionModes:
    @pytest.mark.asyncio
    async def test_mode1_save_podcast_immediately(self, in_memory_session: AsyncSession):
        """Mode 1: Save discovered podcast show immediately to feeds table without downloading episodes."""
        service = FeedIngestionService()
        podcast = Podcast(
            title="Huberman Lab",
            feed_url="https://feeds.megaphone.fm/hubermanlab",
            author="Scicomm Media",
            description="Science-based tools for everyday life.",
            artwork_url="https://example.com/art.jpg",
            primary_genre="Health & Fitness",
            provider="itunes",
            itunes_id=1545953110,
        )

        feed = await service.save_podcast(in_memory_session, podcast)

        assert feed.feed_id is not None
        assert feed.title == "Huberman Lab"
        assert feed.author == "Scicomm Media"
        assert feed.rss_url == "https://feeds.megaphone.fm/hubermanlab"
        assert feed.sync_status == "discovered"
        assert feed.last_fetched_at is None

        # Verify persisted in database
        res = await in_memory_session.execute(select(Feed).where(Feed.rss_url == podcast.feed_url))
        persisted_feed = res.scalar_one_or_none()
        assert persisted_feed is not None
        assert persisted_feed.feed_id == feed.feed_id

    @pytest.mark.asyncio
    async def test_mode1_discover_and_save_podcasts(
        self, in_memory_session: AsyncSession, itunes_search_json: str
    ):
        """Mode 1: Discovers podcasts and saves all shows immediately to database."""
        def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code=200, text=itunes_search_json)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            service = FeedIngestionService()
            saved_feeds = await service.discover_and_save_podcasts(
                db=in_memory_session,
                query="huberman",
                client=client,
            )

            # Fixture contains 2 valid shows with feedUrl
            assert len(saved_feeds) == 2
            assert saved_feeds[0].title == "Huberman Lab"
            assert saved_feeds[1].title == "Lex Fridman Podcast"

            # Check DB count
            res = await in_memory_session.execute(select(Feed))
            all_feeds = res.scalars().all()
            assert len(all_feeds) == 2

    @pytest.mark.asyncio
    async def test_mode2_sync_podcast_episodes(
        self, in_memory_session: AsyncSession, sample_feed_xml: str
    ):
        """Mode 2: Given a saved podcast show, downloads and saves its episodes for LLM reading."""
        # 1. First save podcast show
        service = FeedIngestionService()
        podcast = Podcast(
            title="AI Frontier Podcast",
            feed_url="https://aifrontier.example.com/feed.xml",
            author="Frontier Labs",
        )
        feed = await service.save_podcast(in_memory_session, podcast)

        def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code=200, text=sample_feed_xml)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            # 2. Sync episodes for this specific podcast
            synced_feed, new_episodes = await service.sync_podcast_episodes(
                db=in_memory_session,
                feed_or_id_or_url=feed.feed_id,
                client=client,
            )

            assert synced_feed.sync_status == "active"
            assert synced_feed.last_fetched_at is not None
            assert len(new_episodes) == 3

            # Verify all episodes stored with processed=False for LLM
            for ep in new_episodes:
                assert ep.processed is False
                assert ep.audio_url.startswith("https://")
                assert ep.guid is not None

            # Verify persisted in database
            ep_res = await in_memory_session.execute(
                select(Episode).where(Episode.feed_id == feed.feed_id)
            )
            persisted_eps = ep_res.scalars().all()
            assert len(persisted_eps) == 3

    @pytest.mark.asyncio
    async def test_mode3_full_pipeline_ingest_podcast(
        self, in_memory_session: AsyncSession, sample_feed_xml: str
    ):
        """Mode 3: Discover/register podcast and immediately download its episodes."""
        def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code=200, text=sample_feed_xml)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            service = FeedIngestionService()
            podcast = Podcast(
                title="AI Frontier",
                feed_url="https://aifrontier.example.com/feed.xml",
            )
            feed, episodes = await service.ingest_podcast(
                db=in_memory_session,
                podcast=podcast,
                client=client,
                auto_sync_episodes=True,
            )

            assert feed.sync_status == "active"
            assert len(episodes) == 3

    @pytest.mark.asyncio
    async def test_mode4_batch_sync_all_pending_feeds(
        self, in_memory_session: AsyncSession, sample_feed_xml: str
    ):
        """Mode 4: Batch sync all discovered/pending feeds across the database."""
        service = FeedIngestionService()

        # Seed 2 pending feeds
        p1 = Podcast(title="Show 1", feed_url="https://aifrontier.example.com/feed1.xml")
        p2 = Podcast(title="Show 2", feed_url="https://aifrontier.example.com/feed2.xml")
        await service.save_podcast(in_memory_session, p1)
        await service.save_podcast(in_memory_session, p2)

        def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code=200, text=sample_feed_xml)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            stats = await service.sync_all_pending_feeds(
                db=in_memory_session,
                client=client,
            )

            assert stats["total_feeds_processed"] == 2
            assert stats["total_synced"] == 2
            assert stats["total_episodes_saved"] == 6
            assert stats["failed_count"] == 0

    @pytest.mark.asyncio
    async def test_llm_query_and_mark_processed(
        self, in_memory_session: AsyncSession, sample_feed_xml: str
    ):
        """Verify helper queries for retrieving unprocessed episodes for LLM and marking them done."""
        def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code=200, text=sample_feed_xml)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            service = FeedIngestionService()
            feed, episodes = await service.ingest_feed(
                db=in_memory_session,
                rss_url="https://aifrontier.example.com/feed.xml",
                client=client,
            )

            # Query unprocessed episodes for LLM
            unprocessed = await service.get_unprocessed_episodes(
                db=in_memory_session,
                feed_id=feed.feed_id,
            )
            assert len(unprocessed) == 3

            # Mark first episode as processed by LLM
            ep1 = unprocessed[0]
            updated_ep = await service.mark_episode_processed(
                db=in_memory_session,
                episode_id=ep1.episode_id,
                processed=True,
            )
            assert updated_ep is not None
            assert updated_ep.processed is True

            # Remaining unprocessed should now be 2
            remaining = await service.get_unprocessed_episodes(
                db=in_memory_session,
                feed_id=feed.feed_id,
            )
            assert len(remaining) == 2

    @pytest.mark.asyncio
    async def test_settings_database_selection(self):
        """Verify the settings dispatcher selects SimpleDB by default and sessions work."""
        from backend import settings

        assert settings.get_database_backend() == "simple"
        assert "simple.db" in settings.describe_database()

        await settings.init_db()
        async with settings.session_scope() as session:
            res = await session.execute(select(Feed))
            assert isinstance(res.scalars().all(), list)

    def test_settings_env_override(self, monkeypatch):
        """DATABASE_BACKEND env var overrides the settings.yaml value."""
        from backend import settings

        monkeypatch.setenv("DATABASE_BACKEND", "app")
        assert settings.get_database_backend() == "app"
        assert "App database" in settings.describe_database()

        monkeypatch.setenv("DATABASE_BACKEND", "bogus")
        with pytest.raises(ValueError, match="Unknown database backend"):
            settings.get_database_backend()

