import json
from pathlib import Path
import pytest
import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from backend.ingestion.itunes import ITunesSearchClient
from backend.ingestion.models import Podcast
from backend.ingestion.parser import PodcastFeedParser
from backend.ingestion.service import FeedIngestionService
from backend.persistence.models.base import Base
from backend.persistence.models.feed import Feed
from backend.persistence.repositories import Store
from backend.persistence.sqlalchemy_store import SQLAlchemyStore

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
async def in_memory_store():
    """SQLAlchemyStore backed by an in-memory SQLite db."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    store = SQLAlchemyStore(session_factory)
    yield store

    await store.close()
    await engine.dispose()


class TestFeedIngestionModes:
    @pytest.mark.asyncio
    async def test_mode1_save_podcast_immediately(self, in_memory_store: Store):
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

        feed = await service.save_podcast(in_memory_store, podcast)

        assert feed.feed_id is not None
        assert feed.title == "Huberman Lab"
        assert feed.author == "Scicomm Media"
        assert feed.rss_url == "https://feeds.megaphone.fm/hubermanlab"
        assert feed.sync_status == "discovered"
        assert feed.last_fetched_at is None

        # Verify persisted in database
        persisted_feed = await in_memory_store.feeds.get_by_rss_url(podcast.feed_url)
        assert persisted_feed is not None
        assert persisted_feed.feed_id == feed.feed_id

    @pytest.mark.asyncio
    async def test_mode1_discover_and_save_podcasts(
        self, in_memory_store: Store, itunes_search_json: str
    ):
        """Mode 1: Discovers podcasts and saves all shows immediately to database."""
        def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code=200, text=itunes_search_json)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            service = FeedIngestionService()
            saved_feeds = await service.discover_and_save_podcasts(
                store=in_memory_store,
                query="huberman",
                client=client,
            )

            # Fixture contains 2 valid shows with feedUrl
            assert len(saved_feeds) == 2
            assert saved_feeds[0].title == "Huberman Lab"
            assert saved_feeds[1].title == "Lex Fridman Podcast"

            # Check DB count
            all_feeds = await in_memory_store.feeds.list_all()
            assert len(all_feeds) == 2

    @pytest.mark.asyncio
    async def test_mode2_sync_podcast_episodes(
        self, in_memory_store: Store, sample_feed_xml: str
    ):
        """Mode 2: Given a saved podcast show, downloads and saves its episodes for LLM reading."""
        # 1. First save podcast show
        service = FeedIngestionService()
        podcast = Podcast(
            title="AI Frontier Podcast",
            feed_url="https://aifrontier.example.com/feed.xml",
            author="Frontier Labs",
        )
        feed = await service.save_podcast(in_memory_store, podcast)

        def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code=200, text=sample_feed_xml)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            # 2. Sync episodes for this specific podcast
            synced_feed, new_episodes = await service.sync_podcast_episodes(
                store=in_memory_store,
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
            persisted_eps = await in_memory_store.episodes.list_episodes_by_feed(feed.feed_id)
            assert len(persisted_eps) == 3

    @pytest.mark.asyncio
    async def test_mode3_full_pipeline_ingest_podcast(
        self, in_memory_store: Store, sample_feed_xml: str
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
                store=in_memory_store,
                podcast=podcast,
                client=client,
                auto_sync_episodes=True,
            )

            assert feed.sync_status == "active"
            assert len(episodes) == 3

    @pytest.mark.asyncio
    async def test_mode4_batch_sync_all_pending_feeds(
        self, in_memory_store: Store, sample_feed_xml: str
    ):
        """Mode 4: Batch sync all discovered/pending feeds across the database."""
        service = FeedIngestionService()

        # Seed 2 pending feeds
        p1 = Podcast(title="Show 1", feed_url="https://aifrontier.example.com/feed1.xml")
        p2 = Podcast(title="Show 2", feed_url="https://aifrontier.example.com/feed2.xml")
        await service.save_podcast(in_memory_store, p1)
        await service.save_podcast(in_memory_store, p2)

        def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code=200, text=sample_feed_xml)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            stats = await service.sync_all_pending_feeds(
                store=in_memory_store,
                client=client,
            )

            assert stats["total_feeds_processed"] == 2
            assert stats["total_synced"] == 2
            assert stats["total_episodes_saved"] == 6
            assert stats["failed_count"] == 0

    @pytest.mark.asyncio
    async def test_llm_query_and_mark_processed(
        self, in_memory_store: Store, sample_feed_xml: str
    ):
        """Verify helper queries for retrieving unprocessed episodes for LLM and marking them done."""
        def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code=200, text=sample_feed_xml)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            service = FeedIngestionService()
            feed, episodes = await service.ingest_feed(
                store=in_memory_store,
                rss_url="https://aifrontier.example.com/feed.xml",
                client=client,
            )

            # Query unprocessed episodes for LLM
            unprocessed = await service.get_unprocessed_episodes(
                store=in_memory_store,
                feed_id=feed.feed_id,
            )
            assert len(unprocessed) == 3

            # Mark first episode as processed by LLM
            ep1 = unprocessed[0]
            updated_ep = await service.mark_episode_processed(
                store=in_memory_store,
                episode_id=ep1.episode_id,
                processed=True,
            )
            assert updated_ep is not None
            assert updated_ep.processed is True

            # Remaining unprocessed should now be 2
            remaining = await service.get_unprocessed_episodes(
                store=in_memory_store,
                feed_id=feed.feed_id,
            )
            assert len(remaining) == 2

    @pytest.mark.asyncio
    async def test_settings_database_selection(self):
        """Verify the settings dispatcher selects SimpleDB by default and sessions work."""
        import settings
        from sqlalchemy import select

        assert settings.get_database_backend() == "simple"
        assert "simple.db" in settings.describe_database()

        await settings.init_db()
        async with settings.session_scope() as session:
            res = await session.execute(select(Feed))
            assert isinstance(res.scalars().all(), list)

    def test_settings_env_override(self, monkeypatch):
        """DATABASE_BACKEND env var overrides the settings.yaml value."""
        import settings

        monkeypatch.setenv("DATABASE_BACKEND", "app")
        assert settings.get_database_backend() == "app"
        assert "App database" in settings.describe_database()

        monkeypatch.setenv("DATABASE_BACKEND", "bogus")
        with pytest.raises(ValueError, match="Unknown database backend"):
            settings.get_database_backend()



class TestSettingsYamlLoading:
    """Settings loaded from a real settings.yaml on disk (tmp_path)."""

    @staticmethod
    def _use_yaml(monkeypatch, tmp_path, text):
        """Point settings at a tmp settings.yaml and clear its cache."""
        import settings

        yaml_file = tmp_path / "settings.yaml"
        yaml_file.write_text(text, encoding="utf-8")
        monkeypatch.setattr(settings, "SETTINGS_PATH", yaml_file)
        monkeypatch.setattr(settings, "_yaml_cache", None)
        return settings

    def test_loads_real_settings_yaml(self, tmp_path, monkeypatch):
        settings = self._use_yaml(
            monkeypatch,
            tmp_path,
            "database:\n"
            "  backend: simple\n"
            "  simple:\n"
            "    path: backend/ingestion/custom.db\n"
            "ingestion:\n"
            "  crawler_countries:\n"
            "    - us\n"
            "    - jp\n"
            "  auto_queue_episodes: 7\n",
        )
        assert settings.get_database_backend() == "simple"
        assert settings.get_settings()["database"]["simple"]["path"] == (
            "backend/ingestion/custom.db"
        )
        assert settings.get_crawler_countries() == ["us", "jp"]
        assert settings.get_auto_queue_episodes() == 7

    def test_null_section_keeps_defaults(self, tmp_path, monkeypatch):
        """`database:` with nothing under it must not crash; defaults survive."""
        settings = self._use_yaml(
            monkeypatch, tmp_path, "database:\ningestion:\n  auto_queue_episodes: 2\n"
        )
        assert settings.get_database_backend() == "simple"
        assert settings.get_settings()["database"]["simple"]["path"] == (
            "backend/ingestion/simple.db"
        )
        assert settings.get_auto_queue_episodes() == 2
        # Built-in default countries still apply when the key is absent.
        assert settings.get_crawler_countries() == ["us", "gb", "ca", "au", "de", "fr"]

    @pytest.mark.parametrize("bad", [-1, "many", 2.5, True, None, "", "--5", "5.5"])
    def test_auto_queue_episodes_rejects_bad_values(self, tmp_path, monkeypatch, bad):
        import yaml as pyyaml

        settings = self._use_yaml(
            monkeypatch,
            tmp_path,
            "ingestion:\n  auto_queue_episodes: " + pyyaml.safe_dump(bad, default_flow_style=True).strip() + "\n",
        )
        with pytest.raises(ValueError, match="auto_queue_episodes"):
            settings.get_auto_queue_episodes()

    @pytest.mark.parametrize("bad", [[], "us", [""], ["us", 42], None])
    def test_crawler_countries_rejects_bad_values(self, tmp_path, monkeypatch, bad):
        import yaml as pyyaml

        settings = self._use_yaml(
            monkeypatch,
            tmp_path,
            "ingestion:\n  crawler_countries: " + pyyaml.safe_dump(bad, default_flow_style=True).strip() + "\n",
        )
        with pytest.raises(ValueError, match="crawler_countries"):
            settings.get_crawler_countries()

    def test_describe_database_prefers_legacy_env_override(self, monkeypatch):
        """INGESTION_DATABASE_URL is what simple_db actually uses; name it."""
        import settings

        monkeypatch.setenv(
            "INGESTION_DATABASE_URL", "sqlite+aiosqlite:////tmp/legacy.db"
        )
        assert "legacy.db" in settings.describe_database()

    def test_describe_database_displays_url_path_as_is(self, tmp_path, monkeypatch):
        """A simple.path that is already a URL must not be path-resolved."""
        import settings

        self._use_yaml(
            monkeypatch,
            tmp_path,
            "database:\n"
            "  backend: simple\n"
            "  simple:\n"
            "    path: sqlite+aiosqlite:////tmp/remote.db\n",
        )
        monkeypatch.delenv("INGESTION_DATABASE_URL", raising=False)
        assert settings.describe_database() == (
            "SimpleDB (SQLite): sqlite+aiosqlite:////tmp/remote.db"
        )

    def test_invalid_backend_error_names_env_source(self, monkeypatch):
        import settings

        monkeypatch.setenv("DATABASE_BACKEND", "bogus")
        with pytest.raises(ValueError, match="DATABASE_BACKEND environment variable"):
            settings.get_database_backend()

    def test_apply_to_env_mirrors_yaml_path(self, tmp_path, monkeypatch):
        """The yaml simple.path is mirrored into INGESTION_DATABASE_URL (setdefault)."""
        import os

        import settings

        self._use_yaml(
            monkeypatch,
            tmp_path,
            "database:\n  backend: simple\n  simple:\n    path: backend/ingestion/custom.db\n",
        )
        monkeypatch.delenv("INGESTION_DATABASE_URL", raising=False)
        settings._apply_to_env()
        assert os.environ["INGESTION_DATABASE_URL"].endswith("custom.db")

        # An explicitly-set variable is never clobbered.
        monkeypatch.setenv("INGESTION_DATABASE_URL", "sqlite+aiosqlite:////tmp/keep.db")
        settings._apply_to_env()
        assert os.environ["INGESTION_DATABASE_URL"].endswith("keep.db")


class TestErrorRetryPolicy:
    """XIN-34: feeds stuck in sync_status='error' must be retried with backoff."""

    def _make_error_feed(self, error_count: int, last_fetched_ago_seconds: float) -> Feed:
        from datetime import datetime, timedelta, timezone

        return Feed(
            rss_url=f"https://example.com/error-feed-{error_count}-{last_fetched_ago_seconds}.xml",
            title="Error Feed",
            sync_status="error",
            error_count=error_count,
            last_fetched_at=datetime.now(timezone.utc) - timedelta(seconds=last_fetched_ago_seconds),
        )

    def _ok_parse_result(self) -> "FeedParseResult":
        from backend.ingestion.models import FeedParseResult, ParsedFeedMetadata

        return FeedParseResult(
            metadata=ParsedFeedMetadata(title="Recovered Podcast", rss_url="https://example.com/x.xml"),
            episodes=[],
            total_feed_episodes=0,
        )

    @pytest.mark.asyncio
    async def test_errored_feed_retried_after_backoff_elapses(self, in_memory_store: Store):
        """An errored feed past its backoff window is retried and recovers."""
        from unittest.mock import AsyncMock, patch

        from backend.ingestion import service as service_module

        feed = await in_memory_store.feeds.save(
            self._make_error_feed(error_count=1, last_fetched_ago_seconds=3600)
        )
        await in_memory_store.commit()

        svc = FeedIngestionService()
        with patch.object(
            PodcastFeedParser, "fetch_and_parse", new=AsyncMock(return_value=self._ok_parse_result())
        ):
            summary = await svc.sync_all_pending_feeds(in_memory_store)

        assert summary["total_synced"] == 1
        feed = await in_memory_store.feeds.get_by_id(feed.feed_id)
        assert feed.sync_status == "active"
        assert feed.error_count == 0
        assert service_module.ERROR_RETRY_MAX_ATTEMPTS > 1

    @pytest.mark.asyncio
    async def test_errored_feed_not_retried_within_backoff(self, in_memory_store: Store):
        """An errored feed inside its backoff window is left alone."""
        from unittest.mock import AsyncMock, patch

        feed = await in_memory_store.feeds.save(
            self._make_error_feed(error_count=1, last_fetched_ago_seconds=60)
        )
        await in_memory_store.commit()

        svc = FeedIngestionService()
        with patch.object(
            PodcastFeedParser, "fetch_and_parse", new=AsyncMock(return_value=self._ok_parse_result())
        ) as mock_fetch:
            summary = await svc.sync_all_pending_feeds(in_memory_store)

        mock_fetch.assert_not_called()
        assert summary["total_synced"] == 0
        feed = await in_memory_store.feeds.get_by_id(feed.feed_id)
        assert feed.sync_status == "error"

    @pytest.mark.asyncio
    async def test_errored_feed_abandoned_after_max_attempts(self, in_memory_store: Store):
        """An errored feed past the max attempt count is never retried."""
        from unittest.mock import AsyncMock, patch

        from backend.ingestion import service as service_module

        feed = await in_memory_store.feeds.save(
            self._make_error_feed(
                error_count=service_module.ERROR_RETRY_MAX_ATTEMPTS,
                last_fetched_ago_seconds=30 * 24 * 3600,
            )
        )
        await in_memory_store.commit()

        svc = FeedIngestionService()
        with patch.object(
            PodcastFeedParser, "fetch_and_parse", new=AsyncMock(return_value=self._ok_parse_result())
        ) as mock_fetch:
            summary = await svc.sync_all_pending_feeds(in_memory_store)

        mock_fetch.assert_not_called()
        assert summary["total_synced"] == 0
        feed = await in_memory_store.feeds.get_by_id(feed.feed_id)
        assert feed.sync_status == "error"
