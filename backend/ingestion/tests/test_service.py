import json
import uuid as uuid_lib
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest
import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from backend.ingestion.itunes import ITunesSearchClient
from backend.ingestion.models import Podcast
from backend.ingestion.parser import PodcastFeedParser
from backend.ingestion.service import FeedIngestionService, _error_retry_due
from backend.ingestion.task_queue.local import LocalInMemoryDriver
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
        """Verify the settings dispatcher selects SimpleDB by default and stores work."""
        import settings

        assert settings.get_database_backend() == "simple"
        assert "simple.db" in settings.describe_database()

        await settings.init_db()
        async with settings.session_scope() as store:
            # session_scope() yields a Store (no ad-hoc SQL in application code).
            assert await store.feeds.count_all() == 0

    @pytest.mark.asyncio
    async def test_settings_open_store_dynamodb(self, monkeypatch):
        """open_store() builds a DynamoDBStore from settings + env overrides."""
        import settings

        monkeypatch.setenv("DATABASE_BACKEND", "dynamodb")
        monkeypatch.setenv("DATABASE_DYNAMODB_TABLE_NAME", "test-table")
        monkeypatch.setenv("DATABASE_DYNAMODB_REGION", "eu-west-1")
        monkeypatch.setenv(
            "DATABASE_DYNAMODB_ENDPOINT_URL", "http://localhost:8000"
        )

        assert settings.get_database_backend() == "dynamodb"
        cfg = settings.get_dynamodb_config()
        assert cfg == {
            "table_name": "test-table",
            "region": "eu-west-1",
            "endpoint_url": "http://localhost:8000",
        }
        assert "test-table" in settings.describe_database()
        assert "localhost:8000" in settings.describe_database()

        from backend.persistence.dynamodb.store import DynamoDBStore

        # The dynamodb backend requires `async with`: entering the store
        # connects the aioboto3 client context.
        async with settings.open_store() as store:
            assert isinstance(store, DynamoDBStore)
            assert store.table_name == "test-table"
            # The entered client is a real (usable) client object.
            assert hasattr(store._client, "get_item")

        # Explicit backend argument overrides the configured one.
        sql_store = settings.open_store("simple")
        try:
            from backend.persistence.sqlalchemy_store import SQLAlchemyStore

            assert isinstance(sql_store, SQLAlchemyStore)
        finally:
            await sql_store.close()

    def test_settings_dynamodb_defaults(self, monkeypatch):
        """DynamoDB settings fall back to built-in defaults."""
        import settings

        monkeypatch.setenv("DATABASE_BACKEND", "dynamodb")
        for var in (
            "DATABASE_DYNAMODB_TABLE_NAME",
            "DATABASE_DYNAMODB_REGION",
            "DATABASE_DYNAMODB_ENDPOINT_URL",
        ):
            monkeypatch.delenv(var, raising=False)
        cfg = settings.get_dynamodb_config()
        assert cfg["table_name"] == "tunedin"
        assert cfg["region"] == "us-east-1"
        assert cfg["endpoint_url"] is None

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


# ---------------------------------------------------------------------------
# XIN-126 / XIN-127 / XIN-128: fetch-error handling, write resilience, inputs
# ---------------------------------------------------------------------------


def _mock_client(xml_body: str, status_code: int = 200) -> httpx.AsyncClient:
    def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code=status_code, text=xml_body)

    return httpx.AsyncClient(transport=httpx.MockTransport(mock_handler))


class TestSyncFetchErrorPaths:
    """XIN-126: malformed XML routes through the fetch-error path."""

    @pytest.mark.asyncio
    async def test_malformed_xml_marks_feed_error_title_preserved(
        self, in_memory_store, sample_feed_xml: str
    ):
        service = FeedIngestionService()
        podcast = Podcast(title="Real Title", feed_url="https://real.example.com/feed.xml")
        feed = await service.save_podcast(in_memory_store, podcast)

        client = _mock_client("<html><body>not a feed</body></html>")
        async with client:
            with pytest.raises(ValueError):
                await service.sync_podcast_episodes(
                    in_memory_store, feed, client=client
                )

        refreshed = await in_memory_store.feeds.get_by_id(feed.feed_id)
        assert refreshed.sync_status == "error"
        assert refreshed.error_count == 1
        assert refreshed.last_fetched_at is not None
        # The real title must not be overwritten with "Untitled Podcast"
        assert refreshed.title == "Real Title"

    @pytest.mark.asyncio
    async def test_sync_fetch_failure_marks_error_and_reraises(
        self, in_memory_store
    ):
        service = FeedIngestionService()
        podcast = Podcast(title="T", feed_url="https://boom.example.com/feed.xml")
        feed = await service.save_podcast(in_memory_store, podcast)

        def mock_handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("down", request=request)

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler)
        ) as client:
            with pytest.raises(httpx.ConnectError):
                await service.sync_podcast_episodes(
                    in_memory_store, feed, client=client
                )

        refreshed = await in_memory_store.feeds.get_by_id(feed.feed_id)
        assert refreshed.sync_status == "error"
        assert refreshed.error_count == 1

    @pytest.mark.asyncio
    async def test_sync_304_preserves_metadata_returns_empty(
        self, in_memory_store
    ):
        service = FeedIngestionService()
        podcast = Podcast(title="Real Title", feed_url="https://ok.example.com/feed.xml")
        feed = await service.save_podcast(in_memory_store, podcast)

        def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code=304)

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler)
        ) as client:
            synced_feed, new_eps = await service.sync_podcast_episodes(
                in_memory_store, feed, client=client
            )

        assert new_eps == []
        assert synced_feed.title == "Real Title"
        assert synced_feed.sync_status == "active"
        assert synced_feed.error_count == 0


class TestSyncInputVariants:
    """XIN-128: Feed / UUID / URL-string inputs; invalid identifiers rejected."""

    @pytest.mark.asyncio
    async def test_sync_with_feed_object_input(
        self, in_memory_store, sample_feed_xml: str
    ):
        service = FeedIngestionService()
        feed = await service.save_podcast(
            in_memory_store,
            Podcast(title="T", feed_url="https://aifrontier.example.com/feed.xml"),
        )
        client = _mock_client(sample_feed_xml)
        async with client:
            synced, new_eps = await service.sync_podcast_episodes(
                in_memory_store, feed, client=client
            )
        assert len(new_eps) == 3
        assert synced.feed_id == feed.feed_id

    @pytest.mark.asyncio
    async def test_sync_with_uuid_object_input(
        self, in_memory_store, sample_feed_xml: str
    ):
        service = FeedIngestionService()
        feed = await service.save_podcast(
            in_memory_store,
            Podcast(title="T", feed_url="https://aifrontier.example.com/feed.xml"),
        )
        client = _mock_client(sample_feed_xml)
        async with client:
            synced, new_eps = await service.sync_podcast_episodes(
                in_memory_store, feed.feed_id, client=client
            )
        assert len(new_eps) == 3
        assert synced.feed_id == feed.feed_id

    @pytest.mark.asyncio
    async def test_sync_with_uuid_string_input(
        self, in_memory_store, sample_feed_xml: str
    ):
        service = FeedIngestionService()
        feed = await service.save_podcast(
            in_memory_store,
            Podcast(title="T", feed_url="https://aifrontier.example.com/feed.xml"),
        )
        client = _mock_client(sample_feed_xml)
        async with client:
            synced, new_eps = await service.sync_podcast_episodes(
                in_memory_store, str(feed.feed_id), client=client
            )
        assert len(new_eps) == 3

    @pytest.mark.asyncio
    async def test_sync_with_new_rss_url_string_creates_then_syncs(
        self, in_memory_store, sample_feed_xml: str
    ):
        service = FeedIngestionService()
        client = _mock_client(sample_feed_xml)
        async with client:
            synced, new_eps = await service.sync_podcast_episodes(
                in_memory_store,
                "https://aifrontier.example.com/feed.xml",
                client=client,
            )
        assert len(new_eps) == 3
        assert synced.title == "AI Frontier Podcast"
        assert synced.sync_status == "active"

    @pytest.mark.asyncio
    async def test_sync_uuid_string_not_found_raises_valueerror(
        self, in_memory_store
    ):
        service = FeedIngestionService()
        with pytest.raises(ValueError):
            await service.sync_podcast_episodes(
                in_memory_store, str(uuid_lib.uuid4())
            )

    @pytest.mark.asyncio
    async def test_sync_invalid_identifier_raises_without_creating_feed(
        self, in_memory_store
    ):
        """XIN-128: an invalid string identifier must not create a Feed row."""
        service = FeedIngestionService()
        with pytest.raises(ValueError, match="Invalid feed identifier"):
            await service.sync_podcast_episodes(in_memory_store, "not-a-url")
        assert await in_memory_store.feeds.count_all() == 0

    @pytest.mark.asyncio
    async def test_sync_invalid_type_raises_typeerror(self, in_memory_store):
        service = FeedIngestionService()
        with pytest.raises(TypeError):
            await service.sync_podcast_episodes(in_memory_store, 123)


class TestSavePodcastResilience:
    """XIN-127: duplicate RSS URLs and mid-write IntegrityErrors."""

    @pytest.mark.asyncio
    async def test_save_podcast_updates_existing(self, in_memory_store):
        service = FeedIngestionService()
        feed1 = await service.save_podcast(
            in_memory_store,
            Podcast(title="Old Title", feed_url="https://dup.example.com/feed.xml"),
        )
        feed2 = await service.save_podcast(
            in_memory_store,
            Podcast(title="New Title", feed_url="https://dup.example.com/feed.xml"),
        )
        assert feed2.feed_id == feed1.feed_id
        assert feed2.title == "New Title"
        assert await in_memory_store.feeds.count_all() == 1

    @pytest.mark.asyncio
    async def test_save_podcast_missing_feed_url_raises(self, in_memory_store):
        service = FeedIngestionService()
        with pytest.raises(ValueError, match="feed_url"):
            await service.save_podcast(
                in_memory_store, Podcast(title="No URL", feed_url="")
            )

    @pytest.mark.asyncio
    async def test_save_podcast_duplicate_race_is_resilient(
        self, in_memory_store, monkeypatch
    ):
        """Simulates a concurrent insert winning the race after our read."""
        service = FeedIngestionService()
        podcast = Podcast(title="Race", feed_url="https://race.example.com/feed.xml")
        feed1 = await service.save_podcast(in_memory_store, podcast)

        orig_get = in_memory_store.feeds.get_by_rss_url
        calls = 0

        async def flaky_get(url):
            nonlocal calls
            calls += 1
            if calls == 1:
                return None  # stale read: another writer inserted first
            return await orig_get(url)

        monkeypatch.setattr(in_memory_store.feeds, "get_by_rss_url", flaky_get)

        feed2 = await service.save_podcast(in_memory_store, podcast)
        assert feed2.feed_id == feed1.feed_id
        assert await in_memory_store.feeds.count_all() == 1

    @pytest.mark.asyncio
    async def test_save_podcasts_batch_is_resilient(self, in_memory_store):
        service = FeedIngestionService()
        podcasts = [
            Podcast(title=f"Show {i}", feed_url=f"https://batch{i}.example.com/feed.xml")
            for i in range(3)
        ]
        feeds = await service.save_podcasts(in_memory_store, podcasts)
        assert len(feeds) == 3
        # Saving the same batch again updates in place without duplicates
        feeds2 = await service.save_podcasts(in_memory_store, podcasts)
        assert len(feeds2) == 3
        assert await in_memory_store.feeds.count_all() == 3


class TestBatchSyncResilience:
    """XIN-127: per-feed failures roll back and the batch continues."""

    @pytest.mark.asyncio
    async def test_per_feed_failure_marks_error_and_continues(
        self, in_memory_store, monkeypatch
    ):
        service = FeedIngestionService()
        feed1 = await service.save_podcast(
            in_memory_store,
            Podcast(title="One", feed_url="https://one.example.com/feed.xml"),
        )
        feed2 = await service.save_podcast(
            in_memory_store,
            Podcast(title="Two", feed_url="https://two.example.com/feed.xml"),
        )

        async def fake_sync(store, feed_or_id_or_url, client=None, auto_queue_episodes=0):
            if "one.example.com" in feed_or_id_or_url.rss_url:
                raise RuntimeError("write boom")
            return feed_or_id_or_url, []

        monkeypatch.setattr(service, "sync_podcast_episodes", fake_sync)

        result = await service.sync_all_pending_feeds(in_memory_store)

        assert result["failed_count"] == 1
        assert result["failed_feed_ids"] == [str(feed1.feed_id)]
        assert result["total_synced"] == 1

        failed = await in_memory_store.feeds.get_by_id(feed1.feed_id)
        assert failed.sync_status == "error"
        assert failed.error_count == 1
        assert failed.last_fetched_at is not None

        ok = await in_memory_store.feeds.get_by_id(feed2.feed_id)
        assert ok.sync_status == "discovered"  # untouched by the failing feed

    @pytest.mark.asyncio
    async def test_sync_all_pending_feeds_max_feeds_cap(
        self, in_memory_store, monkeypatch
    ):
        service = FeedIngestionService()
        for i in range(3):
            await service.save_podcast(
                in_memory_store,
                Podcast(title=f"S{i}", feed_url=f"https://cap{i}.example.com/feed.xml"),
            )

        async def fake_sync(store, feed_or_id_or_url, client=None, auto_queue_episodes=0):
            return feed_or_id_or_url, []

        monkeypatch.setattr(service, "sync_podcast_episodes", fake_sync)
        result = await service.sync_all_pending_feeds(in_memory_store, max_feeds=2)
        assert result["total_feeds_processed"] == 2
        assert result["total_synced"] == 2

    def test_error_retry_due_naive_datetime(self):
        """SQLite returns naive datetimes; the guard must not crash (XIN-128)."""
        feed = Feed(
            rss_url="https://x.example.com/f.xml",
            title="T",
            sync_status="error",
            error_count=1,
            last_fetched_at=datetime(2026, 1, 1),  # naive
        )
        assert _error_retry_due(feed, datetime.now(timezone.utc)) is True

    @pytest.mark.asyncio
    async def test_list_error_due_retry_includes_null_last_fetched_at(
        self, in_memory_store
    ):
        """XIN-128: error rows with NULL last_fetched_at must be retried."""
        feed = Feed(
            rss_url="https://nullts.example.com/f.xml",
            title="NullTs",
            sync_status="error",
            error_count=1,
            last_fetched_at=None,
        )
        await in_memory_store.feeds.save(feed)
        await in_memory_store.commit()

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=300)
        rows = await in_memory_store.feeds.list_error_due_retry(cutoff, 10)
        assert feed.feed_id in [f.feed_id for f in rows]


class TestQueueBranches:
    """Queue driver wiring: auto_queue_episodes and driver presence."""

    @pytest.mark.asyncio
    async def test_auto_queue_enqueues_tasks(
        self, in_memory_store, sample_feed_xml: str
    ):
        driver = LocalInMemoryDriver()
        service = FeedIngestionService(queue_driver=driver)
        feed = await service.save_podcast(
            in_memory_store,
            Podcast(title="T", feed_url="https://aifrontier.example.com/feed.xml"),
        )
        client = _mock_client(sample_feed_xml)
        async with client:
            _, new_eps = await service.sync_podcast_episodes(
                in_memory_store, feed, client=client, auto_queue_episodes=2
            )
        assert len(new_eps) == 3
        assert len(driver.tasks) == 2
        assert all(t.task_type == "PROCESS_EPISODE" for t in driver.tasks)

    @pytest.mark.asyncio
    async def test_auto_queue_zero_enqueues_nothing(
        self, in_memory_store, sample_feed_xml: str
    ):
        driver = LocalInMemoryDriver()
        service = FeedIngestionService(queue_driver=driver)
        feed = await service.save_podcast(
            in_memory_store,
            Podcast(title="T", feed_url="https://aifrontier.example.com/feed.xml"),
        )
        client = _mock_client(sample_feed_xml)
        async with client:
            await service.sync_podcast_episodes(
                in_memory_store, feed, client=client, auto_queue_episodes=0
            )
        assert driver.tasks == []

    @pytest.mark.asyncio
    async def test_no_driver_enqueues_nothing(
        self, in_memory_store, sample_feed_xml: str
    ):
        service = FeedIngestionService(queue_driver=None)
        feed = await service.save_podcast(
            in_memory_store,
            Podcast(title="T", feed_url="https://aifrontier.example.com/feed.xml"),
        )
        client = _mock_client(sample_feed_xml)
        async with client:
            _, new_eps = await service.sync_podcast_episodes(
                in_memory_store, feed, client=client, auto_queue_episodes=5
            )
        assert len(new_eps) == 3  # episodes still saved; just not queued


class TestIngestPodcastVariants:
    @pytest.mark.asyncio
    async def test_ingest_podcast_no_auto_sync(self, in_memory_store):
        service = FeedIngestionService()
        feed, new_eps = await service.ingest_podcast(
            in_memory_store,
            Podcast(title="NoSync", feed_url="https://nosync.example.com/feed.xml"),
            auto_sync_episodes=False,
        )
        assert feed.title == "NoSync"
        assert new_eps == []

    @pytest.mark.asyncio
    async def test_discover_top_and_save_podcasts(self, in_memory_store, monkeypatch):
        service = FeedIngestionService()

        async def fake_top(limit=25, country="US", client=None):
            return [
                Podcast(title="Top A", feed_url="https://topa.example.com/feed.xml"),
                Podcast(title="Top B", feed_url="https://topb.example.com/feed.xml"),
            ]

        monkeypatch.setattr(service.itunes_client, "get_top_podcasts", fake_top)
        feeds = await service.discover_top_and_save_podcasts(
            in_memory_store, limit=2, country="US"
        )
        assert len(feeds) == 2
        assert {f.title for f in feeds} == {"Top A", "Top B"}
