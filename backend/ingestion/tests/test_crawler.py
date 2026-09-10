import json
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
import pytest
import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from backend.ingestion import crawler as crawler_module
from backend.ingestion.crawler import PodcastCrawler
from backend.ingestion.service import FeedIngestionService
from backend.persistence.models.base import Base
from backend.persistence.models.episode import Episode
from backend.persistence.models.feed import Feed
from backend.persistence.sqlalchemy_store import SQLAlchemyStore

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def itunes_search_json() -> str:
    with open(FIXTURES_DIR / "itunes_search.json", "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def sample_feed_xml() -> str:
    with open(FIXTURES_DIR / "sample_feed.xml", "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
async def in_memory_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session

    await engine.dispose()


class TestPodcastCrawler:
    @pytest.mark.asyncio
    async def test_crawl_topics_batches_and_persists(
        self, in_memory_session: AsyncSession, itunes_search_json: str
    ):
        def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code=200, text=itunes_search_json)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            crawler = PodcastCrawler(request_delay=0.0)
            stats = await crawler.crawl_topics(
                store=SQLAlchemyStore(lambda: in_memory_session),
                topics=["AI", "Neuroscience"],
                country="us",
                client=client,
            )

            assert stats["total_discovered"] == 4  # 2 per topic
            assert stats["unique_saved"] == 2      # Deduplicated across queries

            # Verify saved in feeds table
            res = await in_memory_session.execute(select(Feed))
            feeds = res.scalars().all()
            assert len(feeds) == 2
            assert all(f.sync_status == "discovered" for f in feeds)

    @pytest.mark.asyncio
    async def test_resolve_and_save_ids_in_chunks(
        self, in_memory_session: AsyncSession, itunes_search_json: str
    ):
        def mock_handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/lookup"
            return httpx.Response(status_code=200, text=itunes_search_json)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            crawler = PodcastCrawler(request_delay=0.0, batch_size=2)
            collection_ids = [1545953110, 1600000001, 1700000002, 1800000003]

            result = await crawler.resolve_and_save_ids(
                store=SQLAlchemyStore(lambda: in_memory_session),
                collection_ids=collection_ids,
                client=client,
            )

            assert len(result["saved_feeds"]) == 2
            assert result["failed_chunks"] == []

    @pytest.mark.asyncio
    async def test_crawl_top_charts_across_countries(
        self, in_memory_session: AsyncSession, itunes_search_json: str
    ):
        charts_response = {
            "feed": {
                "results": [
                    {"id": "1545953110", "name": "Huberman Lab"},
                    {"id": "1600000001", "name": "Lex Fridman Podcast"},
                ]
            }
        }
        lookup_data = json.loads(itunes_search_json)

        def mock_handler(request: httpx.Request) -> httpx.Response:
            if "applemarketingtools.com" in request.url.host:
                return httpx.Response(status_code=200, json=charts_response)
            elif "/lookup" in request.url.path:
                return httpx.Response(status_code=200, json=lookup_data)
            return httpx.Response(status_code=404)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            crawler = PodcastCrawler(request_delay=0.0)
            stats = await crawler.crawl_top_charts(
                store=SQLAlchemyStore(lambda: in_memory_session),
                countries=["us", "gb"],
                limit_per_chart=25,
                client=client,
            )

            assert stats["total_discovered"] == 4
            assert stats["unique_saved"] == 2
            assert stats["countries_crawled"] == ["us", "gb"]

    @pytest.mark.asyncio
    async def test_sync_episodes_concurrently_fetches_via_repositories(
        self, in_memory_session: AsyncSession, monkeypatch
    ):
        """sync_episodes_concurrently must fetch pending feeds through the
        repository layer (no raw select) and pass feed ids to the service."""
        async def _add_feed(rss_url: str, sync_status: str) -> Feed:
            feed = Feed(rss_url=rss_url, title=rss_url, sync_status=sync_status)
            in_memory_session.add(feed)
            await in_memory_session.flush()
            return feed

        pending1 = await _add_feed("https://example.com/a.xml", "pending")
        pending2 = await _add_feed("https://example.com/b.xml", "discovered")
        await _add_feed("https://example.com/c.xml", "active")
        await in_memory_session.commit()

        seen_ids = []

        class FakeService:
            itunes_client = None

            async def sync_podcast_episodes(self, *, store, feed_or_id_or_url, **kwargs):
                seen_ids.append(feed_or_id_or_url)
                return Feed(rss_url="x", title="Synced"), ["ep1", "ep2"]

        @asynccontextmanager
        async def fake_session_scope():
            yield SQLAlchemyStore(lambda: in_memory_session)

        monkeypatch.setattr(crawler_module, "session_scope", fake_session_scope)

        crawler = PodcastCrawler(service=FakeService(), request_delay=0.0)
        stats = await crawler.sync_episodes_concurrently(concurrency=2)

        assert {str(i) for i in seen_ids} == {
            str(pending1.feed_id),
            str(pending2.feed_id),
        }
        assert stats == {
            "total_feeds": 2,
            "synced": 2,
            "episodes_saved": 4,
            "failed": 0,
        }


# ---------------------------------------------------------------------------
# XIN-128 / XIN-130: crawler robustness and coverage
# ---------------------------------------------------------------------------


class TestCrawlerRobustness:
    @pytest.mark.asyncio
    async def test_resolve_and_save_ids_chunk_failure_records_range(
        self, in_memory_session: AsyncSession, itunes_search_json: str
    ):
        """XIN-128: a failed chunk is recorded; prior chunks' feeds are kept."""
        calls = 0

        def mock_handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            if calls == 2:
                return httpx.Response(status_code=500, text="boom")
            return httpx.Response(status_code=200, text=itunes_search_json)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            # max_retries=0 so the 500 fails the chunk immediately
            crawler = PodcastCrawler(request_delay=0.0, batch_size=2)
            crawler.itunes_client.max_retries = 0
            result = await crawler.resolve_and_save_ids(
                store=SQLAlchemyStore(lambda: in_memory_session),
                collection_ids=[1545953110, 1600000001, 1700000002, 1800000003],
                client=client,
            )

        assert result["failed_chunks"] == [(2, 4)]
        assert len(result["saved_feeds"]) == 2

    @pytest.mark.asyncio
    async def test_crawl_top_charts_country_exception_swallowing(
        self, in_memory_session: AsyncSession, monkeypatch
    ):
        """One country's failure must not abort the remaining countries."""
        from backend.ingestion.models import Podcast

        async def fake_get_top(limit=100, country="US", client=None):
            if country == "XX":
                raise RuntimeError("charts down")
            return [
                Podcast(
                    title="US Show",
                    feed_url="https://usshow.example.com/feed.xml",
                )
            ]

        crawler = PodcastCrawler(request_delay=0.0)
        monkeypatch.setattr(crawler.itunes_client, "get_top_podcasts", fake_get_top)

        stats = await crawler.crawl_top_charts(
            store=SQLAlchemyStore(lambda: in_memory_session),
            countries=["US", "XX"],
        )
        # US succeeded (1 discovered), XX failed but was swallowed
        assert stats["total_discovered"] == 1
        assert stats["unique_saved"] == 1
        assert stats["countries_crawled"] == ["US", "XX"]

    @pytest.mark.asyncio
    async def test_crawl_topics_min_episodes_passthrough(
        self, in_memory_session: AsyncSession, itunes_search_json: str, monkeypatch
    ):
        """min_episodes must reach the iTunes search call."""
        seen = {}

        async def fake_search(query, limit=200, country="us", min_episodes=None, client=None):
            seen["min_episodes"] = min_episodes
            return []

        crawler = PodcastCrawler(request_delay=0.0)
        monkeypatch.setattr(crawler.itunes_client, "search_podcasts", fake_search)
        stats = await crawler.crawl_topics(
            store=SQLAlchemyStore(lambda: in_memory_session),
            topics=["AI"],
            min_episodes=10,
        )
        assert seen["min_episodes"] == 10
        assert stats["total_discovered"] == 0

    @pytest.mark.asyncio
    async def test_crawl_alphabetical_prefixes_delegates(
        self, in_memory_session: AsyncSession, monkeypatch
    ):
        """Alphabetical prefixes flow through the shared crawl helper."""
        queries = []

        async def fake_search(query, limit=200, country="us", min_episodes=None, client=None):
            queries.append(query)
            return []

        crawler = PodcastCrawler(request_delay=0.0)
        monkeypatch.setattr(crawler.itunes_client, "search_podcasts", fake_search)
        stats = await crawler.crawl_alphabetical_prefixes(
            store=SQLAlchemyStore(lambda: in_memory_session),
            prefixes=["aa", "ab"],
        )
        assert queries == ["aa", "ab"]
        assert stats["total_discovered"] == 0

    @pytest.mark.asyncio
    async def test_sync_episodes_concurrently_failure_counting(
        self, in_memory_session: AsyncSession, monkeypatch
    ):
        """Per-feed failures are counted, not raised; empty pending is a no-op."""
        from backend.persistence.models.feed import Feed as FeedModel

        for i in range(3):
            in_memory_session.add(
                FeedModel(
                    rss_url=f"https://w{i}.example.com/feed.xml",
                    title=f"W{i}",
                    sync_status="pending",
                )
            )
        await in_memory_session.commit()

        async def fake_sync(store, feed_or_id_or_url, auto_queue_episodes=0):
            feed = await store.feeds.get_by_id(feed_or_id_or_url)
            if "w1.example.com" in feed.rss_url:
                raise RuntimeError("sync boom")
            return feed, []

        @asynccontextmanager
        async def fake_session_scope():
            yield SQLAlchemyStore(lambda: in_memory_session)

        crawler = PodcastCrawler(request_delay=0.0)
        monkeypatch.setattr(crawler.service, "sync_podcast_episodes", fake_sync)
        monkeypatch.setattr(crawler_module, "session_scope", fake_session_scope)
        monkeypatch.setattr(crawler_module, "get_auto_queue_episodes", lambda: 0)

        result = await crawler.sync_episodes_concurrently(concurrency=2)
        assert result["total_feeds"] == 3
        assert result["synced"] == 2
        assert result["failed"] == 1

    @pytest.mark.asyncio
    async def test_sync_episodes_concurrently_empty_pending(
        self, in_memory_session: AsyncSession, monkeypatch
    ):
        @asynccontextmanager
        async def fake_session_scope():
            yield SQLAlchemyStore(lambda: in_memory_session)

        crawler = PodcastCrawler(request_delay=0.0)
        monkeypatch.setattr(crawler_module, "session_scope", fake_session_scope)
        result = await crawler.sync_episodes_concurrently()
        assert result == {
            "total_feeds": 0,
            "synced": 0,
            "episodes_saved": 0,
            "failed": 0,
        }
