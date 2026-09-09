import json
from pathlib import Path
import pytest
import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from backend.ingestion.crawler import PodcastCrawler
from backend.ingestion.service import FeedIngestionService
from backend.persistence.models.base import Base
from backend.persistence.models.episode import Episode
from backend.persistence.models.feed import Feed

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
                db=in_memory_session,
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

            saved_feeds = await crawler.resolve_and_save_ids(
                db=in_memory_session,
                collection_ids=collection_ids,
                client=client,
            )

            assert len(saved_feeds) == 2

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
                db=in_memory_session,
                countries=["us", "gb"],
                limit_per_chart=25,
                client=client,
            )

            assert stats["total_discovered"] == 4
            assert stats["unique_saved"] == 2
            assert stats["countries_crawled"] == ["us", "gb"]
