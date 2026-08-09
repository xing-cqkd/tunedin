from datetime import datetime, timezone
from pathlib import Path
import pytest
import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from backend.ingestion.models import FeedParseResult, ParsedEpisode
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
def sample_itunes_xml() -> str:
    with open(FIXTURES_DIR / "sample_itunes.xml", "r", encoding="utf-8") as f:
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


class TestDurationParsing:
    def test_duration_formats(self):
        assert PodcastFeedParser.parse_duration(None) is None
        assert PodcastFeedParser.parse_duration("") is None
        assert PodcastFeedParser.parse_duration(1800) == 1800
        assert PodcastFeedParser.parse_duration("1800") == 1800
        assert PodcastFeedParser.parse_duration("45:30") == 2730  # 45*60 + 30
        assert PodcastFeedParser.parse_duration("01:14:22") == 4462  # 3600 + 14*60 + 22
        assert PodcastFeedParser.parse_duration("1:00:00") == 3600
        assert PodcastFeedParser.parse_duration("invalid_duration") is None


class TestMockXmlParsing:
    """Tests feed parsing by directly passing mock XML strings."""

    def test_direct_mock_xml_parsing(self):
        mock_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <title>Mock AI Podcast</title>
            <link>https://mockai.org</link>
            <description>A mock podcast for parsing verification.</description>
            <category>Technology &amp; Science</category>
            <image>
              <url>https://mockai.org/cover.png</url>
              <title>Mock AI Podcast</title>
            </image>
            <item>
              <title>Deep Dive: Neural Compilers</title>
              <guid>mock-item-001</guid>
              <pubDate>Thu, 12 Feb 2026 14:00:00 GMT</pubDate>
              <description>Comprehensive look at graph compilation.</description>
              <enclosure url="https://mockai.org/audio/ep1.mp3" length="45000000" type="audio/mpeg"/>
            </item>
          </channel>
        </rss>
        """

        result = PodcastFeedParser.parse_xml_content(
            content=mock_xml,
            rss_url="https://mockai.org/rss",
        )

        assert isinstance(result, FeedParseResult)
        assert result.metadata.title == "Mock AI Podcast"
        assert result.metadata.description == "A mock podcast for parsing verification."
        assert result.metadata.image_url == "https://mockai.org/cover.png"
        assert result.metadata.category == "Technology & Science"
        assert result.metadata.link == "https://mockai.org"
        assert result.total_feed_episodes == 1
        assert len(result.episodes) == 1

        ep = result.episodes[0]
        assert ep.guid == "mock-item-001"
        assert ep.title == "Deep Dive: Neural Compilers"
        assert ep.audio_url == "https://mockai.org/audio/ep1.mp3"
        assert ep.enclosure_type == "audio/mpeg"
        assert ep.summary == "Comprehensive look at graph compilation."
        assert ep.published_at == datetime(2026, 2, 12, 14, 0, tzinfo=timezone.utc)

    def test_mock_xml_fallback_guid_and_defaults(self):
        """Verify fallback when GUID, title, or enclosures use non-standard attributes."""
        mock_xml_no_guid = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <title>Fallback Podcast</title>
            <item>
              <link>https://fallback.org/episodes/1</link>
              <enclosure url="https://fallback.org/audio.mp3" type="audio/mpeg"/>
            </item>
          </channel>
        </rss>
        """

        result = PodcastFeedParser.parse_xml_content(
            content=mock_xml_no_guid,
            rss_url="https://fallback.org/rss",
        )

        assert len(result.episodes) == 1
        ep = result.episodes[0]
        # Should fallback to enclosure URL or link if GUID tag is absent
        assert ep.guid in ("https://fallback.org/audio.mp3", "https://fallback.org/episodes/1")
        assert ep.title == "Untitled Episode"
        assert ep.audio_url == "https://fallback.org/audio.mp3"

    def test_mock_itunes_xml_parsing(self, sample_itunes_xml):
        result = PodcastFeedParser.parse_xml_content(
            content=sample_itunes_xml,
            rss_url="https://siliconpulse.example.com/rss",
        )

        assert result.metadata.title == "Silicon Pulse Daily"
        assert result.metadata.author == "Tech Pulse Media"
        assert result.metadata.image_url == "https://siliconpulse.example.com/artwork.png"
        assert result.total_feed_episodes == 3
        assert len(result.episodes) == 3

        # Episode 101: 01:14:22 = 4462 seconds
        ep101 = result.episodes[0]
        assert ep101.guid == "sp-ep-101"
        assert ep101.duration == 4462
        assert ep101.enclosure_type == "audio/x-m4a"

        # Episode 102: 45:30 = 2730 seconds
        ep102 = result.episodes[1]
        assert ep102.guid == "sp-ep-102"
        assert ep102.duration == 2730


class TestMockJsonParsing:
    """Tests feed parsing by directly passing mock JSON feeds (str, bytes, dict)."""

    def test_direct_mock_json_string_parsing(self):
        mock_json = """{
            "version": "https://jsonfeed.org/version/1.1",
            "title": "Quantum Wave Podcast",
            "home_page_url": "https://quantumwave.example.com",
            "description": "Explorations in quantum physics and computing",
            "icon": "https://quantumwave.example.com/icon.jpg",
            "authors": [{"name": "Dr. Aris Vance"}],
            "items": [
                {
                    "id": "qw-ep-01",
                    "title": "Superposition & Entanglement",
                    "summary": "Introduction to qubits and quantum state vectors.",
                    "date_published": "2026-03-01T15:30:00Z",
                    "url": "https://quantumwave.example.com/ep1",
                    "attachments": [
                        {
                            "url": "https://quantumwave.example.com/audio/ep1.mp3",
                            "mime_type": "audio/mpeg",
                            "duration_in_seconds": 3600
                        }
                    ]
                },
                {
                    "id": "qw-ep-02",
                    "title": "Quantum Error Correction",
                    "summary": "Surface codes and topological protection.",
                    "date_published": "2026-03-08T15:30:00Z",
                    "url": "https://quantumwave.example.com/ep2",
                    "attachments": [
                        {
                            "url": "https://quantumwave.example.com/audio/ep2.m4a",
                            "mime_type": "audio/x-m4a",
                            "duration_in_seconds": 2700
                        }
                    ]
                }
            ]
        }"""

        result = PodcastFeedParser.parse_json_content(
            content=mock_json,
            rss_url="https://quantumwave.example.com/feed.json",
        )

        assert isinstance(result, FeedParseResult)
        assert result.metadata.title == "Quantum Wave Podcast"
        assert result.metadata.author == "Dr. Aris Vance"
        assert result.metadata.image_url == "https://quantumwave.example.com/icon.jpg"
        assert result.metadata.description == "Explorations in quantum physics and computing"
        assert result.total_feed_episodes == 2
        assert len(result.episodes) == 2

        ep1 = result.episodes[0]
        assert ep1.guid == "qw-ep-01"
        assert ep1.title == "Superposition & Entanglement"
        assert ep1.audio_url == "https://quantumwave.example.com/audio/ep1.mp3"
        assert ep1.duration == 3600
        assert ep1.published_at == datetime(2026, 3, 1, 15, 30, tzinfo=timezone.utc)

    def test_direct_mock_json_dict_and_unified_parse(self):
        mock_data = {
            "title": "Dict Podcast",
            "author": "Alice Developer",
            "items": [
                {
                    "id": "dict-ep-1",
                    "title": "Episode from Python Dict",
                    "audio_url": "https://example.com/dict.mp3",
                    "duration": "00:30:00",
                    "published_at": "2026-04-01T10:00:00Z",
                }
            ],
        }

        result = PodcastFeedParser.parse_content(
            content=mock_data,
            rss_url="https://example.com/dict-feed",
        )

        assert result.metadata.title == "Dict Podcast"
        assert result.metadata.author == "Alice Developer"
        assert len(result.episodes) == 1
        assert result.episodes[0].guid == "dict-ep-1"
        assert result.episodes[0].duration == 1800
        assert result.episodes[0].published_at == datetime(2026, 4, 1, 10, 0, tzinfo=timezone.utc)


class TestIncrementalFiltering:
    def test_filter_by_last_updated_date(self, sample_feed_xml):
        # Jan 17, 2026 10:00:00 UTC is Ep 2's timestamp
        last_updated = datetime(2026, 1, 17, 10, 0, tzinfo=timezone.utc)

        result = PodcastFeedParser.parse_xml_content(
            content=sample_feed_xml,
            rss_url="https://aifrontier.example.com/feed.xml",
            last_updated_at=last_updated,
        )

        # Should only include episode 3 (Jan 24, 2026)
        assert result.total_feed_episodes == 3
        assert len(result.episodes) == 1
        assert result.episodes[0].guid == "ai-frontier-ep-003"

    def test_filter_by_known_guids(self, sample_feed_xml):
        known = {"ai-frontier-ep-001", "ai-frontier-ep-003"}

        result = PodcastFeedParser.parse_xml_content(
            content=sample_feed_xml,
            rss_url="https://aifrontier.example.com/feed.xml",
            known_guids=known,
        )

        assert result.total_feed_episodes == 3
        assert len(result.episodes) == 1
        assert result.episodes[0].guid == "ai-frontier-ep-002"

    def test_filter_when_all_episodes_seen(self, sample_feed_xml):
        all_guids = {"ai-frontier-ep-001", "ai-frontier-ep-002", "ai-frontier-ep-003"}

        result = PodcastFeedParser.parse_xml_content(
            content=sample_feed_xml,
            rss_url="https://aifrontier.example.com/feed.xml",
            known_guids=all_guids,
        )

        assert result.total_feed_episodes == 3
        assert len(result.episodes) == 0


class TestAsyncHttpAndCaching:
    @pytest.mark.asyncio
    async def test_fetch_304_not_modified(self):
        rss_url = "https://example.com/podcast.xml"

        def mock_handler(request: httpx.Request) -> httpx.Response:
            assert request.headers.get("If-None-Match") == '"test-etag-123"'
            return httpx.Response(status_code=304)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            result = await PodcastFeedParser.fetch_and_parse(
                rss_url=rss_url,
                etag='"test-etag-123"',
                client=client,
            )

            assert result.is_not_modified is True
            assert len(result.episodes) == 0


class TestIngestionService:
    @pytest.mark.asyncio
    async def test_service_initial_sync_and_incremental_update(
        self, in_memory_session: AsyncSession, sample_feed_xml: str
    ):
        rss_url = "https://aifrontier.example.com/feed.xml"

        def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                status_code=200,
                text=sample_feed_xml,
                headers={"ETag": '"etag-v1"', "Last-Modified": "Mon, 24 Jan 2026 12:00:00 GMT"},
            )

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            service = FeedIngestionService()

            # 1. Initial Ingestion
            feed, new_eps = await service.ingest_feed(
                db=in_memory_session,
                rss_url=rss_url,
                client=client,
            )

            assert feed.title == "AI Frontier Podcast"
            assert feed.sync_status == "active"
            assert len(new_eps) == 3
            assert feed.etag == '"etag-v1"'

            # 2. Re-running ingestion without feed changes should detect known GUIDs and insert 0 new episodes
            feed2, second_run_eps = await service.ingest_feed(
                db=in_memory_session,
                rss_url=rss_url,
                client=client,
            )

            assert len(second_run_eps) == 0
            assert feed2.feed_id == feed.feed_id
