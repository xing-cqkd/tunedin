from datetime import datetime, timezone
from pathlib import Path
import logging
import pytest
import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from backend.ingestion.models import FeedParseResult, ParsedEpisode
from backend.ingestion.parser import PodcastFeedParser
from backend.ingestion.service import FeedIngestionService
from backend.persistence.models.base import Base
from backend.persistence.models.episode import Episode
from backend.persistence.models.feed import Feed
from backend.persistence.sqlalchemy_store import SQLAlchemyStore

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

    @pytest.mark.asyncio
    async def test_fetch_json_feed_over_http_is_not_misparsed_as_xml(self):
        """Regression test for XIN-33: fetch_and_parse must route through the
        unified parse_content entrypoint so JSON Feeds served over HTTP are
        detected and parsed as JSON."""
        mock_json = """{
            "version": "https://jsonfeed.org/version/1.1",
            "title": "Quantum Wave Podcast",
            "items": [
                {
                    "id": "qw-ep-01",
                    "title": "Superposition & Entanglement",
                    "date_published": "2026-03-01T15:30:00Z",
                    "url": "https://quantumwave.example.com/ep1",
                    "attachments": [
                        {"url": "https://quantumwave.example.com/audio/ep1.mp3",
                         "mime_type": "audio/mpeg",
                         "duration_in_seconds": 3600}
                    ]
                }
            ]
        }"""

        def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                status_code=200,
                content=mock_json.encode("utf-8"),
                headers={"Content-Type": "application/feed+json"},
            )

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            result = await PodcastFeedParser.fetch_and_parse(
                rss_url="https://quantumwave.example.com/feed.json",
                client=client,
            )

            assert result.metadata.title == "Quantum Wave Podcast"
            assert result.total_feed_episodes == 1
            assert len(result.episodes) == 1
            assert result.episodes[0].guid == "qw-ep-01"
            assert result.episodes[0].audio_url == "https://quantumwave.example.com/audio/ep1.mp3"


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
                store=SQLAlchemyStore(lambda: in_memory_session),
                rss_url=rss_url,
                client=client,
            )

            assert feed.title == "AI Frontier Podcast"
            assert feed.sync_status == "active"
            assert len(new_eps) == 3
            assert feed.etag == '"etag-v1"'

            # 2. Re-running ingestion without feed changes should detect known GUIDs and insert 0 new episodes
            feed2, second_run_eps = await service.ingest_feed(
                store=SQLAlchemyStore(lambda: in_memory_session),
                rss_url=rss_url,
                client=client,
            )

            assert len(second_run_eps) == 0
            assert feed2.feed_id == feed.feed_id


# ---------------------------------------------------------------------------
# XIN-126: parser robustness (bozo check, null handling, enclosure filtering)
# ---------------------------------------------------------------------------


class TestParserRobustness:
    """Malformed feeds must raise ValueError (never silently produce a feed)."""

    def test_malformed_html_raises_value_error(self):
        with pytest.raises(ValueError):
            PodcastFeedParser.parse_xml_content(
                "<html><body>This is not a feed</body></html>",
                rss_url="https://example.com/feed.xml",
            )

    def test_empty_content_raises_value_error(self):
        with pytest.raises(ValueError):
            PodcastFeedParser.parse_xml_content(
                "", rss_url="https://example.com/feed.xml"
            )

    def test_truncated_xml_raises_value_error(self):
        with pytest.raises(ValueError):
            PodcastFeedParser.parse_xml_content(
                '<?xml version="1.0"?><rss version="2.0"><channel><title>Cut',
                rss_url="https://example.com/feed.xml",
            )

    def test_feed_without_title_or_link_raises_value_error(self):
        with pytest.raises(ValueError):
            PodcastFeedParser.parse_xml_content(
                '<?xml version="1.0"?><rss version="2.0"><channel></channel></rss>',
                rss_url="https://example.com/feed.xml",
            )

    def test_feed_with_link_but_no_title_does_not_raise(self):
        result = PodcastFeedParser.parse_xml_content(
            '<?xml version="1.0"?><rss version="2.0">'
            "<channel><link>https://example.com</link></channel></rss>",
            rss_url="https://example.com/feed.xml",
        )
        assert result.metadata.title == "Untitled Podcast"
        assert result.episodes == []

    def test_xml_entry_without_title_gets_default(self):
        result = PodcastFeedParser.parse_xml_content(
            '<?xml version="1.0"?><rss version="2.0">'
            "<channel><title>T</title><link>https://example.com</link>"
            "<item><guid>g1</guid>"
            '<enclosure url="https://example.com/e.mp3" type="audio/mpeg"/>'
            "</item></channel></rss>",
            rss_url="https://example.com/feed.xml",
        )
        assert len(result.episodes) == 1
        assert result.episodes[0].title == "Untitled Episode"

    def test_json_items_null_does_not_crash(self):
        result = PodcastFeedParser.parse_json_content(
            {"title": "JSON Cast", "items": None},
            rss_url="https://example.com/feed.json",
        )
        assert result.metadata.title == "JSON Cast"
        assert result.episodes == []

    def test_json_null_feed_title_does_not_crash(self):
        result = PodcastFeedParser.parse_json_content(
            {"title": None, "items": []},
            rss_url="https://example.com/feed.json",
        )
        assert result.metadata.title == "Untitled Podcast"

    def test_json_null_episode_title_does_not_crash(self):
        result = PodcastFeedParser.parse_json_content(
            {
                "title": "JSON Cast",
                "items": [
                    {
                        "id": "ep-1",
                        "title": None,
                        "attachments": [
                            {"url": "https://example.com/ep1.mp3", "mime_type": "audio/mpeg"}
                        ],
                    }
                ],
            },
            rss_url="https://example.com/feed.json",
        )
        assert len(result.episodes) == 1
        assert result.episodes[0].title == "Untitled Episode"

    def test_pdf_only_enclosure_has_no_audio_url(self, rich_feed_xml: str):
        """XIN-126: a PDF enclosure must not be recorded as the audio URL."""
        result = PodcastFeedParser.parse_xml_content(
            rich_feed_xml, rss_url="https://rich.example.com/feed.xml"
        )
        pdf_ep = next(e for e in result.episodes if e.title == "PDF Only Episode")
        assert pdf_ep.audio_url == ""
        assert pdf_ep.enclosure_type is None

        audio_ep = next(e for e in result.episodes if e.title == "Episode With Extras")
        assert audio_ep.audio_url == "https://rich.example.com/ep1.mp3"
        assert audio_ep.enclosure_type == "audio/mpeg"

    def test_rich_feed_extracts_transcript_chapters_content_image(
        self, rich_feed_xml: str
    ):
        result = PodcastFeedParser.parse_xml_content(
            rich_feed_xml, rss_url="https://rich.example.com/feed.xml"
        )
        ep = next(e for e in result.episodes if e.title == "Episode With Extras")
        assert ep.transcript_url == "https://rich.example.com/ep1.vtt"
        assert ep.chapters_url == "https://rich.example.com/ep1.json"
        assert "<strong>HTML</strong>" in (ep.content_html or "")
        assert ep.image_url == "https://rich.example.com/ep1.jpg"

    def test_parse_published_date_unparseable_returns_none(self):
        assert (
            PodcastFeedParser.parse_published_date({"published": "not a date"})
            is None
        )

    def test_parse_published_date_naive_string_assumes_utc(self):
        dt = PodcastFeedParser.parse_published_date(
            {"published": "2026-01-05 10:00:00"}
        )
        assert dt is not None
        assert dt.tzinfo == timezone.utc


@pytest.fixture
def rich_feed_xml() -> str:
    with open(FIXTURES_DIR / "rich_feed.xml", "r", encoding="utf-8") as f:
        return f.read()


class TestFetchAndParseErrors:
    """HTTP/network failures must surface as exceptions (XIN-126)."""

    @pytest.mark.asyncio
    async def test_fetch_404_raises(self):
        def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code=404, text="not found")

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler)
        ) as client:
            with pytest.raises(httpx.HTTPStatusError):
                await PodcastFeedParser.fetch_and_parse(
                    "https://example.com/feed.xml", client=client
                )

    @pytest.mark.asyncio
    async def test_fetch_500_raises(self):
        def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code=500, text="boom")

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler)
        ) as client:
            with pytest.raises(httpx.HTTPStatusError):
                await PodcastFeedParser.fetch_and_parse(
                    "https://example.com/feed.xml", client=client
                )

    @pytest.mark.asyncio
    async def test_fetch_network_error_raises(self):
        def mock_handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler)
        ) as client:
            with pytest.raises(httpx.ConnectError):
                await PodcastFeedParser.fetch_and_parse(
                    "https://example.com/feed.xml", client=client
                )

    @pytest.mark.asyncio
    async def test_fetch_sends_conditional_headers(self, sample_feed_xml: str):
        seen = {}

        def mock_handler(request: httpx.Request) -> httpx.Response:
            seen["if-none-match"] = request.headers.get("If-None-Match")
            seen["if-modified-since"] = request.headers.get("If-Modified-Since")
            return httpx.Response(status_code=200, text=sample_feed_xml)

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler)
        ) as client:
            await PodcastFeedParser.fetch_and_parse(
                "https://example.com/feed.xml",
                etag='"etag-1"',
                last_modified="Mon, 24 Jan 2026 12:00:00 GMT",
                client=client,
            )
        assert seen["if-none-match"] == '"etag-1"'
        assert seen["if-modified-since"] == "Mon, 24 Jan 2026 12:00:00 GMT"


# ---------------------------------------------------------------------------
# XIN-82: BOM-prefixed JSON detection in parse_content
# ---------------------------------------------------------------------------


class TestBomPrefixedJsonDetection:
    """A leading UTF-8 BOM must not defeat parse_content's JSON sniffing."""

    JSON_DOC = """{
        "version": "https://jsonfeed.org/version/1.1",
        "title": "BOM Cast",
        "items": [
            {
                "id": "bom-ep-1",
                "title": "BOM Episode",
                "date_published": "2026-05-01T10:00:00Z",
                "attachments": [
                    {"url": "https://example.com/bom.mp3", "mime_type": "audio/mpeg"}
                ]
            }
        ]
    }"""

    def _assert_parsed_as_json(self, result):
        assert isinstance(result, FeedParseResult)
        assert result.metadata.title == "BOM Cast"
        assert result.total_feed_episodes == 1
        assert len(result.episodes) == 1
        assert result.episodes[0].guid == "bom-ep-1"

    def test_bom_prefixed_json_str_is_detected(self):
        payload = "\ufeff" + self.JSON_DOC
        assert not payload.strip().startswith("{")  # sanity: the old check failed
        result = PodcastFeedParser.parse_content(
            content=payload, rss_url="https://example.com/feed.json"
        )
        self._assert_parsed_as_json(result)

    def test_bom_prefixed_json_bytes_are_detected(self):
        payload = b"\xef\xbb\xbf" + self.JSON_DOC.encode("utf-8")
        assert not payload.strip().startswith(b"{")  # sanity: the old check failed
        result = PodcastFeedParser.parse_content(
            content=payload, rss_url="https://example.com/feed.json"
        )
        self._assert_parsed_as_json(result)

    def test_json_without_bom_still_detected(self):
        result = PodcastFeedParser.parse_content(
            content=self.JSON_DOC, rss_url="https://example.com/feed.json"
        )
        self._assert_parsed_as_json(result)


# ---------------------------------------------------------------------------
# XIN-52: date-parse failures must be logged, not silently swallowed
# ---------------------------------------------------------------------------


class TestDateParseFailureLogging:
    """The former `except Exception: pass` sites in the date parsers now log."""

    def test_struct_time_failure_logs_and_returns_none(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="backend.ingestion.parser"):
            result = PodcastFeedParser.parse_published_date(
                {"published_parsed": "garbage-struct-time"}
            )
        assert result is None
        assert "struct_time" in caplog.text

    def test_unparseable_date_string_logs_and_returns_none(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="backend.ingestion.parser"):
            result = PodcastFeedParser.parse_published_date(
                {"published": "not a date"}
            )
        assert result is None
        assert "not a date" in caplog.text

    def test_unparseable_json_date_logs_and_returns_none(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="backend.ingestion.parser"):
            result = PodcastFeedParser._parse_json_published_at(
                {"date_published": "not a date either"}
            )
        assert result is None
        assert "not a date either" in caplog.text
