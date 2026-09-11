import json
from pathlib import Path
import pytest
import httpx

from backend.ingestion.itunes import ITunesSearchClient, podcast_from_itunes
from backend.ingestion.models import Podcast, PodcastSearchResult

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def itunes_search_json() -> str:
    with open(FIXTURES_DIR / "itunes_search.json", "r", encoding="utf-8") as f:
        return f.read()


class TestITunesPodcastModelParsing:
    def test_parse_itunes_dict(self, itunes_search_json: str):
        data = json.loads(itunes_search_json)
        item = data["results"][0]

        podcast = podcast_from_itunes(item)

        assert podcast.title == "Huberman Lab"
        assert podcast.author == "Scicomm Media"
        assert podcast.feed_url == "https://feeds.megaphone.fm/hubermanlab"
        assert podcast.provider_id == "1545953110"
        assert podcast.provider == "itunes"
        assert podcast.provider_id == "1545953110"
        assert podcast.artwork_url == "https://is1-ssl.mzstatic.com/image/thumb/Podcasts116/v4/huberman_600x600.jpg"
        assert podcast.primary_genre == "Science"
        assert "Health & Fitness" in podcast.genres
        assert podcast.episode_count == 210
        assert podcast.country == "USA"
        assert podcast.external_url == "https://podcasts.apple.com/us/podcast/huberman-lab/id1545953110?uo=4"
        assert podcast.release_date is not None

    def test_podcast_model_is_provider_neutral(self, itunes_search_json: str):
        """XIN-64: the domain model carries no provider-specific fields;
        provider data lives in the generic provider/provider_id/external_url
        triple, parsed by the provider module."""
        import dataclasses

        data = json.loads(itunes_search_json)
        podcast = podcast_from_itunes(data["results"][0])

        field_names = {f.name for f in dataclasses.fields(podcast)}
        assert "itunes_id" not in field_names
        assert "itunes_url" not in field_names
        assert not hasattr(podcast, "itunes_id")
        assert not hasattr(podcast, "itunes_url")
        assert podcast.provider == "itunes"
        assert podcast.provider_id == "1545953110"
        assert podcast.external_url == (
            "https://podcasts.apple.com/us/podcast/huberman-lab/id1545953110?uo=4"
        )


class TestITunesSearchClient:
    @pytest.mark.asyncio
    async def test_search_podcasts_filters_missing_feed_url(self, itunes_search_json: str):
        def mock_handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/search"
            assert request.url.params.get("term") == "huberman"
            assert request.url.params.get("entity") == "podcast"
            assert request.url.params.get("media") == "podcast"
            return httpx.Response(status_code=200, text=itunes_search_json)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            search_client = ITunesSearchClient()
            podcasts = await search_client.search_podcasts("huberman", client=client)

            # Fixture contains 3 results, but 3rd has no feedUrl -> should return 2
            assert len(podcasts) == 2
            assert podcasts[0].title == "Huberman Lab"
            assert podcasts[1].title == "Lex Fridman Podcast"

    @pytest.mark.asyncio
    async def test_search_podcasts_filters_by_min_episodes(self, itunes_search_json: str):
        def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code=200, text=itunes_search_json)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            search_client = ITunesSearchClient()
            # In fixture: Huberman has 210 eps, Lex has 420 eps.
            # Filtering min_episodes=300 should only return Lex Fridman Podcast
            podcasts = await search_client.search_podcasts(
                "science", min_episodes=300, client=client
            )

            assert len(podcasts) == 1
            assert podcasts[0].title == "Lex Fridman Podcast"

    @pytest.mark.asyncio
    async def test_search_returns_standard_search_result(self, itunes_search_json: str):
        def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code=200, text=itunes_search_json)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            search_client = ITunesSearchClient()
            result = await search_client.search("science", client=client)

            assert isinstance(result, PodcastSearchResult)
            assert result.query == "science"
            assert result.count == 2
            assert result.provider == "itunes"
            assert len(result.podcasts) == 2

    @pytest.mark.asyncio
    async def test_search_empty_query_returns_empty_list(self):
        search_client = ITunesSearchClient()
        assert await search_client.search_podcasts("") == []
        assert await search_client.search_podcasts("   ") == []

    @pytest.mark.asyncio
    async def test_lookup_podcast_by_id(self, itunes_search_json: str):
        single_result = {
            "resultCount": 1,
            "results": [json.loads(itunes_search_json)["results"][0]],
        }

        def mock_handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/lookup"
            assert request.url.params.get("id") == "1545953110"
            return httpx.Response(status_code=200, json=single_result)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            search_client = ITunesSearchClient()
            podcast = await search_client.lookup_podcast_by_id(1545953110, client=client)

            assert podcast is not None
            assert podcast.provider_id == "1545953110"
            assert podcast.title == "Huberman Lab"

    @pytest.mark.asyncio
    async def test_lookup_podcasts_by_ids_batched(self, itunes_search_json: str):
        data = json.loads(itunes_search_json)

        def mock_handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/lookup"
            assert request.url.params.get("id") == "1545953110,1600000001"
            return httpx.Response(status_code=200, json=data)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            search_client = ITunesSearchClient()
            podcasts = await search_client.lookup_podcasts_by_ids(
                [1545953110, 1600000001], client=client
            )

            assert len(podcasts) == 2
            assert podcasts[0].provider_id == "1545953110"
            assert podcasts[1].provider_id == "1600000001"

    @pytest.mark.asyncio
    async def test_get_top_podcasts_resolves_feed_urls(self, itunes_search_json: str):
        charts_response = {
            "feed": {
                "title": "Top Podcasts",
                "results": [
                    {"id": "1545953110", "name": "Huberman Lab", "artistName": "Scicomm Media"},
                    {"id": "1600000001", "name": "Lex Fridman Podcast", "artistName": "Lex Fridman"},
                ],
            }
        }
        lookup_data = json.loads(itunes_search_json)

        def mock_handler(request: httpx.Request) -> httpx.Response:
            if "applemarketingtools.com" in request.url.host:
                assert "/podcasts/top/25/podcasts.json" in request.url.path
                return httpx.Response(status_code=200, json=charts_response)
            elif "/lookup" in request.url.path:
                return httpx.Response(status_code=200, json=lookup_data)
            return httpx.Response(status_code=404)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            search_client = ITunesSearchClient()
            top_podcasts = await search_client.get_top_podcasts(limit=25, country="us", client=client)

            assert len(top_podcasts) == 2
            assert top_podcasts[0].provider_id == "1545953110"
            assert top_podcasts[0].feed_url == "https://feeds.megaphone.fm/hubermanlab"
            assert top_podcasts[1].provider_id == "1600000001"
            assert top_podcasts[1].feed_url == "https://lexfridman.com/feed/podcast/"

    @pytest.mark.asyncio
    async def test_rate_limit_retry_handling(self):
        attempts = 0

        def mock_handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                return httpx.Response(status_code=429)
            return httpx.Response(
                status_code=200,
                json={
                    "resultCount": 1,
                    "results": [
                        {
                            "collectionId": 100,
                            "collectionName": "Recovered Show",
                            "feedUrl": "https://recovered.example.com/rss",
                        }
                    ],
                },
            )

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            search_client = ITunesSearchClient(max_retries=2)
            podcasts = await search_client.search_podcasts("retry test", client=client)

            assert attempts == 2
            assert len(podcasts) == 1
            assert podcasts[0].title == "Recovered Show"


    # ------------------------------------------------------------------
    # XIN-128 / XIN-130: retry branches and edge cases
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_5xx_retry_then_success(self):
        attempts = 0

        def mock_handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                return httpx.Response(status_code=503, text="unavailable")
            return httpx.Response(
                status_code=200,
                json={
                    "resultCount": 1,
                    "results": [
                        {
                            "collectionId": 42,
                            "collectionName": "Recovered",
                            "feedUrl": "https://recovered.example.com/rss",
                        }
                    ],
                },
            )

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            search_client = ITunesSearchClient(max_retries=3)
            podcasts = await search_client.search_podcasts("retry", client=client)

        assert attempts == 3
        assert len(podcasts) == 1
        assert podcasts[0].title == "Recovered"

    @pytest.mark.asyncio
    async def test_connect_error_retry_then_success(self):
        attempts = 0

        def mock_handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise httpx.ConnectError("boom", request=request)
            return httpx.Response(
                status_code=200,
                json={
                    "resultCount": 1,
                    "results": [
                        {
                            "collectionId": 43,
                            "collectionName": "Reconnected",
                            "feedUrl": "https://reconnected.example.com/rss",
                        }
                    ],
                },
            )

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            search_client = ITunesSearchClient(max_retries=2)
            podcasts = await search_client.search_podcasts("retry", client=client)

        assert attempts == 2
        assert podcasts[0].title == "Reconnected"

    @pytest.mark.asyncio
    async def test_retries_exhausted_raises(self):
        def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code=500, text="still down")

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            search_client = ITunesSearchClient(max_retries=1)
            with pytest.raises(httpx.HTTPStatusError):
                await search_client.search_podcasts("retry", client=client)

    @pytest.mark.asyncio
    async def test_timeout_exhausted_raises(self):
        def mock_handler(request: httpx.Request) -> httpx.Response:
            raise httpx.TimeoutException("timed out", request=request)

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            search_client = ITunesSearchClient(max_retries=0)
            with pytest.raises(httpx.TimeoutException):
                await search_client.search_podcasts("retry", client=client)

    @pytest.mark.asyncio
    async def test_lookup_podcasts_by_ids_empty(self):
        search_client = ITunesSearchClient()
        assert await search_client.lookup_podcasts_by_ids([]) == []
        assert await search_client.lookup_podcasts_by_ids(["  "]) == []

    @pytest.mark.asyncio
    async def test_get_top_podcasts_empty_chart(self):
        def mock_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                status_code=200, json={"feed": {"title": "Top", "results": []}}
            )

        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            search_client = ITunesSearchClient()
            assert await search_client.get_top_podcasts(limit=5, client=client) == []
