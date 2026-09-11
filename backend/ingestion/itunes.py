import logging
from typing import Any, Dict, List, Optional, Union
import httpx

from backend.ingestion.http_util import get_with_retry, maybe_client
from backend.ingestion.models import Podcast, PodcastSearchResult

logger = logging.getLogger(__name__)


class ITunesSearchClient:
    """
    Asynchronous client for podcast discovery via Apple Podcasts (iTunes Search & Lookup API
    and Apple Marketing Tools Top Charts API). Requires zero API keys or authentication.
    """

    def __init__(
        self,
        base_url: str = "https://itunes.apple.com",
        charts_url: str = "https://rss.applemarketingtools.com/api/v2",
        default_country: str = "US",
        timeout: float = 15.0,
        user_agent: str = "TunedIn/1.0 (+https://github.com/tunedin)",
        max_retries: int = 2,
    ):
        self.base_url = base_url.rstrip("/")
        self.charts_url = charts_url.rstrip("/")
        self.default_country = default_country
        self.timeout = timeout
        self.user_agent = user_agent
        self.max_retries = max_retries

    def _get_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": self.user_agent,
            "Accept": "application/json, text/javascript, */*",
        }

    async def _get_with_retry(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        client: Optional[httpx.AsyncClient] = None,
    ) -> httpx.Response:
        """Execute GET request with exponential backoff on HTTP 429 / 5xx responses.

        XIN-49/XIN-56: delegates to the shared client lifecycle and retry
        helper in ``backend.ingestion.http_util``.
        """
        async with maybe_client(client, timeout=self.timeout) as c:
            return await get_with_retry(
                c,
                url,
                params=params,
                headers=self._get_headers(),
                max_retries=self.max_retries,
            )

    async def search_podcasts(
        self,
        query: str,
        limit: int = 20,
        country: Optional[str] = None,
        genre_id: Optional[int] = None,
        min_episodes: Optional[int] = None,
        client: Optional[httpx.AsyncClient] = None,
    ) -> List[Podcast]:
        """
        Search podcasts by keyword/term on the iTunes Search API.
        Optionally filter by Apple genre ID and minimum episode count (popularity/longevity indicator).
        Filters out entries without a canonical feedUrl.
        """
        if not query or not query.strip():
            return []

        country_code = (country or self.default_country).lower()
        url = f"{self.base_url}/search"
        params: Dict[str, Any] = {
            "term": query.strip(),
            "media": "podcast",
            "entity": "podcast",
            "limit": min(max(1, limit), 200),
            "country": country_code,
        }
        if genre_id is not None:
            params["genreId"] = genre_id

        response = await self._get_with_retry(url, params=params, client=client)
        data = response.json()
        results = data.get("results", [])

        podcasts: List[Podcast] = []
        for item in results:
            feed_url = item.get("feedUrl")
            if not feed_url or not feed_url.strip():
                continue

            track_count = item.get("trackCount")
            if min_episodes is not None and (track_count is None or track_count < min_episodes):
                continue

            podcasts.append(Podcast.from_itunes(item))

        return podcasts

    async def search(
        self,
        query: str,
        limit: int = 20,
        country: Optional[str] = None,
        genre_id: Optional[int] = None,
        min_episodes: Optional[int] = None,
        client: Optional[httpx.AsyncClient] = None,
    ) -> PodcastSearchResult:
        """
        Search podcasts and return a standardized PodcastSearchResult.
        """
        podcasts = await self.search_podcasts(
            query=query,
            limit=limit,
            country=country,
            genre_id=genre_id,
            min_episodes=min_episodes,
            client=client,
        )
        return PodcastSearchResult(
            query=query,
            count=len(podcasts),
            podcasts=podcasts,
            provider="itunes",
        )

    async def lookup_podcast_by_id(
        self,
        collection_id: Union[int, str],
        country: Optional[str] = None,
        client: Optional[httpx.AsyncClient] = None,
    ) -> Optional[Podcast]:
        """
        Lookup exact podcast metadata by Apple Podcasts collection / track ID.
        """
        podcasts = await self.lookup_podcasts_by_ids(
            collection_ids=[collection_id],
            country=country,
            client=client,
        )
        return podcasts[0] if podcasts else None

    async def lookup_podcasts_by_ids(
        self,
        collection_ids: List[Union[int, str]],
        country: Optional[str] = None,
        client: Optional[httpx.AsyncClient] = None,
    ) -> List[Podcast]:
        """
        Batched lookup of podcast metadata for multiple Apple collection IDs in a single query.
        """
        if not collection_ids:
            return []

        clean_ids = [str(cid).strip() for cid in collection_ids if str(cid).strip()]
        if not clean_ids:
            return []

        country_code = (country or self.default_country).lower()
        url = f"{self.base_url}/lookup"
        params = {
            "id": ",".join(clean_ids),
            "entity": "podcast",
            "country": country_code,
        }

        response = await self._get_with_retry(url, params=params, client=client)
        data = response.json()
        results = data.get("results", [])

        podcasts: List[Podcast] = []
        for item in results:
            feed_url = item.get("feedUrl")
            if feed_url and feed_url.strip():
                podcasts.append(Podcast.from_itunes(item))

        return podcasts

    async def get_top_podcasts(
        self,
        limit: int = 25,
        country: Optional[str] = None,
        client: Optional[httpx.AsyncClient] = None,
    ) -> List[Podcast]:
        """
        Fetch top / trending podcasts from Apple's chart API, then resolves
        collection details via batched lookup to guarantee canonical feedUrl is populated.
        """
        country_code = (country or self.default_country).lower()
        limit_val = min(max(1, limit), 100)
        url = f"{self.charts_url}/{country_code}/podcasts/top/{limit_val}/podcasts.json"

        response = await self._get_with_retry(url, client=client)
        data = response.json()

        feed_data = data.get("feed", {})
        chart_results = feed_data.get("results", [])
        if not chart_results:
            return []

        # Extract Apple collection IDs from chart items
        collection_ids = [
            item.get("id") for item in chart_results if item.get("id")
        ]

        if not collection_ids:
            return []

        # Perform batched lookup to obtain feedUrl and complete metadata
        resolved_podcasts = await self.lookup_podcasts_by_ids(
            collection_ids=collection_ids,
            country=country_code,
            client=client,
        )

        # Maintain chart ordering
        podcast_map = {
            str(p.itunes_id): p for p in resolved_podcasts if p.itunes_id is not None
        }
        ordered_podcasts: List[Podcast] = []
        for cid in collection_ids:
            cid_str = str(cid)
            if cid_str in podcast_map:
                ordered_podcasts.append(podcast_map[cid_str])

        return ordered_podcasts
