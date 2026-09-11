import asyncio
import logging
from typing import Any, Callable, Coroutine, Dict, List, Optional, Sequence, Set, Tuple, Union
import httpx
from backend.persistence.repositories import Store

from backend.ingestion.errors import IngestionError
from backend.ingestion.http_util import maybe_client
from backend.ingestion.itunes import ITunesSearchClient
from backend.ingestion.models import Podcast
from backend.ingestion.service import FeedIngestionService, FeedSyncStatus
from backend.ingestion.task_queue import get_queue_driver
from settings import get_auto_queue_episodes, get_crawler_countries, session_scope
from backend.persistence.models.feed import Feed

logger = logging.getLogger(__name__)

# Default comprehensive seed topics for broad podcast harvesting
DEFAULT_TOPICS = [
    # Technology & AI
    "Artificial Intelligence", "Machine Learning", "Software Engineering",
    "Computer Science", "Data Science", "Cybersecurity", "Blockchain",
    # Science & Health
    "Neuroscience", "Physics", "Biology", "Psychology", "Health & Fitness",
    "Medicine", "Longevity", "Space & Astronomy", "Environmental Science",
    # Business & Economics
    "Venture Capital", "Startups", "Economics", "Finance", "Investing",
    "Product Management", "Marketing", "Entrepreneurship", "Real Estate",
    # Society & Humanities
    "Philosophy", "History", "World Politics", "True Crime", "Documentary",
    "News & Current Events", "Culture", "Education", "Books & Literature",
]

class PodcastCrawler:
    """
    Automated crawler for discovering, batching, and ingesting massive catalogs
    of podcasts and episodes into SQLite/Postgres for downstream LLM analysis.
    """

    def __init__(
        self,
        service: Optional[FeedIngestionService] = None,
        itunes_client: Optional[ITunesSearchClient] = None,
        request_delay: float = 0.5,
        batch_size: int = 200,
    ):
        self.service = service or FeedIngestionService(queue_driver=get_queue_driver())
        self.itunes_client = itunes_client or self.service.itunes_client
        self.request_delay = request_delay
        self.batch_size = min(max(1, batch_size), 200)

    # -------------------------------------------------------------------------
    # Stage 1: Breadth Crawling (Show Discovery & Immediate Persistence)
    # -------------------------------------------------------------------------

    async def _crawl_and_save(
        self,
        store: Store,
        items: Sequence[str],
        fetch_for_item: Callable[[str, httpx.AsyncClient], Coroutine[Any, Any, List[Podcast]]],
        progress_label: Callable[[str], str],
        client: Optional[httpx.AsyncClient] = None,
        on_progress: Optional[Callable[[str, int], None]] = None,
    ) -> Dict[str, Any]:
        """
        Shared crawl loop (XIN-129): for each item (country/topic), fetch
        podcasts via ``fetch_for_item``, dedup by feed URL within this crawl,
        persist via the service, report progress, and sleep politely.
        Per-item failures are logged and the crawl continues.
        """
        total_discovered = 0
        total_saved = 0
        visited_urls: Set[str] = set()

        # XIN-49: shared client lifecycle.
        async with maybe_client(client) as client:
            for item in items:
                try:
                    podcasts = await fetch_for_item(item, client)
                    total_discovered += len(podcasts)

                    # Filter duplicates within current crawl session
                    new_podcasts = [
                        p for p in podcasts if p.feed_url and p.feed_url not in visited_urls
                    ]
                    for p in new_podcasts:
                        visited_urls.add(p.feed_url)

                    saved_feeds = await self.service.save_podcasts(store, new_podcasts)
                    total_saved += len(saved_feeds)

                    if on_progress:
                        on_progress(progress_label(item), len(saved_feeds))

                    await asyncio.sleep(self.request_delay)
                except Exception as e:
                    logger.error("Error crawling %s: %s", progress_label(item), str(e))

        return {
            "total_discovered": total_discovered,
            "unique_saved": total_saved,
            "items_crawled": list(items),
        }

    async def crawl_top_charts(
        self,
        store: Store,
        countries: Optional[Sequence[str]] = None,
        limit_per_chart: int = 100,
        client: Optional[httpx.AsyncClient] = None,
        on_progress: Optional[Callable[[str, int], None]] = None,
    ) -> Dict[str, Any]:
        """
        Crawls top charts across multiple Apple storefront countries,
        resolves publisher RSS URLs via batched lookups, and immediately saves shows.

        `countries` defaults to ingestion.crawler_countries from settings.yaml.
        """
        country_list = list(countries) if countries else get_crawler_countries()

        async def _fetch(country: str, http_client: httpx.AsyncClient) -> List[Podcast]:
            logger.info("Crawling top charts for country: %s...", country.upper())
            return await self.itunes_client.get_top_podcasts(
                limit=limit_per_chart,
                country=country,
                client=http_client,
            )

        stats = await self._crawl_and_save(
            store,
            country_list,
            _fetch,
            lambda c: f"top_charts_{c}",
            client=client,
            on_progress=on_progress,
        )
        return {
            "total_discovered": stats["total_discovered"],
            "unique_saved": stats["unique_saved"],
            "countries_crawled": country_list,
        }

    async def crawl_topics(
        self,
        store: Store,
        topics: Sequence[str] = DEFAULT_TOPICS,
        limit_per_topic: int = 200,
        country: str = "us",
        min_episodes: Optional[int] = None,
        client: Optional[httpx.AsyncClient] = None,
        on_progress: Optional[Callable[[str, int], None]] = None,
    ) -> Dict[str, Any]:
        """
        Crawls podcasts across a list of keyword topics, immediately persisting discovered shows.
        Optionally filters for popular/established shows with at least `min_episodes`.
        """
        topic_list = list(topics)

        async def _fetch(topic: str, http_client: httpx.AsyncClient) -> List[Podcast]:
            logger.info("Searching podcasts for topic: '%s'...", topic)
            return await self.itunes_client.search_podcasts(
                query=topic,
                limit=limit_per_topic,
                country=country,
                min_episodes=min_episodes,
                client=http_client,
            )

        stats = await self._crawl_and_save(
            store,
            topic_list,
            _fetch,
            lambda t: t,
            client=client,
            on_progress=on_progress,
        )
        return {
            "total_discovered": stats["total_discovered"],
            "unique_saved": stats["unique_saved"],
            "topics_crawled": topic_list,
        }

    async def crawl_alphabetical_prefixes(
        self,
        store: Store,
        prefixes: Optional[Sequence[str]] = None,
        limit_per_prefix: int = 200,
        country: str = "us",
        client: Optional[httpx.AsyncClient] = None,
        on_progress: Optional[Callable[[str, int], None]] = None,
    ) -> Dict[str, Any]:
        """
        Systematic dictionary/prefix crawl (e.g. 'aa', 'ab', ..., 'zz') to discover the long tail of podcasts.
        """
        if prefixes is None:
            import string
            prefixes = [f"{a}{b}" for a in string.ascii_lowercase for b in string.ascii_lowercase]

        return await self.crawl_topics(
            store=store,
            topics=prefixes,
            limit_per_topic=limit_per_prefix,
            country=country,
            client=client,
            on_progress=on_progress,
        )

    async def resolve_and_save_ids(
        self,
        store: Store,
        collection_ids: Sequence[Union[int, str]],
        country: str = "us",
        client: Optional[httpx.AsyncClient] = None,
    ) -> Dict[str, Any]:
        """
        Takes an arbitrary list of Apple Collection IDs, batches them into chunks of 200,
        resolves show details and RSS URLs in single HTTP calls, and saves them immediately.

        Per-chunk failures (429-after-retries, timeouts, ...) are logged and the
        run continues with the next chunk instead of aborting the whole ID list.
        Returns the saved feeds plus the ``(start, end)`` index ranges of the
        chunks that failed, so a caller can retry just those ranges.
        """
        all_saved_feeds: List[Feed] = []
        failed_chunks: List[Tuple[int, int]] = []
        seen_feed_ids: Set[Any] = set()
        # XIN-49: shared client lifecycle.
        async with maybe_client(client) as client:
            # Chunk collection IDs into batches of up to 200
            for i in range(0, len(collection_ids), self.batch_size):
                chunk = list(collection_ids[i : i + self.batch_size])
                try:
                    podcasts = await self.itunes_client.lookup_podcasts_by_ids(
                        collection_ids=chunk,
                        country=country,
                        client=client,
                    )
                    saved_feeds = await self.service.save_podcasts(
                        store, podcasts
                    )
                    for f in saved_feeds:
                        if f.feed_id not in seen_feed_ids:
                            seen_feed_ids.add(f.feed_id)
                            all_saved_feeds.append(f)
                except Exception as e:
                    logger.error(
                        "Error resolving ID chunk [%d:%d]: %s",
                        i,
                        i + len(chunk),
                        str(e),
                    )
                    failed_chunks.append((i, i + len(chunk)))
                await asyncio.sleep(self.request_delay)

        return {"saved_feeds": all_saved_feeds, "failed_chunks": failed_chunks}

    # -------------------------------------------------------------------------
    # Stage 2: Depth Crawling (Concurrent Episode Downloading for LLM)
    # -------------------------------------------------------------------------

    async def sync_episodes_concurrently(
        self,
        concurrency: int = 5,
        max_feeds: Optional[int] = None,
        on_feed_synced: Optional[Callable[[str, int], None]] = None,
    ) -> Dict[str, Any]:
        """
        Spawns worker pool to download episodes for all discovered/pending feeds
        in the configured database with bounded async concurrency.
        """
        # 1. Fetch pending feed records
        async with session_scope() as store:
            pending_feeds = await store.feeds.list_by_statuses(
                [FeedSyncStatus.DISCOVERED.value, FeedSyncStatus.PENDING.value],
                limit=max_feeds,
            )
            pending_ids = [f.feed_id for f in pending_feeds]

        if not pending_ids:
            return {"total_feeds": 0, "synced": 0, "episodes_saved": 0, "failed": 0}

        semaphore = asyncio.Semaphore(concurrency)
        total_episodes_saved = 0
        successful_feeds = 0
        failed_feeds = 0

        async def _worker(feed_id: Any) -> None:
            nonlocal total_episodes_saved, successful_feeds, failed_feeds
            async with semaphore:
                try:
                    async with session_scope() as store:
                        feed, episodes = await self.service.sync_podcast_episodes(
                            store=store,
                            feed_or_id_or_url=feed_id,
                            auto_queue_episodes=get_auto_queue_episodes(),
                        )
                        successful_feeds += 1
                        total_episodes_saved += len(episodes)
                        if on_feed_synced:
                            on_feed_synced(feed.title, len(episodes))
                except IngestionError as err:
                    # XIN-53: the service records the failure on the feed row
                    # and raises WITHOUT logging; log exactly once here.
                    logger.warning(
                        "Failed to sync episodes for feed %s: %s: %s",
                        str(feed_id),
                        type(err).__name__,
                        str(err),
                    )
                    failed_feeds += 1
                except Exception as err:
                    # Non-ingestion failure (unexpected); still counted, logged once.
                    logger.warning(
                        "Unexpected error syncing feed %s: %s",
                        str(feed_id),
                        str(err),
                    )
                    failed_feeds += 1

        tasks = [_worker(fid) for fid in pending_ids]
        await asyncio.gather(*tasks)

        return {
            "total_feeds": len(pending_ids),
            "synced": successful_feeds,
            "episodes_saved": total_episodes_saved,
            "failed": failed_feeds,
        }
