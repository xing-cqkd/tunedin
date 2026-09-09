import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Union
import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.ingestion.itunes import ITunesSearchClient
from backend.ingestion.models import Podcast
from backend.ingestion.service import FeedIngestionService
from backend.ingestion.task_queue import get_queue_driver
from backend.settings import get_auto_queue_episodes, get_crawler_countries, session_scope
from backend.persistence.models.episode import Episode
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

# Major Apple Podcasts storefront countries
DEFAULT_COUNTRIES = ["us", "gb", "ca", "au", "de", "fr"]


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

    async def crawl_top_charts(
        self,
        db: AsyncSession,
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
        total_discovered = 0
        total_saved = 0
        visited_urls: Set[str] = set()

        close_client = False
        if client is None:
            client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
            close_client = True

        try:
            for country in country_list:
                try:
                    logger.info("Crawling top charts for country: %s...", country.upper())
                    podcasts = await self.itunes_client.get_top_podcasts(
                        limit=limit_per_chart,
                        country=country,
                        client=client,
                    )
                    total_discovered += len(podcasts)

                    # Filter duplicates within current crawl session
                    new_podcasts = [p for p in podcasts if p.feed_url and p.feed_url not in visited_urls]
                    for p in new_podcasts:
                        visited_urls.add(p.feed_url)

                    saved_feeds = await self.service.save_podcasts(db, new_podcasts)
                    total_saved += len(saved_feeds)

                    if on_progress:
                        on_progress(f"top_charts_{country}", len(saved_feeds))

                    await asyncio.sleep(self.request_delay)
                except Exception as e:
                    logger.error("Error crawling top charts for country %s: %s", country, str(e))
        finally:
            if close_client:
                await client.aclose()

        return {
            "total_discovered": total_discovered,
            "unique_saved": total_saved,
            "countries_crawled": country_list,
        }

    async def crawl_topics(
        self,
        db: AsyncSession,
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
        total_discovered = 0
        total_saved = 0
        visited_urls: Set[str] = set()

        close_client = False
        if client is None:
            client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
            close_client = True

        try:
            for topic in topics:
                try:
                    logger.info("Searching podcasts for topic: '%s'...", topic)
                    podcasts = await self.itunes_client.search_podcasts(
                        query=topic,
                        limit=limit_per_topic,
                        country=country,
                        min_episodes=min_episodes,
                        client=client,
                    )
                    total_discovered += len(podcasts)

                    new_podcasts = [p for p in podcasts if p.feed_url and p.feed_url not in visited_urls]
                    for p in new_podcasts:
                        visited_urls.add(p.feed_url)

                    saved_feeds = await self.service.save_podcasts(db, new_podcasts)
                    total_saved += len(saved_feeds)

                    if on_progress:
                        on_progress(topic, len(saved_feeds))

                    await asyncio.sleep(self.request_delay)
                except Exception as e:
                    logger.error("Error crawling topic '%s': %s", topic, str(e))
        finally:
            if close_client:
                await client.aclose()

        return {
            "total_discovered": total_discovered,
            "unique_saved": total_saved,
            "topics_crawled": list(topics),
        }

    async def crawl_alphabetical_prefixes(
        self,
        db: AsyncSession,
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
            db=db,
            topics=prefixes,
            limit_per_topic=limit_per_prefix,
            country=country,
            client=client,
            on_progress=on_progress,
        )

    async def resolve_and_save_ids(
        self,
        db: AsyncSession,
        collection_ids: Sequence[Union[int, str]],
        country: str = "us",
        client: Optional[httpx.AsyncClient] = None,
    ) -> List[Feed]:
        """
        Takes an arbitrary list of Apple Collection IDs, batches them into chunks of 200,
        resolves show details and RSS URLs in single HTTP calls, and saves them immediately.
        """
        all_saved_feeds: List[Feed] = []
        seen_feed_ids: Set[Any] = set()
        close_client = False
        if client is None:
            client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
            close_client = True

        try:
            # Chunk collection IDs into batches of up to 200
            for i in range(0, len(collection_ids), self.batch_size):
                chunk = list(collection_ids[i : i + self.batch_size])
                podcasts = await self.itunes_client.lookup_podcasts_by_ids(
                    collection_ids=chunk,
                    country=country,
                    client=client,
                )
                saved_feeds = await self.service.save_podcasts(db, podcasts)
                for f in saved_feeds:
                    if f.feed_id not in seen_feed_ids:
                        seen_feed_ids.add(f.feed_id)
                        all_saved_feeds.append(f)
                await asyncio.sleep(self.request_delay)
        finally:
            if close_client:
                await client.aclose()

        return all_saved_feeds

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
        async with session_scope() as session:
            stmt = select(Feed.feed_id).where(Feed.sync_status.in_(["discovered", "pending"]))
            if max_feeds:
                stmt = stmt.limit(max_feeds)
            res = await session.execute(stmt)
            pending_ids = list(res.scalars().all())

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
                    async with session_scope() as worker_session:
                        feed, episodes = await self.service.sync_podcast_episodes(
                            db=worker_session,
                            feed_or_id_or_url=feed_id,
                            auto_queue_episodes=get_auto_queue_episodes(),
                        )
                        successful_feeds += 1
                        total_episodes_saved += len(episodes)
                        if on_feed_synced:
                            on_feed_synced(feed.title, len(episodes))
                except Exception as err:
                    logger.warning("Failed to sync episodes for feed %s: %s", str(feed_id), str(err))
                    failed_feeds += 1

        tasks = [_worker(fid) for fid in pending_ids]
        await asyncio.gather(*tasks)

        return {
            "total_feeds": len(pending_ids),
            "synced": successful_feeds,
            "episodes_saved": total_episodes_saved,
            "failed": failed_feeds,
        }
