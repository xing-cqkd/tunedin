import logging
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.ingestion.itunes import ITunesSearchClient
from backend.ingestion.models import FeedParseResult, ParsedEpisode, Podcast
from backend.ingestion.parser import PodcastFeedParser
from backend.persistence.models.episode import Episode
from backend.persistence.models.feed import Feed

logger = logging.getLogger(__name__)


class IngestionMode(str, Enum):
    """Modes of podcast data ingestion."""
    DISCOVER_ONLY = "discover_only"       # Save show/feed metadata immediately without downloading episodes
    SYNC_EPISODES = "sync_episodes"       # Given a feed/show, fetch and save all new episodes
    FULL_PIPELINE = "full_pipeline"       # Discover show metadata AND download episodes immediately
    BATCH_PENDING = "batch_pending"       # Sync all pending/un-synced feeds in database


class FeedIngestionService:
    """
    Service layer coordinating podcast discovery, immediate persistence,
    and multi-mode RSS feed episode synchronization for LLM processing pipelines.
    """

    def __init__(
        self,
        parser: Optional[PodcastFeedParser] = None,
        itunes_client: Optional[ITunesSearchClient] = None,
    ):
        self.parser = parser or PodcastFeedParser()
        self.itunes_client = itunes_client or ITunesSearchClient()

    # -------------------------------------------------------------------------
    # Mode 1: Show Discovery & Registration ("Find All Podcasts & Save Immediately")
    # -------------------------------------------------------------------------

    async def save_podcast(self, db: AsyncSession, podcast: Podcast) -> Feed:
        """
        Immediately saves/upserts a discovered Podcast show into the `feeds` database table.
        Marks sync_status as 'discovered' if new, ready for downstream episode syncing.
        """
        if not podcast.feed_url:
            raise ValueError("Cannot save podcast without a valid canonical feed_url")

        stmt = select(Feed).where(Feed.rss_url == podcast.feed_url)
        res = await db.execute(stmt)
        feed = res.scalar_one_or_none()

        if feed is None:
            feed = Feed(
                rss_url=podcast.feed_url,
                title=podcast.title or "Untitled Podcast",
                author=podcast.author,
                description=podcast.description,
                image_url=podcast.artwork_url,
                category=podcast.primary_genre,
                language=podcast.language,
                website_url=podcast.website_url,
                sync_status="discovered",
                error_count=0,
            )
            db.add(feed)
        else:
            # Update show metadata if available
            if podcast.title:
                feed.title = podcast.title
            if podcast.author:
                feed.author = podcast.author
            if podcast.description:
                feed.description = podcast.description
            if podcast.artwork_url:
                feed.image_url = podcast.artwork_url
            if podcast.primary_genre:
                feed.category = podcast.primary_genre
            if podcast.language:
                feed.language = podcast.language
            if podcast.website_url:
                feed.website_url = podcast.website_url

        await db.commit()
        await db.refresh(feed)
        return feed

    async def save_podcasts(self, db: AsyncSession, podcasts: List[Podcast]) -> List[Feed]:
        """
        Immediately persists a batch of discovered podcasts into the `feeds` table.
        """
        saved_feeds: List[Feed] = []
        for p in podcasts:
            if p.feed_url:
                feed = await self.save_podcast(db, p)
                saved_feeds.append(feed)
        return saved_feeds

    async def discover_and_save_podcasts(
        self,
        db: AsyncSession,
        query: str,
        limit: int = 20,
        country: str = "US",
        client: Optional[httpx.AsyncClient] = None,
    ) -> List[Feed]:
        """
        Discovers podcasts via keyword query and saves all matched shows immediately to DB.
        """
        podcasts = await self.itunes_client.search_podcasts(
            query=query,
            limit=limit,
            country=country,
            client=client,
        )
        return await self.save_podcasts(db, podcasts)

    async def discover_top_and_save_podcasts(
        self,
        db: AsyncSession,
        limit: int = 25,
        country: str = "US",
        client: Optional[httpx.AsyncClient] = None,
    ) -> List[Feed]:
        """
        Discovers top trending podcasts from Apple Charts and saves all shows immediately to DB.
        """
        podcasts = await self.itunes_client.get_top_podcasts(
            limit=limit,
            country=country,
            client=client,
        )
        return await self.save_podcasts(db, podcasts)

    # -------------------------------------------------------------------------
    # Mode 2: Episode Synchronization ("Given a Podcast, Download Episodes")
    # -------------------------------------------------------------------------

    async def sync_podcast_episodes(
        self,
        db: AsyncSession,
        feed_or_id_or_url: Union[Feed, uuid.UUID, str],
        client: Optional[httpx.AsyncClient] = None,
    ) -> Tuple[Feed, List[Episode]]:
        """
        Given a Feed entity, feed_id, or rss_url:
        Fetches and parses the RSS/XML or JSON feed, and immediately inserts all new
        episodes into the database with `processed=False` (ready for LLM reading/tagging).
        """
        # 1. Resolve Feed record
        if isinstance(feed_or_id_or_url, Feed):
            feed = feed_or_id_or_url
        elif isinstance(feed_or_id_or_url, uuid.UUID):
            stmt = select(Feed).where(Feed.feed_id == feed_or_id_or_url)
            res = await db.execute(stmt)
            feed = res.scalar_one_or_none()
            if not feed:
                raise ValueError(f"Feed with ID {feed_or_id_or_url} not found.")
        elif isinstance(feed_or_id_or_url, str):
            # Check if UUID string or URL
            try:
                feed_uuid = uuid.UUID(feed_or_id_or_url)
                stmt = select(Feed).where(Feed.feed_id == feed_uuid)
            except ValueError:
                stmt = select(Feed).where(Feed.rss_url == feed_or_id_or_url)
            res = await db.execute(stmt)
            feed = res.scalar_one_or_none()
            if not feed:
                # If it's a URL and doesn't exist yet, create initial feed
                feed = Feed(
                    rss_url=feed_or_id_or_url,
                    title="Fetching Podcast...",
                    sync_status="pending",
                )
                db.add(feed)
                await db.commit()
                await db.refresh(feed)
        else:
            raise TypeError(f"Invalid feed identifier type: {type(feed_or_id_or_url)}")

        # 2. Query known GUIDs for this feed to perform incremental deduplication
        ep_stmt = select(Episode.guid).where(Episode.feed_id == feed.feed_id)
        ep_res = await db.execute(ep_stmt)
        known_guids: Set[str] = {g for g in ep_res.scalars().all() if g}

        # 3. Fetch and parse feed with error recovery
        now_utc = datetime.now(timezone.utc)
        try:
            parse_result: FeedParseResult = await self.parser.fetch_and_parse(
                rss_url=feed.rss_url,
                known_guids=known_guids if known_guids else None,
                etag=feed.etag,
                last_modified=feed.last_modified,
                client=client,
            )
        except Exception as err:
            logger.error("Failed to fetch/parse feed '%s': %s", feed.rss_url, str(err))
            feed.error_count += 1
            feed.sync_status = "error"
            feed.last_fetched_at = now_utc
            await db.commit()
            await db.refresh(feed)
            raise

        # 4. Update Feed metadata
        if not parse_result.is_not_modified and parse_result.metadata.title:
            feed.title = parse_result.metadata.title
            if parse_result.metadata.author:
                feed.author = parse_result.metadata.author
            if parse_result.metadata.description:
                feed.description = parse_result.metadata.description
            if parse_result.metadata.image_url:
                feed.image_url = parse_result.metadata.image_url
            if parse_result.metadata.category:
                feed.category = parse_result.metadata.category
            if parse_result.metadata.language:
                feed.language = parse_result.metadata.language
            if parse_result.metadata.website_url:
                feed.website_url = parse_result.metadata.website_url
            if parse_result.metadata.feed_type:
                feed.feed_type = parse_result.metadata.feed_type
            if parse_result.metadata.podcast_guid:
                feed.podcast_guid = parse_result.metadata.podcast_guid
            if parse_result.metadata.explicit is not None:
                feed.explicit = parse_result.metadata.explicit

        feed.etag = parse_result.metadata.etag or feed.etag
        feed.last_modified = parse_result.metadata.last_modified or feed.last_modified
        feed.last_fetched_at = now_utc
        feed.sync_status = "active"
        feed.error_count = 0

        if parse_result.is_not_modified:
            await db.commit()
            await db.refresh(feed)
            return feed, []

        # 5. Sort candidate episodes chronologically from earliest to latest
        def _chronological_sort_key(ep: ParsedEpisode) -> tuple:
            dt = ep.published_at
            if dt is not None:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
            else:
                dt = datetime.min.replace(tzinfo=timezone.utc)
            ep_num = ep.episode_number if ep.episode_number is not None else 0
            return (dt, ep_num)

        if all(ep.published_at is None for ep in parse_result.episodes):
            sorted_episodes = list(reversed(parse_result.episodes))
        else:
            sorted_episodes = sorted(parse_result.episodes, key=_chronological_sort_key)

        # Insert new Episode records (ready for LLM ingestion)
        new_episodes: List[Episode] = []
        for ep_data in sorted_episodes:
            ep = Episode(
                feed_id=feed.feed_id,
                guid=ep_data.guid,
                title=ep_data.title,
                audio_url=ep_data.audio_url,
                duration=ep_data.duration,
                published_at=ep_data.published_at,
                summary=ep_data.summary,
                content_html=ep_data.content_html,
                transcript_url=ep_data.transcript_url,
                chapters_url=ep_data.chapters_url,
                image_url=ep_data.image_url,
                episode_type=ep_data.episode_type,
                episode_number=ep_data.episode_number,
                season_number=ep_data.season_number,
                explicit=ep_data.explicit,
                processed=False,
            )
            db.add(ep)
            new_episodes.append(ep)

        await db.commit()
        await db.refresh(feed)
        for ep in new_episodes:
            await db.refresh(ep)

        return feed, new_episodes

    async def ingest_feed(
        self,
        db: AsyncSession,
        rss_url: str,
        client: Optional[httpx.AsyncClient] = None,
    ) -> Tuple[Feed, List[Episode]]:
        """
        Convenience / backward compatible method to sync feed & episodes by RSS URL.
        """
        return await self.sync_podcast_episodes(db, feed_or_id_or_url=rss_url, client=client)

    # -------------------------------------------------------------------------
    # Mode 3: Combined Discovery & Ingestion ("Register & Sync Immediately")
    # -------------------------------------------------------------------------

    async def ingest_podcast(
        self,
        db: AsyncSession,
        podcast: Podcast,
        client: Optional[httpx.AsyncClient] = None,
        auto_sync_episodes: bool = True,
    ) -> Tuple[Feed, List[Episode]]:
        """
        Immediately saves a discovered Podcast entity, then downloads and saves its episodes.
        """
        feed = await self.save_podcast(db, podcast)
        if auto_sync_episodes:
            return await self.sync_podcast_episodes(db, feed, client=client)
        return feed, []

    async def ingest_from_itunes(
        self,
        db: AsyncSession,
        itunes_podcast: Podcast,
        client: Optional[httpx.AsyncClient] = None,
    ) -> Tuple[Feed, List[Episode]]:
        """
        Alias for ingest_podcast for backwards compatibility.
        """
        return await self.ingest_podcast(db, podcast=itunes_podcast, client=client, auto_sync_episodes=True)

    # -------------------------------------------------------------------------
    # Mode 4: Batch Synchronization ("Batch Sync All Discovered/Pending Feeds")
    # -------------------------------------------------------------------------

    async def sync_all_pending_feeds(
        self,
        db: AsyncSession,
        max_feeds: Optional[int] = None,
        client: Optional[httpx.AsyncClient] = None,
    ) -> Dict[str, Any]:
        """
        Finds all feeds with sync_status in ('discovered', 'pending') or last_fetched_at is None,
        and downloads all new episodes into the database for each feed.
        """
        stmt = (
            select(Feed)
            .where(Feed.sync_status.in_(["discovered", "pending"]))
            .order_by(Feed.created_at.asc())
        )
        if max_feeds:
            stmt = stmt.limit(max_feeds)

        res = await db.execute(stmt)
        pending_feeds = res.scalars().all()

        total_synced = 0
        total_episodes_saved = 0
        failed_feeds: List[str] = []

        for feed in pending_feeds:
            try:
                _, new_eps = await self.sync_podcast_episodes(db, feed, client=client)
                total_synced += 1
                total_episodes_saved += len(new_eps)
            except Exception as e:
                logger.warning("Failed batch sync for feed %s: %s", feed.rss_url, str(e))
                failed_feeds.append(str(feed.feed_id))

        return {
            "total_feeds_processed": len(pending_feeds),
            "total_synced": total_synced,
            "total_episodes_saved": total_episodes_saved,
            "failed_count": len(failed_feeds),
            "failed_feed_ids": failed_feeds,
        }

    # -------------------------------------------------------------------------
    # LLM Pipeline Query & Status Management Helpers
    # -------------------------------------------------------------------------

    async def get_unprocessed_episodes(
        self,
        db: AsyncSession,
        feed_id: Optional[uuid.UUID] = None,
        limit: int = 50,
    ) -> List[Episode]:
        """
        Retrieves episodes waiting to be read, analyzed, and tagged by the LLM (processed=False).
        Ordered chronologically descending.
        """
        stmt = select(Episode).where(Episode.processed == False)
        if feed_id is not None:
            stmt = stmt.where(Episode.feed_id == feed_id)
        stmt = stmt.order_by(Episode.published_at.desc().nullslast()).limit(limit)

        res = await db.execute(stmt)
        return list(res.scalars().all())

    async def mark_episode_processed(
        self,
        db: AsyncSession,
        episode_id: uuid.UUID,
        processed: bool = True,
    ) -> Optional[Episode]:
        """
        Updates the LLM processing status for an episode once insights and tags have been saved.
        """
        stmt = select(Episode).where(Episode.episode_id == episode_id)
        res = await db.execute(stmt)
        episode = res.scalar_one_or_none()
        if episode:
            episode.processed = processed
            await db.commit()
            await db.refresh(episode)
        return episode
