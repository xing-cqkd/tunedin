from datetime import datetime, timezone
from typing import List, Optional, Set
import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.ingestion.models import FeedParseResult, ParsedEpisode
from backend.ingestion.parser import PodcastFeedParser
from backend.persistence.models.episode import Episode
from backend.persistence.models.feed import Feed


class FeedIngestionService:
    """
    Service layer coordinating feed parsing and database synchronization.
    """

    def __init__(self, parser: Optional[PodcastFeedParser] = None):
        self.parser = parser or PodcastFeedParser()

    async def ingest_feed(
        self,
        db: AsyncSession,
        rss_url: str,
        client: Optional[httpx.AsyncClient] = None,
    ) -> tuple[Feed, List[Episode]]:
        """
        Synchronizes a podcast feed by URL with the database.
        Creates the Feed if non-existent, parses new episodes, and inserts them.
        """
        # 1. Look up existing feed
        stmt = select(Feed).where(Feed.rss_url == rss_url)
        res = await db.execute(stmt)
        feed = res.scalar_one_or_none()

        last_fetched_at = None
        etag = None
        last_modified = None
        known_guids: Set[str] = set()

        if feed is not None:
            etag = feed.etag
            last_modified = feed.last_modified
            # Query known episode GUIDs for this feed
            ep_stmt = select(Episode.guid).where(Episode.feed_id == feed.feed_id)
            ep_res = await db.execute(ep_stmt)
            known_guids = {g for g in ep_res.scalars().all() if g}
            last_fetched_at = feed.last_fetched_at

        # 2. Fetch and parse feed
        parse_result: FeedParseResult = await self.parser.fetch_and_parse(
            rss_url=rss_url,
            known_guids=known_guids if known_guids else None,
            etag=etag,
            last_modified=last_modified,
            client=client,
        )

        now_utc = datetime.now(timezone.utc)

        # 3. Create or update Feed model
        if feed is None:
            feed = Feed(
                rss_url=rss_url,
                title=parse_result.metadata.title or "Untitled Podcast",
                author=parse_result.metadata.author,
                description=parse_result.metadata.description,
                image_url=parse_result.metadata.image_url,
                category=parse_result.metadata.category,
                etag=parse_result.metadata.etag,
                last_modified=parse_result.metadata.last_modified,
                last_fetched_at=now_utc,
                sync_status="active",
            )
            db.add(feed)
            await db.flush()
        else:
            # Update sync metadata
            if not parse_result.is_not_modified and parse_result.metadata.title:
                feed.title = parse_result.metadata.title
                feed.author = parse_result.metadata.author or feed.author
                feed.description = parse_result.metadata.description or feed.description
                feed.image_url = parse_result.metadata.image_url or feed.image_url
                feed.category = parse_result.metadata.category or feed.category
            
            feed.etag = parse_result.metadata.etag or feed.etag
            feed.last_modified = parse_result.metadata.last_modified or feed.last_modified
            feed.last_fetched_at = now_utc
            feed.sync_status = "active"

        if parse_result.is_not_modified:
            await db.commit()
            await db.refresh(feed)
            return feed, []

        # 4. Insert new episode records
        new_episodes: List[Episode] = []
        for ep_data in parse_result.episodes:
            ep = Episode(
                feed_id=feed.feed_id,
                guid=ep_data.guid,
                title=ep_data.title,
                audio_url=ep_data.audio_url,
                duration=ep_data.duration,
                published_at=ep_data.published_at,
                summary=ep_data.summary,
                processed=False,
            )
            db.add(ep)
            new_episodes.append(ep)

        await db.commit()
        await db.refresh(feed)
        for ep in new_episodes:
            await db.refresh(ep)

        return feed, new_episodes
