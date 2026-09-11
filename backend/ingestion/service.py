import logging
import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse
import httpx
from sqlalchemy.exc import IntegrityError

from backend.ingestion.errors import (
    FeedFetchError,
    FeedNotFoundError,
    FeedParseError,
    FeedSyncError,
    FeedValidationError,
)
from backend.ingestion.itunes import ITunesSearchClient
from backend.ingestion.models import FeedParseResult, ParsedEpisode, Podcast
from backend.ingestion.parser import PodcastFeedParser
from backend.ingestion.task_queue.base import TaskQueueDriver
from backend.persistence.models.episode import Episode
from backend.persistence.models.feed import Feed
from backend.persistence.repositories import Store

logger = logging.getLogger(__name__)


class FeedSyncStatus(str, Enum):
    """
    Lifecycle states of a Feed row's ``sync_status`` column (XIN-51).

    DISCOVERED: seen via discovery/crawl, episodes not yet synced.
    PENDING:    newly registered, awaiting its first sync.
    ACTIVE:    last sync succeeded.
    ERROR:     last sync failed; retried with exponential backoff (XIN-34).
    """

    DISCOVERED = "discovered"
    PENDING = "pending"
    ACTIVE = "active"
    ERROR = "error"


# Retry policy for feeds stuck in sync_status=ERROR (XIN-34). A transient
# fetch/parse failure must not strand a feed forever: errored feeds become
# eligible for retry after an exponential backoff based on consecutive
# error_count, and are abandoned after MAX attempts.
ERROR_RETRY_BASE_BACKOFF_SECONDS = 300  # 5 minutes; doubles per consecutive failure
ERROR_RETRY_MAX_ATTEMPTS = 10  # stop retrying after this many consecutive failures


def _error_retry_due(feed: "Feed", now: datetime) -> bool:
    """True if an errored feed's backoff window has elapsed and it may be retried."""
    if feed.error_count >= ERROR_RETRY_MAX_ATTEMPTS:
        return False
    # Note: no upper cap on the backoff — max attempts is 10 and
    # error_count=9 yields 76,800s of backoff, so a 1-day cap would never bind.
    backoff = ERROR_RETRY_BASE_BACKOFF_SECONDS * (2 ** max(feed.error_count - 1, 0))
    last = feed.last_fetched_at
    if last is not None and last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)  # SQLite returns naive datetimes
    return last is None or last <= now - timedelta(seconds=backoff)


def _looks_like_url(value: str) -> bool:
    """True if the string parses as an absolute http(s) URL."""
    try:
        parts = urlparse(value)
    except Exception:
        return False
    return parts.scheme in ("http", "https") and bool(parts.netloc)


class FeedIngestionService:
    """
    Service layer coordinating podcast discovery, immediate persistence,
    and multi-mode RSS feed episode synchronization for LLM processing pipelines.
    """

    def __init__(
        self,
        parser: Optional[PodcastFeedParser] = None,
        itunes_client: Optional[ITunesSearchClient] = None,
        queue_driver: Optional[TaskQueueDriver] = None,
    ):
        self.parser = parser or PodcastFeedParser()
        self.itunes_client = itunes_client or ITunesSearchClient()
        self.queue_driver = queue_driver

    # -------------------------------------------------------------------------
    # Mode 1: Show Discovery & Registration ("Find All Podcasts & Save Immediately")
    # -------------------------------------------------------------------------

    @staticmethod
    async def _mark_feed_error(store: Store, feed: Feed, now: datetime) -> Feed:
        """
        Records a sync failure on the feed row (status=ERROR, error_count+1,
        last_fetched_at) and commits. Per the XIN-53 error policy this is the
        durable record; the caller then raises a typed error WITHOUT logging
        again (the catching caller logs once, in its own format).
        """
        feed.error_count += 1
        feed.sync_status = FeedSyncStatus.ERROR.value
        feed.last_fetched_at = now
        feed = await store.feeds.save(feed)
        await store.commit()
        return feed

    @staticmethod
    def _apply_podcast_metadata(feed: Feed, podcast: Podcast) -> Feed:
        """Overlays discovered show metadata onto an existing Feed (no save)."""
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
        return feed

    async def _get_or_create_feed(self, store: Store, podcast: Podcast) -> Feed:
        """
        Read-then-write upsert of a discovered Podcast into the feeds table,
        WITHOUT committing. Marks sync_status as DISCOVERED if new.

        Raises FeedValidationError (a ValueError) when the podcast has no
        feed_url — the batch sibling save_podcasts instead skips-and-warns
        on such podcasts (see its docstring).
        """
        if not podcast.feed_url:
            raise FeedValidationError(
                "Cannot save podcast without a valid canonical feed_url"
            )

        feed = await store.feeds.get_by_rss_url(podcast.feed_url)

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
                sync_status=FeedSyncStatus.DISCOVERED.value,
                error_count=0,
            )
        else:
            # Update show metadata if available
            feed = self._apply_podcast_metadata(feed, podcast)

        return await store.feeds.save(feed)

    async def save_podcast(self, store: Store, podcast: Podcast) -> Feed:
        """
        Immediately saves/upserts a discovered Podcast show into the `feeds` database table.
        Marks sync_status as DISCOVERED if new, ready for downstream episode syncing.

        Raise-vs-skip contract (XIN-53): the single-podcast method RAISES
        FeedValidationError on a missing feed_url (a caller bug); the batch
        sibling save_podcasts instead skips such podcasts with a warning, so a
        bad item never aborts a whole crawl batch.

        Duplicate-feed race (XIN-127): two writers can both see
        ``get_by_rss_url -> None`` and both insert, violating the unique
        ``rss_url`` constraint. On IntegrityError the session is rolled back
        and the now-existing feed is re-fetched and updated instead of
        propagating (which would poison the session for the rest of a crawl).
        """
        try:
            feed = await self._get_or_create_feed(store, podcast)
            await store.commit()
        except IntegrityError:
            logger.warning(
                "Duplicate rss_url race for %s; adopting the existing feed",
                podcast.feed_url,
            )
            await store.rollback()
            feed = await self._get_or_create_feed(store, podcast)
            await store.commit()
        return feed

    async def save_podcasts(self, store: Store, podcasts: List[Podcast]) -> List[Feed]:
        """
        Immediately persists a batch of discovered podcasts into the `feeds` table.

        Raise-vs-skip contract (XIN-53): unlike save_podcast (which raises
        FeedValidationError on a missing feed_url), the batch SKIPS podcasts
        without a feed_url and logs a warning for each, so one bad item never
        aborts the batch.

        Batches the saves and commits once (XIN-129). On an IntegrityError
        (e.g. a duplicate-feed race mid-batch) rolls back and falls back to
        per-podcast saves, each with its own duplicate handling (XIN-127).
        """
        skipped = [p for p in podcasts if not p.feed_url]
        for p in skipped:
            logger.warning(
                "Skipping podcast %r: no feed_url (batch save skips, single save_podcast raises)",
                p.title,
            )
        candidates = [p for p in podcasts if p.feed_url]
        try:
            saved_feeds = [await self._get_or_create_feed(store, p) for p in candidates]
            await store.commit()
        except IntegrityError:
            await store.rollback()
            saved_feeds = [await self.save_podcast(store, p) for p in candidates]
        return saved_feeds

    async def discover_and_save_podcasts(
        self,
        store: Store,
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
        return await self.save_podcasts(store, podcasts)

    async def discover_top_and_save_podcasts(
        self,
        store: Store,
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
        return await self.save_podcasts(store, podcasts)

    # -------------------------------------------------------------------------
    # Mode 2: Episode Synchronization ("Given a Podcast, Download Episodes")
    # -------------------------------------------------------------------------

    async def sync_podcast_episodes(
        self,
        store: Store,
        feed_or_id_or_url: Union[Feed, uuid.UUID, str],
        client: Optional[httpx.AsyncClient] = None,
        auto_queue_episodes: int = 0,
    ) -> Tuple[Feed, List[Episode]]:
        """
        Given a Feed entity, feed_id, or rss_url:
        Fetches and parses the RSS/XML or JSON feed, and immediately inserts all new
        episodes into the database with `processed=False` (ready for LLM reading/tagging).

        Raises (XIN-53, typed errors in backend/ingestion/errors.py):
          FeedValidationError - bad identifier / missing feed_url (a ValueError)
          FeedNotFoundError   - feed ID/URL resolved to no row (a ValueError)
          FeedFetchError      - transport-level fetch failure; original chained
          FeedParseError      - fetched bytes were not a parseable feed (a ValueError)
        A failed fetch/parse marks the feed ERROR and commits before raising;
        the catching caller logs once — the service never logs at the raise site.
        """
        # 1. Resolve Feed record
        if isinstance(feed_or_id_or_url, Feed):
            feed = feed_or_id_or_url
        elif isinstance(feed_or_id_or_url, uuid.UUID):
            feed = await store.feeds.get_by_id(feed_or_id_or_url)
            if not feed:
                raise FeedNotFoundError(f"Feed with ID {feed_or_id_or_url} not found.")
        elif isinstance(feed_or_id_or_url, str):
            # Check if UUID string or URL
            try:
                feed_uuid = uuid.UUID(feed_or_id_or_url)
                feed = await store.feeds.get_by_id(feed_uuid)
            except ValueError:
                feed = await store.feeds.get_by_rss_url(feed_or_id_or_url)
            if not feed:
                # An invalid identifier (non-UUID, non-URL) must not create a
                # placeholder Feed row — validate before saving (XIN-128).
                if not _looks_like_url(feed_or_id_or_url):
                    raise FeedValidationError(
                        f"Invalid feed identifier {feed_or_id_or_url!r}: "
                        "expected a Feed, UUID, or http(s) URL"
                    )
                # If it's a URL and doesn't exist yet, create initial feed
                feed = Feed(
                    rss_url=feed_or_id_or_url,
                    title="Fetching Podcast...",
                    sync_status=FeedSyncStatus.PENDING.value,
                )
                feed = await store.feeds.save(feed)
                await store.commit()
        else:
            raise TypeError(f"Invalid feed identifier type: {type(feed_or_id_or_url)}")

        # 2. Query known GUIDs for this feed to perform incremental deduplication
        known_guids: Set[str] = await store.episodes.list_guids_by_feed(feed.feed_id)

        # 3. Fetch and parse feed with error recovery: record the failure on the
        # feed row (the durable record) and raise a TYPED error (XIN-53).
        # No logging here — the catching caller logs exactly once.
        now_utc = datetime.now(timezone.utc)
        try:
            parse_result: FeedParseResult = await self.parser.fetch_and_parse(
                rss_url=feed.rss_url,
                known_guids=known_guids if known_guids else None,
                etag=feed.etag,
                last_modified=feed.last_modified,
                client=client,
            )
        except httpx.HTTPError as err:
            feed = await self._mark_feed_error(store, feed, now_utc)
            raise FeedFetchError(
                f"Failed to fetch feed '{feed.rss_url}': {err}"
            ) from err
        except Exception as err:
            feed = await self._mark_feed_error(store, feed, now_utc)
            raise FeedParseError(
                f"Failed to parse feed '{feed.rss_url}': {err}"
            ) from err

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
        feed.sync_status = FeedSyncStatus.ACTIVE.value
        feed.error_count = 0

        if parse_result.is_not_modified:
            feed = await store.feeds.save(feed)
            await store.commit()
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
            new_episodes.append(ep)

        try:
            new_episodes = await store.episodes.save_many(new_episodes)
            feed = await store.feeds.save(feed)
            await store.commit()
        except Exception as err:
            # A persistence-step failure after a successful parse: raise typed
            # so the batch caller can mark the feed ERROR and continue (XIN-53).
            # The session may be in a failed state; the caller rolls back.
            raise FeedSyncError(
                f"Failed to persist episodes for feed '{feed.rss_url}': {err}"
            ) from err

        # 6. Optionally enqueue background tasks for downstream AI insight extraction
        if self.queue_driver and auto_queue_episodes > 0 and new_episodes:
            to_queue = new_episodes[-auto_queue_episodes:]
            for ep in to_queue:
                payload = {
                    "episode_id": str(ep.episode_id),
                    "feed_id": str(feed.feed_id),
                    "title": ep.title,
                    "audio_url": ep.audio_url,
                    "transcript_url": ep.transcript_url,
                }
                await self.queue_driver.enqueue(
                    task_type="PROCESS_EPISODE",
                    payload=payload,
                )

        return feed, new_episodes

    async def ingest_feed(
        self,
        store: Store,
        rss_url: str,
        client: Optional[httpx.AsyncClient] = None,
        auto_queue_episodes: int = 0,
    ) -> Tuple[Feed, List[Episode]]:
        """
        Convenience / backward compatible method to sync feed & episodes by RSS URL.
        """
        return await self.sync_podcast_episodes(
            store,
            feed_or_id_or_url=rss_url,
            client=client,
            auto_queue_episodes=auto_queue_episodes,
        )

    # -------------------------------------------------------------------------
    # Mode 3: Combined Discovery & Ingestion ("Register & Sync Immediately")
    # -------------------------------------------------------------------------

    async def ingest_podcast(
        self,
        store: Store,
        podcast: Podcast,
        client: Optional[httpx.AsyncClient] = None,
        auto_sync_episodes: bool = True,
    ) -> Tuple[Feed, List[Episode]]:
        """
        Immediately saves a discovered Podcast entity, then downloads and saves its episodes.
        """
        feed = await self.save_podcast(store, podcast)
        if auto_sync_episodes:
            return await self.sync_podcast_episodes(store, feed, client=client)
        return feed, []

    async def ingest_from_itunes(
        self,
        store: Store,
        itunes_podcast: Podcast,
        client: Optional[httpx.AsyncClient] = None,
    ) -> Tuple[Feed, List[Episode]]:
        """
        Alias for ingest_podcast for backwards compatibility.
        """
        return await self.ingest_podcast(store, podcast=itunes_podcast, client=client, auto_sync_episodes=True)

    # -------------------------------------------------------------------------
    # Mode 4: Batch Synchronization ("Batch Sync All Discovered/Pending Feeds")
    # -------------------------------------------------------------------------

    async def sync_all_pending_feeds(
        self,
        store: Store,
        max_feeds: Optional[int] = None,
        client: Optional[httpx.AsyncClient] = None,
    ) -> Dict[str, Any]:
        """
        Finds all feeds with sync_status in (DISCOVERED, PENDING), plus errored
        feeds whose retry backoff has elapsed, and downloads all new episodes
        into the database for each feed.

        Returns a summary dict; ``total_feeds_processed`` counts only feeds
        actually attempted (XIN-79), and ``max_feeds`` applies after the
        backoff/due filter so not-yet-due feeds never consume limit slots
        (XIN-80). ``skipped_backoff`` reports how many error feeds were
        filtered as not yet due.
        """
        now = datetime.now(timezone.utc)
        min_backoff_cutoff = now - timedelta(seconds=ERROR_RETRY_BASE_BACKOFF_SECONDS)
        pending = await store.feeds.list_by_statuses(
            [FeedSyncStatus.DISCOVERED.value, FeedSyncStatus.PENDING.value]
        )
        due_retry = await store.feeds.list_error_due_retry(
            min_backoff_cutoff, ERROR_RETRY_MAX_ATTEMPTS
        )
        # Reproduce the original single-query OR semantics: merge the two
        # created_at-ordered result sets (the stable sort keeps
        # discovered/pending feeds first on created_at ties).
        merged = sorted(pending + due_retry, key=lambda f: f.created_at)
        # XIN-79/XIN-80: apply the per-row backoff/due filter FIRST, then the
        # max_feeds slice — counting and limiting only feeds actually attempted.
        due_feeds = [
            f
            for f in merged
            if not (
                f.sync_status == FeedSyncStatus.ERROR.value
                and not _error_retry_due(f, now)
            )
        ]
        skipped_backoff = len(merged) - len(due_feeds)
        if max_feeds:
            due_feeds = due_feeds[:max_feeds]
        pending_feeds = due_feeds

        total_synced = 0
        total_episodes_saved = 0
        failed_feeds: List[str] = []

        # Capture PKs up front and re-fetch each feed inside the loop: a
        # rollback in one iteration expires every instance in the session, so
        # holding ORM objects across iterations would trigger implicit IO
        # (MissingGreenlet) on the next attribute access.
        pending_ids = [f.feed_id for f in pending_feeds]

        for feed_id in pending_ids:
            feed = await store.feeds.get_by_id(feed_id)
            if feed is None:
                continue
            # Capture display fields now: sync_podcast_episodes may commit or
            # leave the session in a failed state, expiring this instance.
            rss_url = feed.rss_url
            # Defensive re-check of the exact per-feed backoff window (the
            # list above was already due-filtered; this only fires on races).
            if feed.sync_status == FeedSyncStatus.ERROR.value and not _error_retry_due(
                feed, now
            ):
                continue
            pre_error_count = feed.error_count
            try:
                _, new_eps = await self.sync_podcast_episodes(store, feed, client=client)
                total_synced += 1
                total_episodes_saved += len(new_eps)
            except Exception as e:
                logger.warning("Failed batch sync for feed %s: %s", rss_url, str(e))
                # XIN-127: a write failure (e.g. a duplicate-episode race in
                # save_many) must not poison the shared session for the
                # remaining feeds — roll back before continuing.
                await store.rollback()
                feed = await store.feeds.get_by_id(feed_id)
                if feed is not None and feed.error_count == pre_error_count:
                    # The fetch/parse path inside sync_podcast_episodes marks
                    # and commits the error state itself (bumping error_count);
                    # only mark here when it didn't (e.g. a persistence-step
                    # failure after a successful fetch, raised as FeedSyncError).
                    feed.error_count = pre_error_count + 1
                    feed.sync_status = FeedSyncStatus.ERROR.value
                    feed.last_fetched_at = now
                    feed = await store.feeds.save(feed)
                    await store.commit()
                failed_feeds.append(str(feed_id))

        return {
            "total_feeds_processed": len(pending_feeds),
            "total_synced": total_synced,
            "total_episodes_saved": total_episodes_saved,
            "failed_count": len(failed_feeds),
            "failed_feed_ids": failed_feeds,
            "skipped_backoff": skipped_backoff,
        }

    # -------------------------------------------------------------------------
    # LLM Pipeline Query & Status Management Helpers
    # -------------------------------------------------------------------------

    async def get_unprocessed_episodes(
        self,
        store: Store,
        feed_id: Optional[uuid.UUID] = None,
        limit: int = 50,
    ) -> List[Episode]:
        """
        Retrieves episodes waiting to be read, analyzed, and tagged by the LLM (processed=False).
        Ordered chronologically descending.
        """
        return await store.episodes.list_unprocessed(feed_id=feed_id, limit=limit)

    async def mark_episode_processed(
        self,
        store: Store,
        episode_id: uuid.UUID,
        processed: bool = True,
    ) -> Optional[Episode]:
        """
        Updates the LLM processing status for an episode once insights and tags have been saved.
        """
        episode = await store.episodes.mark_processed(episode_id, processed)
        await store.commit()
        return episode
