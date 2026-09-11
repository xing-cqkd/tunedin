"""
Podcast discovery service (XIN-39 split).

``DiscoveryService`` owns iTunes/Apple Podcasts discovery and Feed upserts:
show registration ("Mode 1") and the combined discover-and-sync flows
("Mode 3", composed with ``FeedSyncService``).

Commit ownership: each public method commits exactly once on success
(``save_podcast`` also commits once on the duplicate-race retry path).
Callers must not wrap these calls in a transaction they expect to roll
back.

Raise-vs-skip contract (XIN-53), unchanged from the original service:
the single-podcast ``save_podcast`` RAISES FeedValidationError on a missing
feed_url (a caller bug); the batch sibling ``save_podcasts`` instead skips
such podcasts with a warning, so a bad item never aborts a whole crawl
batch.
"""
import logging

import httpx
from sqlalchemy.exc import IntegrityError

from backend.ingestion.canonicalize import canonicalize_feed_url
from backend.ingestion.errors import FeedValidationError
from backend.ingestion.itunes import ITunesSearchClient
from backend.ingestion.models import Podcast
from backend.ingestion.service import (
    PODCAST_METADATA_FIELDS,
    FeedSyncService,
    FeedSyncStatus,
    apply_feed_metadata,
)
from backend.persistence.models.episode import Episode
from backend.persistence.models.feed import Feed
from backend.persistence.repositories import Store

logger = logging.getLogger(__name__)


class DiscoveryService:
    """
    Podcast discovery and show registration (XIN-39).

    Owns the iTunes search client and all Feed upserts. Episode
    synchronization belongs to ``FeedSyncService``
    (``backend/ingestion/service.py``); LLM-pipeline queries belong to
    ``backend/insights/pipeline.py``.
    """

    def __init__(
        self,
        itunes_client: ITunesSearchClient | None = None,
    ):
        self.itunes_client = itunes_client or ITunesSearchClient()

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

        # XIN-44: identity is the canonical URL — look up and store the
        # canonical form so http/https, trailing-slash, port, and tracking-
        # param variants of the same feed resolve to one row.
        rss_url = canonicalize_feed_url(podcast.feed_url)

        feed = await store.feeds.get_by_rss_url(rss_url)

        if feed is None:
            feed = Feed(
                rss_url=rss_url,
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
            # Update show metadata if available (shared field-mapping loop,
            # XIN-54 — same mapping FeedSyncService uses for parsed metadata).
            feed = apply_feed_metadata(feed, podcast, PODCAST_METADATA_FIELDS)

        return await store.feeds.save(feed)

    async def save_podcast(self, store: Store, podcast: Podcast) -> Feed:
        """
        Immediately saves/upserts a discovered Podcast show into the `feeds` database table.
        Marks sync_status as DISCOVERED if new, ready for downstream episode syncing.

        Commits once. Duplicate-feed race (XIN-127): two writers can both see
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

    async def save_podcasts(self, store: Store, podcasts: list[Podcast]) -> list[Feed]:
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
        client: httpx.AsyncClient | None = None,
    ) -> list[Feed]:
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
        client: httpx.AsyncClient | None = None,
    ) -> list[Feed]:
        """
        Discovers top trending podcasts from Apple Charts and saves all shows immediately to DB.
        """
        podcasts = await self.itunes_client.get_top_podcasts(
            limit=limit,
            country=country,
            client=client,
        )
        return await self.save_podcasts(store, podcasts)

    async def ingest_podcast(
        self,
        store: Store,
        podcast: Podcast,
        sync_service: FeedSyncService | None = None,
        client: httpx.AsyncClient | None = None,
        auto_sync_episodes: bool = True,
    ) -> tuple[Feed, list[Episode]]:
        """
        Immediately saves a discovered Podcast entity, then downloads and saves its episodes.

        Composition of DiscoveryService (show registration, committed by
        ``save_podcast``) and FeedSyncService (episode sync, one commit per
        feed owned by the sync call). Pass ``sync_service`` to share a
        configured instance (e.g. one with a queue driver); a default
        ``FeedSyncService`` is constructed otherwise.
        """
        feed = await self.save_podcast(store, podcast)
        if auto_sync_episodes:
            sync = sync_service or FeedSyncService()
            return await sync.sync_podcast_episodes_by_feed(
                store, feed, client=client
            )
        return feed, []

    async def ingest_from_itunes(
        self,
        store: Store,
        itunes_podcast: Podcast,
        sync_service: FeedSyncService | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> tuple[Feed, list[Episode]]:
        """
        Alias for ingest_podcast for backwards compatibility.
        """
        return await self.ingest_podcast(
            store,
            podcast=itunes_podcast,
            sync_service=sync_service,
            client=client,
            auto_sync_episodes=True,
        )
