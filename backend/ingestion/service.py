"""
Feed episode synchronization service (XIN-39 split).

This module owns RSS/Atom feed episode synchronization:

- ``FeedSyncStatus``: the lifecycle enum for a Feed row's ``sync_status``
  column (XIN-51).
- Error-retry policy for feeds stuck in ``sync_status=ERROR`` (XIN-34).
- ``FeedSyncService``: fetch/parse -> Episode inserts. Each public sync
  method performs exactly ONE commit per feed (success or recorded-error
  path); callers must not wrap sync calls in a transaction they expect to
  roll back, and must roll back their own session if a ``FeedSyncError``
  leaves it in a failed state.
- Shared feed-metadata overlay helpers (``apply_feed_metadata`` +
  field maps), also used by ``DiscoveryService``
  (``backend/ingestion/discovery.py``) so the two metadata blocks can't
  drift apart again (XIN-54).

Discovery (iTunes -> Feed upserts) lives in
``backend/ingestion/discovery.py``; LLM-pipeline queries live in
``backend/insights/pipeline.py``.
"""
import asyncio
import logging
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import httpx

from backend.ingestion.canonicalize import canonicalize_feed_url
from backend.ingestion.errors import (
    FeedFetchError,
    FeedNotFoundError,
    FeedParseError,
    FeedSyncError,
)
from backend.ingestion.http_util import validate_feed_url
from backend.ingestion.models import FeedParseResult, ParsedEpisode, ParsedFeedMetadata
from backend.ingestion.parser import PodcastFeedParser
from backend.ingestion.task_queue.base import TaskQueueDriver
from backend.ingestion.task_queue.schemas import (
    PROCESS_EPISODE_TASK_TYPE,
    ProcessEpisodePayload,
)
from backend.persistence.models.episode import Episode
from backend.persistence.models.feed import Feed
from backend.persistence.models.task_log import TaskLog
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


# Error-retry backoff policy (XIN-34) now lives in
# backend/ingestion/orchestration.py alongside the single feed-selection
# implementation; this module no longer owns "which feeds are due".


# ---------------------------------------------------------------------------
# Shared feed-metadata overlay (XIN-54)
# ---------------------------------------------------------------------------
# (source_attr, feed_attr) pairs overlaid onto a Feed when the source value is
# truthy. One mapping per source type, shared by DiscoveryService
# (Podcast -> Feed) and FeedSyncService (ParsedFeedMetadata -> Feed).

PODCAST_METADATA_FIELDS: tuple[tuple[str, str], ...] = (
    ("title", "title"),
    ("author", "author"),
    ("description", "description"),
    ("artwork_url", "image_url"),
    ("primary_genre", "category"),
    ("language", "language"),
    ("website_url", "website_url"),
)

PARSED_METADATA_FIELDS: tuple[tuple[str, str], ...] = (
    ("title", "title"),
    ("author", "author"),
    ("description", "description"),
    ("image_url", "image_url"),
    ("category", "category"),
    ("language", "language"),
    ("website_url", "website_url"),
    ("feed_type", "feed_type"),
    ("podcast_guid", "podcast_guid"),
)


def apply_feed_metadata(
    feed: Feed,
    source: Any,
    field_map: tuple[tuple[str, str], ...] = PODCAST_METADATA_FIELDS,
) -> Feed:
    """
    Overlay truthy attributes from ``source`` onto ``feed`` per ``field_map``.
    Pure in-memory mutation — no save, no commit. ``explicit``-style
    tri-state booleans (where False is meaningful) are NOT handled here;
    handle them with an ``is not None`` check at the call site.
    """
    for src_attr, feed_attr in field_map:
        value = getattr(source, src_attr, None)
        if value:
            setattr(feed, feed_attr, value)
    return feed


def _apply_parsed_metadata(feed: Feed, metadata: ParsedFeedMetadata) -> Feed:
    """
    Overlay parsed RSS metadata onto a Feed (no save). ``explicit`` is applied
    on ``is not None`` — False is meaningful (a show explicitly marked
    non-explicit) — while every other field is truthiness-gated by the
    shared field-mapping loop.
    """
    apply_feed_metadata(feed, metadata, PARSED_METADATA_FIELDS)
    if metadata.explicit is not None:
        feed.explicit = metadata.explicit
    return feed


def _chronological_sort_key(ep: ParsedEpisode) -> tuple:
    """Module-level sort key (XIN-54): earliest first, episode_number breaks ties."""
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


def _sort_episodes_chronologically(
    episodes: list[ParsedEpisode],
) -> list[ParsedEpisode]:
    """
    Earliest-first ordering for episode inserts. Feeds with no publish dates
    at all keep the parser's document order (reversed, so the feed's newest
    entry — typically listed first — inserts last).
    """
    if all(ep.published_at is None for ep in episodes):
        return list(reversed(episodes))
    return sorted(episodes, key=_chronological_sort_key)


# Task statuses that block re-enqueue of the same (task_type, episode_id)
# pair (XIN-45 idempotency). Any other status is terminal: re-recording
# resets the row to "queued" with the fresh payload.
_ACTIVE_TASK_STATUSES = frozenset({"pending", "queued", "processing"})


class FeedSyncService:
    """
    RSS/Atom feed episode synchronization (XIN-39).

    Owns the feed parser and the task-queue driver. Public entry points are
    explicit about their input kind (XIN-65):

    - ``sync_podcast_episodes_by_feed``: core implementation, takes a Feed.
    - ``sync_podcast_episodes_by_id``: resolves a feed_id, FeedNotFoundError
      on miss.
    - ``sync_podcast_episodes_by_url``: resolves an rss_url, creating the
      Feed row on miss (documented create-on-miss).

    Commit ownership (XIN-39): each ``sync_*`` method performs exactly ONE
    commit per feed — the success path commits metadata + episodes together;
    the fetch/parse error path commits the ERROR-state feed row, then raises
    the typed error. Callers must not wrap sync calls in their own
    transaction, and on ``FeedSyncError`` (persistence-step failure) the
    session is left uncommitted — possibly in a failed state — so the caller
    must roll back before reuse.
    """

    def __init__(
        self,
        parser: PodcastFeedParser | None = None,
        queue_driver: TaskQueueDriver | None = None,
    ):
        self.parser = parser or PodcastFeedParser()
        self.queue_driver = queue_driver

    # ------------------------------------------------------------------
    # Private pipeline steps (XIN-54 decomposition)
    # ------------------------------------------------------------------

    @staticmethod
    async def _mark_feed_error(store: Store, feed: Feed, now: datetime) -> None:
        """
        Records a sync failure on the feed row (status=ERROR, error_count+1,
        last_fetched_at) and commits — the single commit of the error path.
        Per the XIN-53 error policy this is the durable record; the caller
        then raises a typed error WITHOUT logging again (the catching caller
        logs once, in its own format).
        """
        feed.error_count += 1
        feed.sync_status = FeedSyncStatus.ERROR.value
        feed.last_fetched_at = now
        await store.feeds.save(feed)
        await store.commit()

    async def _fetch_and_parse_feed(
        self,
        store: Store,
        feed: Feed,
        known_guids: set[str],
        client: httpx.AsyncClient | None,
    ) -> FeedParseResult:
        """
        Fetch + parse one feed. On failure marks the feed ERROR (one commit)
        and raises the typed error — no logging at the raise site (XIN-53).
        """
        now_utc = datetime.now(timezone.utc)
        try:
            # XIN-62: SSRF gate at the service boundary, before any fetch.
            # DNS resolution blocks, so run it off the event loop. A rejected
            # URL marks the feed errored like any other terminal fetch failure.
            rss_url = await asyncio.to_thread(validate_feed_url, feed.rss_url)
            return await self.parser.fetch_and_parse(
                rss_url=rss_url,
                known_guids=known_guids if known_guids else None,
                etag=feed.etag,
                last_modified=feed.last_modified,
                client=client,
            )
        except httpx.HTTPError as err:
            await self._mark_feed_error(store, feed, now_utc)
            raise FeedFetchError(
                f"Failed to fetch feed '{feed.rss_url}': {err}"
            ) from err
        except Exception as err:
            await self._mark_feed_error(store, feed, now_utc)
            raise FeedParseError(
                f"Failed to parse feed '{feed.rss_url}': {err}"
            ) from err

    async def _persist_episodes(
        self,
        store: Store,
        feed: Feed,
        sorted_episodes: list[ParsedEpisode],
    ) -> tuple[Feed, list[Episode]]:
        """
        Bulk-insert Episode rows (``processed=False``, ready for LLM
        ingestion) and save the feed's updated metadata.

        WITHOUT committing — the caller (``sync_podcast_episodes_by_feed``)
        records the task-outbox rows and then performs the single commit of
        the success path (XIN-39). On persistence failure the exception
        propagates and the caller raises ``FeedSyncError`` without
        committing; the session may be in a failed state and the caller
        must roll back (XIN-53).
        """
        new_episodes = [
            Episode(
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
            for ep_data in sorted_episodes
        ]
        new_episodes = await store.episodes.save_many(new_episodes)
        feed = await store.feeds.save(feed)
        return feed, new_episodes

    @staticmethod
    def _process_episode_payload(feed: Feed, ep: Episode) -> ProcessEpisodePayload:
        """Build the shared PROCESS_EPISODE payload (XIN-41) for one episode."""
        return ProcessEpisodePayload(
            episode_id=str(ep.episode_id),
            feed_id=str(feed.feed_id),
            title=ep.title,
            audio_url=ep.audio_url,
            transcript_url=ep.transcript_url,
        )

    async def _record_task_outbox(
        self,
        store: Store,
        feed: Feed,
        new_episodes: list[Episode],
        auto_queue_episodes: int,
    ) -> list[Episode]:
        """Record TaskLog outbox rows for episodes queued for downstream LLM
        work (XIN-45).

        WITHOUT committing — the rows ride the sync's single commit, so
        the episode insert and its durable queue record are atomic (durable
        outbox). Returns the episodes the queue driver should be asked to
        enqueue.

        Idempotency: the (task_type, episode_id) pair is the idempotency key
        (unique index ``uq_task_log_type_episode``). Re-recording while the
        task is still active (pending/queued/processing) is a no-op; a task
        in a terminal state is reset to ``queued`` with the fresh payload.
        """
        if auto_queue_episodes <= 0 or not new_episodes:
            return []
        to_enqueue: list[Episode] = []
        for ep in new_episodes[-auto_queue_episodes:]:
            payload = self._process_episode_payload(feed, ep)
            existing = await store.task_logs.get_by_type_and_episode(
                PROCESS_EPISODE_TASK_TYPE, ep.episode_id
            )
            if existing is not None:
                if existing.status in _ACTIVE_TASK_STATUSES:
                    continue  # idempotent no-op: already queued/processing
                existing.status = "queued"
                existing.payload_json = payload.model_dump_json()
                existing.error_message = None
                await store.task_logs.save(existing)
            else:
                await store.task_logs.save(
                    TaskLog(
                        task_type=PROCESS_EPISODE_TASK_TYPE,
                        episode_id=ep.episode_id,
                        payload_json=payload.model_dump_json(),
                        status="queued",
                    )
                )
            to_enqueue.append(ep)
        return to_enqueue

    async def _enqueue_episode_tasks(
        self,
        feed: Feed,
        episodes: list[Episode],
    ) -> None:
        """Enqueue episodes to the queue driver (XIN-41 shared payload schema).

        External side effect — call only AFTER the outbox rows are
        committed, with exactly the episodes :meth:`_record_task_outbox`
        returned.
        """
        if self.queue_driver and episodes:
            for ep in episodes:
                payload = self._process_episode_payload(feed, ep)
                await self.queue_driver.enqueue(
                    task_type=PROCESS_EPISODE_TASK_TYPE,
                    payload=payload.model_dump(),
                )

    # ------------------------------------------------------------------
    # Public entry points (XIN-65: explicit input kinds, no type-sniffing)
    # ------------------------------------------------------------------

    async def sync_podcast_episodes_by_feed(
        self,
        store: Store,
        feed: Feed,
        client: httpx.AsyncClient | None = None,
        auto_queue_episodes: int = 0,
    ) -> tuple[Feed, list[Episode]]:
        """
        Core sync implementation: fetch + parse ``feed``, insert new episodes
        with ``processed=False`` (ready for LLM reading/tagging).

        Exactly one commit per feed (XIN-39): metadata + episodes on success;
        the ERROR-state row on fetch/parse failure.

        Raises (XIN-53, typed errors in backend/ingestion/errors.py):
          FeedFetchError - transport-level fetch failure; original chained
          FeedParseError - fetched bytes were not a parseable feed (a ValueError)
          FeedSyncError  - persistence-step failure after a successful parse
        The catching caller logs once — the service never logs at the raise site.
        """
        # 1. Known GUIDs for incremental deduplication.
        known_guids: set[str] = await store.episodes.list_guids_by_feed(feed.feed_id)

        # 2. Fetch and parse (marks feed ERROR + commits once, then raises
        #    typed, on failure).
        parse_result = await self._fetch_and_parse_feed(
            store, feed, known_guids, client
        )

        # 3. Overlay parsed metadata (in memory; persisted by the single
        #    commit below). Gated on a truthy title, as before: a feed whose
        #    metadata carries no title keeps its existing fields.
        now_utc = datetime.now(timezone.utc)
        if not parse_result.is_not_modified and parse_result.metadata.title:
            feed = _apply_parsed_metadata(feed, parse_result.metadata)

        feed.etag = parse_result.metadata.etag or feed.etag
        feed.last_modified = parse_result.metadata.last_modified or feed.last_modified
        feed.last_fetched_at = now_utc
        feed.sync_status = FeedSyncStatus.ACTIVE.value
        feed.error_count = 0

        # 4. Not-modified: persist the refreshed feed row and stop.
        if parse_result.is_not_modified:
            feed = await store.feeds.save(feed)
            await store.commit()
            return feed, []

        # 5. Persist episodes + feed metadata and record the task-outbox
        #    rows (XIN-45), then commit ONCE — the single commit of the
        #    success path (XIN-39).
        sorted_episodes = _sort_episodes_chronologically(parse_result.episodes)
        try:
            feed, new_episodes = await self._persist_episodes(
                store, feed, sorted_episodes
            )
            episodes_to_enqueue = await self._record_task_outbox(
                store, feed, new_episodes, auto_queue_episodes
            )
            await store.commit()
        except Exception as err:
            # A persistence-step failure after a successful parse: raise typed
            # so the batch caller can mark the feed ERROR and continue (XIN-53).
            # The session may be in a failed state; the caller rolls back.
            raise FeedSyncError(
                f"Failed to persist episodes for feed '{feed.rss_url}': {err}"
            ) from err

        # 6. Enqueue to the queue driver (external side effect, after the
        #    durable commit) for exactly the episodes recorded in the outbox.
        await self._enqueue_episode_tasks(feed, episodes_to_enqueue)

        return feed, new_episodes

    async def sync_podcast_episodes_by_id(
        self,
        store: Store,
        feed_id: uuid.UUID,
        client: httpx.AsyncClient | None = None,
        auto_queue_episodes: int = 0,
    ) -> tuple[Feed, list[Episode]]:
        """
        Sync the feed with primary key ``feed_id``.

        Raises FeedNotFoundError (a ValueError) when no feed row exists for
        the id. Exactly one commit per feed; see
        ``sync_podcast_episodes_by_feed``.
        """
        feed = await store.feeds.get_by_id(feed_id)
        if not feed:
            raise FeedNotFoundError(f"Feed with ID {feed_id} not found.")
        return await self.sync_podcast_episodes_by_feed(
            store, feed, client=client, auto_queue_episodes=auto_queue_episodes
        )

    async def sync_podcast_episodes_by_url(
        self,
        store: Store,
        rss_url: str,
        client: httpx.AsyncClient | None = None,
        auto_queue_episodes: int = 0,
    ) -> tuple[Feed, list[Episode]]:
        """
        Sync the feed at ``rss_url``.

        Explicit create-on-miss (XIN-65): when no feed row exists for the
        URL, a new Feed row (``sync_status=PENDING``, placeholder title) is
        created as part of the sync's single commit — callers can tell from
        this signature that the call may insert a row. ``rss_url`` must be an
        absolute http(s) URL; anything else raises FeedValidationError (a
        ValueError) without creating a row. The URL is canonicalized
        (XIN-44) before lookup/creation, so equivalent URL spellings share
        one feed row.

        Raises FeedNotFoundError never — a missing URL creates; an invalid
        URL raises FeedValidationError.
        """
        rss_url = canonicalize_feed_url(rss_url)
        feed = await store.feeds.get_by_rss_url(rss_url)
        if feed is None:
            feed = Feed(
                rss_url=rss_url,
                title="Fetching Podcast...",
                sync_status=FeedSyncStatus.PENDING.value,
            )
            feed = await store.feeds.save(feed)
        return await self.sync_podcast_episodes_by_feed(
            store, feed, client=client, auto_queue_episodes=auto_queue_episodes
        )

    async def ingest_feed(
        self,
        store: Store,
        rss_url: str,
        client: httpx.AsyncClient | None = None,
        auto_queue_episodes: int = 0,
    ) -> tuple[Feed, list[Episode]]:
        """
        Convenience / backward compatible method to sync feed & episodes by RSS URL.
        """
        return await self.sync_podcast_episodes_by_url(
            store,
            rss_url=rss_url,
            client=client,
            auto_queue_episodes=auto_queue_episodes,
        )

    # ------------------------------------------------------------------
    # Batch synchronization ("Batch Sync All Discovered/Pending Feeds")
    # ------------------------------------------------------------------

    async def sync_all_pending_feeds(
        self,
        store: Store,
        max_feeds: int | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> dict[str, Any]:
        """
        Sync all feeds due for a run: discovered/pending feeds plus errored
        feeds whose retry backoff has elapsed (XIN-34).

        XIN-38: this is now a thin sequential delegate over
        :class:`backend.ingestion.orchestration.FeedSyncOrchestrator` with
        ``concurrency=1`` — the single "sync all" implementation. The
        orchestrator normally opens its own sessions, but this overload
        reuses the caller's already-open ``store`` (the ONE sanctioned
        shared-session case; sequential, so no cross-worker sharing).

        Returns a summary dict; ``total_feeds_processed`` counts only feeds
        actually attempted (XIN-79), and ``max_feeds`` applies after the
        backoff/due filter so not-yet-due feeds never consume limit slots
        (XIN-80). ``skipped_backoff`` reports how many error feeds were
        filtered as not yet due.
        """
        from backend.ingestion.orchestration import (
            FeedSyncOrchestrator,
            SyncPolicy,
            shared_session,
        )

        orchestrator = FeedSyncOrchestrator(
            sync_service=self,
            policy=SyncPolicy(concurrency=1, max_feeds=max_feeds),
            session_factory=lambda: shared_session(store),
            client=client,
        )
        return await orchestrator.run()
