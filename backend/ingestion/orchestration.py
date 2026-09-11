"""
Single feed-sync orchestrator (XIN-38).

This module collapses the four previously divergent "sync all pending feeds"
implementations into one:

- ``FeedSyncService.sync_all_pending_feeds`` (sequential, caller-owned session)
- ``PodcastCrawler.sync_episodes_concurrently`` (semaphore worker pool, own
  session per worker, no error-retry handling)
- ``run_batch_ingest`` (``batch_runner.py``: sequential batches, markdown
  progress log, 429 special-case)
- ``cli.run_sync_only`` (thin CLI entry)

All four are now thin entry points over :class:`FeedSyncOrchestrator`, which
owns the concurrency model, feed selection (including the XIN-34 error-retry
backoff), per-feed error handling, and progress reporting.

Session ownership — exactly one rule (XIN-38):

- The orchestrator opens every session it uses: one short-lived session to
  select the due feed list, then one fresh session per worker feed-sync.
  Callers never pass sessions in.
- Feed IDs (not ORM objects) cross the session boundary: each worker
  re-fetches its feed inside its own session. ORM instances from the
  selection session would be detached in the worker (``MissingGreenlet`` on
  attribute access).
- ``FeedSyncService.sync_*`` takes the open store and performs exactly one
  commit per feed; it never opens sessions. On ``FeedSyncError`` the worker's
  session may be left in a failed state, so the orchestrator rolls it back
  before reusing/marking.
- Sanctioned exception: ``FeedSyncService.sync_all_pending_feeds(store, ...)``
  reuses the caller's already-open store at ``concurrency=1`` (sequential,
  same as the old behavior) via :func:`shared_session`.
"""

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from backend.ingestion.service import FeedSyncService, FeedSyncStatus
from backend.persistence.models.feed import Feed
from backend.persistence.repositories import Store
from settings import session_scope

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Error-retry backoff policy (XIN-34; moved here from service.py so the
# orchestrator is the single owner of "which feeds are due").
# ---------------------------------------------------------------------------

#: Base backoff between retries of an errored feed; doubles per consecutive
#: failure.
ERROR_RETRY_BASE_BACKOFF_SECONDS = 300
#: Stop retrying an errored feed after this many consecutive failures.
ERROR_RETRY_MAX_ATTEMPTS = 10


def error_retry_due(feed: Feed, now: datetime) -> bool:
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


def fetch_status_code(err: Exception) -> int | None:
    """Best-effort HTTP status for a fetch failure.

    ``FeedFetchError.status_code`` is preferred; a raw ``httpx.HTTPStatusError``
    exposes it via ``err.response``. Returns None for non-HTTP failures.
    """
    status = getattr(err, "status_code", None)
    if status is None and isinstance(err, httpx.HTTPStatusError):
        status = err.response.status_code
    return status


async def _select_due_feeds(
    store: Store, max_feeds: int | None
) -> tuple[list[Feed], int]:
    """Select feeds due for a sync run.

    Discovered/pending feeds plus errored feeds whose retry backoff has
    elapsed (XIN-34). Returns ``(due_feeds, skipped_backoff)`` where
    ``skipped_backoff`` counts error feeds filtered as not yet due.

    ``max_feeds`` applies AFTER the backoff/due filter so not-yet-due feeds
    never consume limit slots (XIN-80); the returned count of attempted feeds
    excludes them (XIN-79).
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
    # Apply the per-row backoff/due filter FIRST, then the max_feeds slice —
    # counting and limiting only feeds actually attempted.
    due_feeds = [
        f
        for f in merged
        if not (
            f.sync_status == FeedSyncStatus.ERROR.value
            and not error_retry_due(f, now)
        )
    ]
    skipped_backoff = len(merged) - len(due_feeds)
    if max_feeds:
        due_feeds = due_feeds[:max_feeds]
    return due_feeds, skipped_backoff


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------


@dataclass
class SyncPolicy:
    """Pluggable policy for :class:`FeedSyncOrchestrator`.

    - ``concurrency``: worker pool size; 1 means sequential.
    - ``delay_between_feeds``: politeness sleep after each attempted feed
      (applied per worker, after the worker's session is closed).
    - ``max_feeds``: cap on feeds attempted per run (applied after the
      backoff/due filter).
    - ``auto_queue_episodes``: episodes per feed enqueued for downstream LLM
      processing (0 disables).
    - ``throttle_backoff_seconds``: sleep on HTTP 429 fetch failures before
      continuing (0 disables).
    - ``on_feed_synced`` / ``on_feed_failed``: per-feed callbacks
      ``(title, new_episode_count)`` / ``(feed_label, error)``.
    - ``on_progress``: called once at the end of :meth:`FeedSyncOrchestrator.run`
      with the final summary dict.
    """

    concurrency: int = 5
    delay_between_feeds: float = 0.0
    max_feeds: int | None = None
    auto_queue_episodes: int = 0
    throttle_backoff_seconds: float = 5.0
    on_feed_synced: Callable[[str, int], None] | None = None
    on_feed_failed: Callable[[str, Exception], None] | None = None
    on_progress: Callable[[dict[str, Any]], None] | None = None

    def __post_init__(self) -> None:
        # XIN-76: a bare Semaphore(0) deadlocks the worker pool — reject at
        # construction instead of hanging.
        if self.concurrency < 1:
            raise ValueError(
                f"concurrency must be >= 1, got {self.concurrency} "
                "(concurrency=0 deadlocks the episode sync)"
            )


#: Protocol for the orchestrator's session source: a zero-arg callable
#: returning an async context manager that yields an open Store.
SessionFactory = Callable[[], Any]


@asynccontextmanager
async def shared_session(store: Store) -> AsyncIterator[Store]:
    """Adapt an already-open Store to the orchestrator's session-factory protocol.

    This is the ONE sanctioned shared-session case: the sequential
    ``FeedSyncService.sync_all_pending_feeds(store, ...)`` path reuses the
    caller's store at ``concurrency=1``. The orchestrator itself never shares
    a session across concurrent workers.
    """
    yield store


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


class FeedSyncOrchestrator:
    """The single owner of "sync all due feeds" (XIN-38).

    Owns the concurrency model (semaphore-bounded worker pool), session
    lifecycle (one session for feed selection, one fresh session per worker
    feed — see the module docstring), error/backoff handling via the existing
    ``FeedSyncStatus`` / typed-error machinery, and progress reporting via
    policy callbacks.

    Works *with* the service's one-commit-per-feed contract, never around it:
    each worker calls ``sync_podcast_episodes_by_id`` on its own session and
    rolls that session back if a persistence-step failure
    (``FeedSyncError``) leaves it in a failed state.
    """

    def __init__(
        self,
        sync_service: FeedSyncService | None = None,
        policy: SyncPolicy | None = None,
        session_factory: SessionFactory | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._sync_service = sync_service or FeedSyncService()
        self._policy = policy or SyncPolicy()
        # Default: the orchestrator opens its own sessions (the rule).
        self._session_factory = session_factory or session_scope
        self._client = client

    async def run(self) -> dict[str, Any]:
        """Sync every due feed per the policy; return the summary dict.

        Summary keys (kept identical to the old
        ``FeedSyncService.sync_all_pending_feeds`` shape):
        ``total_feeds_processed`` (feeds actually attempted, XIN-79),
        ``total_synced``, ``total_episodes_saved``, ``failed_count``,
        ``failed_feed_ids``, ``skipped_backoff`` (XIN-80).
        """
        policy = self._policy
        # Selection runs in its own short-lived session; only feed IDs cross
        # into the workers (see the session-ownership rule above).
        async with self._session_factory() as store:
            due_feeds, skipped_backoff = await _select_due_feeds(
                store, policy.max_feeds
            )
            pending_ids = [f.feed_id for f in due_feeds]

        state = {"synced": 0, "episodes": 0, "failed": 0, "failed_ids": []}
        if pending_ids:
            semaphore = asyncio.Semaphore(policy.concurrency)
            await asyncio.gather(
                *(self._sync_one(feed_id, semaphore, state) for feed_id in pending_ids)
            )

        summary = {
            "total_feeds_processed": len(pending_ids),
            "total_synced": state["synced"],
            "total_episodes_saved": state["episodes"],
            "failed_count": state["failed"],
            "failed_feed_ids": state["failed_ids"],
            "skipped_backoff": skipped_backoff,
        }
        if policy.on_progress is not None:
            policy.on_progress(summary)
        return summary

    async def _sync_one(
        self,
        feed_id: Any,
        semaphore: asyncio.Semaphore,
        state: dict[str, Any],
    ) -> None:
        """Sync one feed on its own session; never lets one feed's failure
        poison the others."""
        policy = self._policy
        async with semaphore:
            async with self._session_factory() as store:
                feed = await store.feeds.get_by_id(feed_id)
                if feed is None:
                    # Vanished between selection and worker start: counted in
                    # total_feeds_processed (matches the old sequential path)
                    # but not attempted.
                    logger.debug("Feed %s vanished before sync; skipping", feed_id)
                    return
                label = f"{feed.title[:40]} ({feed.rss_url[:35]}...)"
                now = datetime.now(timezone.utc)
                # Defensive re-check of the exact per-feed backoff window (the
                # selection above was already due-filtered; this only fires on
                # races with another run).
                if feed.sync_status == FeedSyncStatus.ERROR.value and not error_retry_due(
                    feed, now
                ):
                    return
                pre_error_count = feed.error_count
                try:
                    synced_feed, new_eps = (
                        await self._sync_service.sync_podcast_episodes_by_id(
                            store=store,
                            feed_id=feed_id,
                            client=self._client,
                            auto_queue_episodes=policy.auto_queue_episodes,
                        )
                    )
                except Exception as err:
                    await self._handle_feed_failure(
                        store, feed_id, label, pre_error_count, now, err, state
                    )
                    return
                state["synced"] += 1
                state["episodes"] += len(new_eps)
                if policy.on_feed_synced is not None:
                    policy.on_feed_synced(synced_feed.title, len(new_eps))

            # Politeness delay, after the worker's session is closed (it is
            # applied per attempted feed, success or failure).
            if policy.delay_between_feeds:
                await asyncio.sleep(policy.delay_between_feeds)

    async def _handle_feed_failure(
        self,
        store: Store,
        feed_id: Any,
        label: str,
        pre_error_count: int,
        now: datetime,
        err: Exception,
        state: dict[str, Any],
    ) -> None:
        """Record one feed's failure: throttle backoff, session rollback,
        ERROR marking (if the service didn't already), counting, callbacks.

        Per the XIN-53 error policy this is the single place a feed-sync
        failure is logged — the service raises typed errors without logging.
        """
        policy = self._policy
        # Throttle special-case (from batch_runner): 429 → back off before
        # continuing with the next feed.
        if (
            fetch_status_code(err) == 429
            and policy.throttle_backoff_seconds > 0
        ):
            logger.warning(
                "HTTP 429 throttled on %s; backing off %.1fs",
                label,
                policy.throttle_backoff_seconds,
            )
            await asyncio.sleep(policy.throttle_backoff_seconds)
        # XIN-127: a persistence-step failure (FeedSyncError) leaves the
        # session uncommitted and possibly failed — roll back before reuse.
        # For fetch/parse failures the service already committed its ERROR
        # mark, making this a harmless no-op.
        await store.rollback()
        feed = await store.feeds.get_by_id(feed_id)
        if feed is not None and feed.error_count == pre_error_count:
            # The fetch/parse path inside the sync method marks and commits
            # the error state itself (bumping error_count); only mark here
            # when it didn't (e.g. a persistence-step failure after a
            # successful fetch, raised as FeedSyncError).
            feed.error_count = pre_error_count + 1
            feed.sync_status = FeedSyncStatus.ERROR.value
            feed.last_fetched_at = now
            await store.feeds.save(feed)
            await store.commit()
        state["failed"] += 1
        state["failed_ids"].append(str(feed_id))
        logger.warning(
            "Failed feed sync for %s: %s: %s", label, type(err).__name__, err
        )
        if policy.on_feed_failed is not None:
            policy.on_feed_failed(label, err)
