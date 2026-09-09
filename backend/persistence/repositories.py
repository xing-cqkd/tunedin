"""Repository / Store protocol for the tunedin persistence layer.

This module defines the abstract interfaces that all database backends must
implement (Linear: XIN-84). It contains no implementation — concrete stores
(e.g. ``SQLAlchemyStore`` for the ``simple``/``app`` backends, ``DynamoDBStore``
for the future ``dynamodb`` backend) live in sibling modules.

Design notes (per the architect review of the DynamoDB backend plan):

* The ``Store`` is the unit of work. Application code obtains one via
  ``settings.open_store()`` (``session_scope()`` remains only as a thin alias
  for external compatibility — it is not the canonical name).
* Repositories accept and return the SQLAlchemy model classes from
  ``backend.persistence.models`` as plain attribute bags. Application code
  must not traverse ORM relationships through them.
* Migration between backends is NOT done through repositories — it uses the
  ``Backend`` ABC in ``backend.migrate_data`` (``table_names`` /
  ``read_table`` / ``write_rows``). Repositories therefore expose no
  ``iter_all`` / ``bulk_save`` migration seam.
* ``save()`` is the persistence point: on SQL backends it adds + flushes the
  entity without committing (preserving the unit-of-work transaction);
  on write-through backends (DynamoDB) the write is immediate.
  ``store.commit()`` only finalizes the unit of work (a no-op safety net on
  write-through backends).
* Ordering contracts below are binding. Where the underlying backend cannot
  produce an order natively (e.g. a DynamoDB ``FilterExpression``), the
  implementation must satisfy the contract in code (paginate, merge, sort).
  Rows with equal sort keys may be returned in any order on ALL backends —
  tests must never assert an exact sequence across tied values.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
from uuid import UUID

from backend.persistence.models import (
    CuratedPlaylist,
    Episode,
    Feed,
    Insight,
    Tag,
    TaskLog,
    User,
    UserEpisodeProgress,
)


class FeedRepository(ABC):
    """Persistence operations for :class:`Feed`."""

    @abstractmethod
    async def get_by_id(self, feed_id: UUID) -> Optional[Feed]:
        """Return the feed with the given id, or ``None`` if it does not exist."""

    @abstractmethod
    async def get_by_rss_url(self, rss_url: str) -> Optional[Feed]:
        """Return the feed with the given RSS URL, or ``None``.

        ``rss_url`` is unique; at most one row is ever returned.
        """

    @abstractmethod
    async def list_by_statuses(
        self, statuses: list[str], *, limit: Optional[int] = None
    ) -> list[Feed]:
        """Return feeds whose ``sync_status`` is in ``statuses``.

        Ordered by ``created_at`` ascending. Feeds with equal ``created_at``
        may be returned in any relative order on all backends. ``limit``
        caps the number of returned rows (``None`` = no cap). An empty
        ``statuses`` list returns an empty list without hitting the backend.
        """

    @abstractmethod
    async def list_error_due_retry(
        self, cutoff: datetime, max_attempts: int
    ) -> list[Feed]:
        """Return errored feeds that are due for a retry.

        Matches feeds with ``sync_status == 'error'``,
        ``last_fetched_at <= cutoff`` and ``error_count < max_attempts``.
        Ordered by ``created_at`` ascending (same tie rule as
        :meth:`list_by_statuses`). Callers re-check the per-feed backoff
        window themselves; this method applies only the coarse filter.
        """

    @abstractmethod
    async def save(self, feed: Feed) -> Feed:
        """Persist a new or existing feed (upsert by primary key).

        On SQL backends this adds + flushes without committing; on
        write-through backends the write is immediate. Returns the persisted
        entity with its primary key and server-side defaults populated.
        """

    @abstractmethod
    async def count_by_status(self, status: str) -> int:
        """Return the number of feeds with the given ``sync_status``."""

    @abstractmethod
    async def count_all(self) -> int:
        """Return the total number of feeds."""

    @abstractmethod
    async def list_all(self, *, limit: Optional[int] = None) -> list[Feed]:
        """Return all feeds, ordered by ``created_at`` ascending.

        Same tie rule as :meth:`list_by_statuses`. ``limit`` caps the number
        of returned rows (``None`` = no cap).
        """


class EpisodeRepository(ABC):
    """Persistence operations for :class:`Episode`."""

    @abstractmethod
    async def get_by_id(self, episode_id: UUID) -> Optional[Episode]:
        """Return the episode with the given id, or ``None``."""

    @abstractmethod
    async def list_guids_by_feed(self, feed_id: UUID) -> set[str]:
        """Return the set of non-null guids for a feed's episodes.

        Used for guid dedup during sync. Episodes with a null guid are
        excluded (they cannot be deduped). Ordering is undefined.
        """

    @abstractmethod
    async def list_unprocessed(
        self,
        *,
        feed_id: Optional[UUID] = None,
        limit: int = 50,
    ) -> list[Episode]:
        """Return unprocessed episodes (``processed`` is false).

        Ordered by ``published_at`` descending, nulls last. When ``feed_id``
        is given, only that feed's episodes are returned.
        ``limit`` is a hard cap on the number of *matching* rows returned.

        Pagination contract: implementations MUST keep paging until ``limit``
        matches have accumulated or the underlying index is exhausted. This
        is a real correctness requirement, not an optimization hint:
        DynamoDB applies ``Limit`` before ``FilterExpression``, so a naive
        single-page query would silently under-return when ``feed_id`` is
        given.
        """

    @abstractmethod
    async def save(self, episode: Episode) -> Episode:
        """Persist a new or existing episode (upsert by primary key).

        Same write semantics as :meth:`FeedRepository.save`.
        """

    @abstractmethod
    async def save_many(self, episodes: list[Episode]) -> list[Episode]:
        """Persist a batch of episodes as one write unit.

        Returns the persisted entities in input order. On SQL backends this
        is a single flushed batch inside the unit of work; on DynamoDB it is
        the transactional guid-dedup write (marker items + conditional puts),
        with conflicting duplicates dropped and reported via the return
        value (the returned list contains only the episodes that were
        actually persisted).
        """

    @abstractmethod
    async def mark_processed(
        self, episode_id: UUID, processed: bool = True
    ) -> Optional[Episode]:
        """Set the ``processed`` flag on an episode.

        Returns the updated episode, or ``None`` if the id does not exist.
        On backends with a sparse unprocessed index, marking an episode
        processed removes it from that index.
        """

    @abstractmethod
    async def count_all(self) -> int:
        """Return the total number of episodes."""

    @abstractmethod
    async def count_unprocessed(self) -> int:
        """Return the number of episodes with ``processed`` false."""


class InsightRepository(ABC):
    """Persistence operations for :class:`Insight`."""

    @abstractmethod
    async def list_by_episode(self, episode_id: UUID) -> list[Insight]:
        """Return all insights for an episode, ordered by ``created_at``
        ascending. Same tie rule as :meth:`FeedRepository.list_by_statuses`.
        """

    @abstractmethod
    async def save(self, insight: Insight) -> Insight:
        """Persist a new or existing insight (upsert by primary key).

        Same write semantics as :meth:`FeedRepository.save`.
        """

    @abstractmethod
    async def save_many(self, insights: list[Insight]) -> list[Insight]:
        """Persist a batch of insights as one write unit.

        Returns the persisted entities in input order.
        """


class TagRepository(ABC):
    """Persistence operations for :class:`Tag` and episode-tag links."""

    @abstractmethod
    async def get_by_name_category(
        self, name: str, category: Optional[str]
    ) -> Optional[Tag]:
        """Return the tag with the given (name, category), or ``None``.

        The pair is unique; at most one row is ever returned.
        """

    @abstractmethod
    async def get_or_create(
        self, name: str, category: Optional[str]
    ) -> Tag:
        """Return the existing tag for (name, category), creating it if needed.

        Concurrency contract: at most one tag with a given (name, category)
        may be visible afterwards. SQL backends rely on the unique
        constraint within the unit of work; write-through backends use a
        conditional write on the name-category key and re-read on conflict.
        """

    @abstractmethod
    async def add_episode_tag(self, episode_id: UUID, tag_id: UUID) -> None:
        """Link a tag to an episode. Idempotent: adding the same link twice
        is a no-op."""

    @abstractmethod
    async def list_tags_for_episode(self, episode_id: UUID) -> list[Tag]:
        """Return all tags linked to an episode, ordered by tag name
        ascending. Tag names are unique per link set in practice; no tie
        rule is defined beyond the name ordering."""


class UserRepository(ABC):
    """Persistence operations for :class:`User`."""

    @abstractmethod
    async def get_by_id(self, user_id: UUID) -> Optional[User]:
        """Return the user with the given id, or ``None``."""

    @abstractmethod
    async def get_by_email(self, email: str) -> Optional[User]:
        """Return the user with the given email, or ``None``.

        Email is unique; at most one row is ever returned.
        """

    @abstractmethod
    async def save(self, user: User) -> User:
        """Persist a new or existing user (upsert by primary key).

        Same write semantics as :meth:`FeedRepository.save`.
        """


class PlaylistRepository(ABC):
    """Persistence operations for :class:`CuratedPlaylist` and playlist links."""

    @abstractmethod
    async def list_by_user(self, user_id: UUID) -> list[CuratedPlaylist]:
        """Return a user's playlists, ordered by ``created_at`` ascending.

        Same tie rule as :meth:`FeedRepository.list_by_statuses`.
        """

    @abstractmethod
    async def get_by_id(self, playlist_id: UUID) -> Optional[CuratedPlaylist]:
        """Return the playlist with the given id, or ``None``."""

    @abstractmethod
    async def save(self, playlist: CuratedPlaylist) -> CuratedPlaylist:
        """Persist a new or existing playlist (upsert by primary key).

        Same write semantics as :meth:`FeedRepository.save`.
        """

    @abstractmethod
    async def add_episode(
        self, playlist_id: UUID, episode_id: UUID, position: int
    ) -> None:
        """Link an episode into a playlist at ``position``.

        Upsert on (playlist_id, episode_id): re-adding an existing link
        updates its position instead of duplicating it.
        """

    @abstractmethod
    async def list_episodes(self, playlist_id: UUID) -> list[Episode]:
        """Return a playlist's episodes, ordered by ``position`` ascending.

        Ties on ``position`` are broken by ``episode_id`` ascending so the
        order is fully deterministic on all backends.
        """


class ProgressRepository(ABC):
    """Persistence operations for :class:`UserEpisodeProgress`."""

    @abstractmethod
    async def get(
        self, user_id: UUID, episode_id: UUID
    ) -> Optional[UserEpisodeProgress]:
        """Return the progress row for (user, episode), or ``None``."""

    @abstractmethod
    async def save(self, progress: UserEpisodeProgress) -> UserEpisodeProgress:
        """Persist a new or existing progress row (upsert by primary key).

        Same write semantics as :meth:`FeedRepository.save`.
        """


class TaskLogRepository(ABC):
    """Persistence operations for :class:`TaskLog`."""

    @abstractmethod
    async def save(self, task_log: TaskLog) -> TaskLog:
        """Persist a new or existing task log entry (upsert by primary key).

        Same write semantics as :meth:`FeedRepository.save`.
        """

    @abstractmethod
    async def list_by_type_status(
        self,
        task_type: str,
        status: str,
        limit: Optional[int] = None,
    ) -> list[TaskLog]:
        """Return task logs for a (type, status) pair, ordered by
        ``created_at`` descending (newest first). Same tie rule as
        :meth:`FeedRepository.list_by_statuses`. ``limit`` caps the number of
        returned rows (``None`` = no cap).
        """

    @abstractmethod
    async def update_status(
        self,
        task_log_id: UUID,
        status: str,
        error_message: Optional[str] = None,
    ) -> Optional[TaskLog]:
        """Update a task log's status (and optional error message).

        Returns the updated entry, or ``None`` if the id does not exist.
        """


class Store(ABC):
    """Unit of work over all repositories.

    Application code obtains a ``Store`` from ``settings.open_store()`` and
    must not assume anything about the backend behind it. The write pattern
    is: mutate entities -> ``await repo.save(...)`` -> ``await
    store.commit()``. ``save()`` is the persistence point; ``commit()`` only
    finalizes the unit of work (a no-op safety net on write-through
    backends).
    """

    @property
    @abstractmethod
    def feeds(self) -> FeedRepository:
        """The feed repository."""

    @property
    @abstractmethod
    def episodes(self) -> EpisodeRepository:
        """The episode repository."""

    @property
    @abstractmethod
    def insights(self) -> InsightRepository:
        """The insight repository."""

    @property
    @abstractmethod
    def tags(self) -> TagRepository:
        """The tag repository."""

    @property
    @abstractmethod
    def users(self) -> UserRepository:
        """The user repository."""

    @property
    @abstractmethod
    def playlists(self) -> PlaylistRepository:
        """The playlist repository."""

    @property
    @abstractmethod
    def progress(self) -> ProgressRepository:
        """The user-episode-progress repository."""

    @property
    @abstractmethod
    def task_logs(self) -> TaskLogRepository:
        """The task-log repository."""

    @abstractmethod
    async def commit(self) -> None:
        """Finalize the unit of work.

        On SQL backends this commits the transaction. On write-through
        backends (DynamoDB) this is a no-op — ``save()`` already persisted
        each write.
        """

    @abstractmethod
    async def rollback(self) -> None:
        """Discard pending, uncommitted writes.

        On SQL backends this rolls back the transaction. On write-through
        backends this is a no-op with an important caveat: writes that
        already went through ``save()`` cannot be undone.
        """

    @abstractmethod
    async def close(self) -> None:
        """Release all resources held by the store (sessions, clients)."""

    async def __aenter__(self) -> "Store":
        """Enter the store's context. Returns the store itself."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the store's context.

        Commits when the block exits cleanly, rolls back when it exits with
        an exception. Does not suppress exceptions (returns ``None``).
        """
        if exc_type is None:
            await self.commit()
        else:
            await self.rollback()
