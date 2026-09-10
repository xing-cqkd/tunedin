"""SQLAlchemy implementation of the Store/repository protocol (XIN-85).

``SQLAlchemyStore`` wraps an async SQLAlchemy session and implements every
repository ABC defined in :mod:`backend.persistence.repositories`. It serves
BOTH the ``simple`` and ``app`` backends: those modules share one SQLAlchemy
``Base``, so a single implementation wraps whichever async session factory it
is given.

Query logic is ported verbatim from the existing call sites
(``backend/ingestion/service.py``, ``crawler.py``, ``batch_runner.py``,
``cli.py``) — those call sites are untouched; this module only centralizes
their queries behind the repository interface for the DynamoDB effort.

Write semantics: ``save()`` = ``session.add()`` + ``await session.flush()`` —
NO commit. The store is the unit of work; callers commit explicitly via
``await store.commit()``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Optional
from uuid import UUID

from sqlalchemy import and_, event, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from backend.persistence import validation
from backend.persistence.models import (
    Base,
    CuratedPlaylist,
    Episode,
    EpisodeTag,
    Feed,
    Insight,
    PlaylistEpisode,
    Tag,
    TaskLog,
    User,
    UserEpisodeProgress,
)
from backend.persistence.repositories import (
    VISIBILITY_UNLISTED,
    EpisodeRepository,
    FeedRepository,
    InsightRepository,
    PlaylistRepository,
    ProgressRepository,
    SlugConflictError,
    Store,
    TagRepository,
    TaskLogRepository,
    UserRepository,
    _now_utc,
    generate_slug,
    generate_token,
    validate_visibility,
)

SessionFactory = Callable[[], AsyncSession]


def _guard_item_size(entity: Any) -> None:
    """Enforce the shared 400 KiB per-item limit before a write (XIN-95).

    Same guard and same :class:`ItemTooLargeError` as the DynamoDB backend
    (which enforces it in ``codec.model_to_item``). Note the approximation:
    both sides measure the same canonical JSON serialization of the
    entity's column values, but DynamoDB's on-the-wire item is strictly
    larger (DynamoDB-JSON attribute-type wrappers plus key/index overhead).
    A borderline entity (~399.9 KiB here) can therefore pass this guard yet
    still be rejected by DynamoDB itself — in which case the service raises
    loudly. The guard's job is to catch the realistic cases
    (multi-hundred-KB transcripts/descriptions) identically on both
    backends, at write time.
    """
    pk_cols = list(entity.__table__.primary_key.columns)
    pk = f"{pk_cols[0].name}={getattr(entity, pk_cols[0].name)}" if pk_cols else "?"
    validation.check_item_size(
        validation.entity_fields(entity),
        what=f"{type(entity).__name__}({pk})",
    )


class _FeedRepository(FeedRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, feed_id: UUID) -> Optional[Feed]:
        res = await self._session.execute(
            select(Feed).where(Feed.feed_id == feed_id)
        )
        return res.scalar_one_or_none()

    async def get_by_rss_url(self, rss_url: str) -> Optional[Feed]:
        res = await self._session.execute(
            select(Feed).where(Feed.rss_url == rss_url)
        )
        return res.scalar_one_or_none()

    async def list_by_statuses(
        self, statuses: list[str], *, limit: Optional[int] = None
    ) -> list[Feed]:
        if not statuses:
            return []
        stmt = (
            select(Feed)
            .where(Feed.sync_status.in_(statuses))
            .order_by(Feed.created_at.asc())
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        res = await self._session.execute(stmt)
        return list(res.scalars().all())

    async def list_error_due_retry(
        self, cutoff: datetime, max_attempts: int
    ) -> list[Feed]:
        # Coarse SQL pre-filter from FeedIngestionService.sync_all_pending_feeds
        # (XIN-34). Callers still re-check the exact per-feed backoff window.
        stmt = (
            select(Feed)
            .where(
                and_(
                    Feed.sync_status == "error",
                    Feed.error_count < max_attempts,
                    Feed.last_fetched_at.is_not(None),
                    Feed.last_fetched_at <= cutoff,
                )
            )
            .order_by(Feed.created_at.asc())
        )
        res = await self._session.execute(stmt)
        return list(res.scalars().all())

    async def save(self, feed: Feed) -> Feed:
        _guard_item_size(feed)
        self._session.add(feed)
        await self._session.flush()
        return feed

    async def count_by_status(self, status: str) -> int:
        res = await self._session.execute(
            select(func.count(Feed.feed_id)).where(Feed.sync_status == status)
        )
        return res.scalar_one()

    async def count_by_statuses(self, statuses: list[str]) -> int:
        if not statuses:
            return 0
        res = await self._session.execute(
            select(func.count(Feed.feed_id)).where(Feed.sync_status.in_(statuses))
        )
        return res.scalar_one()

    async def count_all(self) -> int:
        res = await self._session.execute(select(func.count(Feed.feed_id)))
        return res.scalar_one()

    async def list_all(self, *, limit: Optional[int] = None) -> list[Feed]:
        stmt = select(Feed).order_by(Feed.created_at.asc())
        if limit is not None:
            stmt = stmt.limit(limit)
        res = await self._session.execute(stmt)
        return list(res.scalars().all())


class _EpisodeRepository(EpisodeRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, episode_id: UUID) -> Optional[Episode]:
        res = await self._session.execute(
            select(Episode).where(Episode.episode_id == episode_id)
        )
        return res.scalar_one_or_none()

    async def list_guids_by_feed(self, feed_id: UUID) -> set[str]:
        # From FeedIngestionService.sync_podcast_episodes (guid dedup step).
        res = await self._session.execute(
            select(Episode.guid).where(Episode.feed_id == feed_id)
        )
        return {g for g in res.scalars().all() if g}

    async def list_episodes_by_feed(self, feed_id: UUID) -> list[Episode]:
        res = await self._session.execute(
            select(Episode)
            .where(Episode.feed_id == feed_id)
            .order_by(Episode.published_at.desc().nullslast())
        )
        return list(res.scalars().all())

    async def list_unprocessed(
        self,
        *,
        feed_id: Optional[UUID] = None,
        limit: int = 50,
    ) -> list[Episode]:
        # From FeedIngestionService.get_unprocessed_episodes. SQL applies the
        # WHERE clause before LIMIT, so the limit is a hard cap on matching
        # rows — the contract the DynamoDB implementation must reproduce by
        # paginating.
        stmt = select(Episode).where(Episode.processed == False)
        if feed_id is not None:
            stmt = stmt.where(Episode.feed_id == feed_id)
        stmt = stmt.order_by(Episode.published_at.desc().nullslast()).limit(limit)
        res = await self._session.execute(stmt)
        return list(res.scalars().all())

    async def save(self, episode: Episode) -> Episode:
        _guard_item_size(episode)
        self._session.add(episode)
        await self._session.flush()
        return episode

    async def save_many(self, episodes: list[Episode]) -> list[Episode]:
        for episode in episodes:
            _guard_item_size(episode)
            self._session.add(episode)
        await self._session.flush()
        return episodes

    async def mark_processed(
        self, episode_id: UUID, processed: bool = True
    ) -> Optional[Episode]:
        # From FeedIngestionService.mark_episode_processed (query logic only —
        # the original committed internally; here the unit of work commits).
        res = await self._session.execute(
            select(Episode).where(Episode.episode_id == episode_id)
        )
        episode = res.scalar_one_or_none()
        if episode is not None:
            episode.processed = processed
            await self._session.flush()
        return episode

    async def count_all(self) -> int:
        res = await self._session.execute(select(func.count(Episode.episode_id)))
        return res.scalar_one()

    async def count_unprocessed(self) -> int:
        res = await self._session.execute(
            select(func.count(Episode.episode_id)).where(Episode.processed == False)
        )
        return res.scalar_one()


class _InsightRepository(InsightRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_by_episode(self, episode_id: UUID) -> list[Insight]:
        res = await self._session.execute(
            select(Insight)
            .where(Insight.episode_id == episode_id)
            .order_by(Insight.created_at.asc())
        )
        return list(res.scalars().all())

    async def save(self, insight: Insight) -> Insight:
        _guard_item_size(insight)
        self._session.add(insight)
        await self._session.flush()
        return insight

    async def save_many(self, insights: list[Insight]) -> list[Insight]:
        for insight in insights:
            _guard_item_size(insight)
            self._session.add(insight)
        await self._session.flush()
        return insights


class _TagRepository(TagRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_name_category(
        self, name: str, category: Optional[str]
    ) -> Optional[Tag]:
        category_filter = (
            Tag.category.is_(None) if category is None else Tag.category == category
        )
        res = await self._session.execute(
            select(Tag).where(Tag.name == name, category_filter)
        )
        return res.scalar_one_or_none()

    async def get_or_create(self, name: str, category: Optional[str]) -> Tag:
        tag = await self.get_by_name_category(name, category)
        if tag is not None:
            return tag
        tag = Tag(name=name, category=category)
        _guard_item_size(tag)
        self._session.add(tag)
        # The uq_tag_name_category constraint is the concurrency backstop: a
        # concurrent insert raises IntegrityError on flush, per the ABC
        # contract.
        await self._session.flush()
        return tag

    async def add_episode_tag(self, episode_id: UUID, tag_id: UUID) -> None:
        res = await self._session.execute(
            select(EpisodeTag).where(
                EpisodeTag.episode_id == episode_id,
                EpisodeTag.tag_id == tag_id,
            )
        )
        if res.scalar_one_or_none() is None:
            self._session.add(
                EpisodeTag(episode_id=episode_id, tag_id=tag_id)
            )
            await self._session.flush()

    async def list_tags_for_episode(self, episode_id: UUID) -> list[Tag]:
        res = await self._session.execute(
            select(Tag)
            .join(EpisodeTag, EpisodeTag.tag_id == Tag.tag_id)
            .where(EpisodeTag.episode_id == episode_id)
            .order_by(Tag.name.asc())
        )
        return list(res.scalars().all())


class _UserRepository(UserRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, user_id: UUID) -> Optional[User]:
        res = await self._session.execute(
            select(User).where(User.user_id == user_id)
        )
        return res.scalar_one_or_none()

    async def get_by_email(self, email: str) -> Optional[User]:
        res = await self._session.execute(
            select(User).where(User.email == email)
        )
        return res.scalar_one_or_none()

    async def save(self, user: User) -> User:
        _guard_item_size(user)
        self._session.add(user)
        await self._session.flush()
        return user


class _PlaylistRepository(PlaylistRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_by_user(self, user_id: UUID) -> list[CuratedPlaylist]:
        res = await self._session.execute(
            select(CuratedPlaylist)
            .where(CuratedPlaylist.user_id == user_id)
            .order_by(CuratedPlaylist.created_at.asc())
        )
        return list(res.scalars().all())

    async def get_by_id(self, playlist_id: UUID) -> Optional[CuratedPlaylist]:
        res = await self._session.execute(
            select(CuratedPlaylist).where(
                CuratedPlaylist.playlist_id == playlist_id
            )
        )
        return res.scalar_one_or_none()

    async def save(self, playlist: CuratedPlaylist) -> CuratedPlaylist:
        _guard_item_size(playlist)
        self._session.add(playlist)
        try:
            await self._session.flush()
        except IntegrityError as exc:
            # The session is unusable after a failed flush; roll back so
            # the unit of work can continue, then surface slug conflicts
            # as the backend-agnostic SlugConflictError.
            await self._session.rollback()
            if "slug" in str(exc.orig).lower():
                raise SlugConflictError(
                    f"playlist slug {playlist.slug!r} is already taken"
                ) from exc
            raise
        return playlist

    async def add_episode(
        self, playlist_id: UUID, episode_id: UUID, position: int
    ) -> None:
        res = await self._session.execute(
            select(PlaylistEpisode).where(
                PlaylistEpisode.playlist_id == playlist_id,
                PlaylistEpisode.episode_id == episode_id,
            )
        )
        link = res.scalar_one_or_none()
        if link is None:
            self._session.add(
                PlaylistEpisode(
                    playlist_id=playlist_id,
                    episode_id=episode_id,
                    position=position,
                )
            )
        else:
            link.position = position
        await self._session.flush()

    async def list_episodes(self, playlist_id: UUID) -> list[Episode]:
        res = await self._session.execute(
            select(Episode)
            .join(
                PlaylistEpisode,
                PlaylistEpisode.episode_id == Episode.episode_id,
            )
            .where(PlaylistEpisode.playlist_id == playlist_id)
            .order_by(PlaylistEpisode.position.asc(), Episode.episode_id.asc())
        )
        return list(res.scalars().all())

    async def publish(
        self, playlist_id: UUID, visibility: str
    ) -> Optional[CuratedPlaylist]:
        validate_visibility(visibility)
        playlist = await self.get_by_id(playlist_id)
        if playlist is None:
            return None
        playlist.visibility = visibility
        if playlist.slug is None:
            playlist.slug = generate_slug(playlist.title)
        if playlist.token is None:
            playlist.token = generate_token()
        return await self.save(playlist)

    async def unpublish(self, playlist_id: UUID) -> Optional[CuratedPlaylist]:
        playlist = await self.get_by_id(playlist_id)
        if playlist is None:
            return None
        playlist.visibility = VISIBILITY_UNLISTED
        return await self.save(playlist)

    async def rotate_token(self, playlist_id: UUID) -> Optional[str]:
        playlist = await self.get_by_id(playlist_id)
        if playlist is None:
            return None
        new_token = generate_token()
        playlist.token = new_token
        playlist.token_revoked_at = _now_utc()
        await self.save(playlist)
        return new_token

    async def get_by_slug(self, slug: str) -> Optional[CuratedPlaylist]:
        res = await self._session.execute(
            select(CuratedPlaylist).where(CuratedPlaylist.slug == slug)
        )
        return res.scalar_one_or_none()


class _ProgressRepository(ProgressRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(
        self, user_id: UUID, episode_id: UUID
    ) -> Optional[UserEpisodeProgress]:
        res = await self._session.execute(
            select(UserEpisodeProgress).where(
                UserEpisodeProgress.user_id == user_id,
                UserEpisodeProgress.episode_id == episode_id,
            )
        )
        return res.scalar_one_or_none()

    async def save(self, progress: UserEpisodeProgress) -> UserEpisodeProgress:
        # Genuine upsert by the composite primary key: add()+flush() would
        # raise IntegrityError when a second instance with the same
        # (user_id, episode_id) is saved in one unit of work, but the ABC
        # contract for save() is "upsert by primary key".
        _guard_item_size(progress)
        merged = await self._session.merge(progress)
        await self._session.flush()
        return merged


class _TaskLogRepository(TaskLogRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, task_log: TaskLog) -> TaskLog:
        _guard_item_size(task_log)
        self._session.add(task_log)
        await self._session.flush()
        return task_log

    async def list_by_type_status(
        self,
        task_type: str,
        status: str,
        limit: Optional[int] = None,
    ) -> list[TaskLog]:
        stmt = (
            select(TaskLog)
            .where(TaskLog.task_type == task_type, TaskLog.status == status)
            .order_by(TaskLog.created_at.desc())
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        res = await self._session.execute(stmt)
        return list(res.scalars().all())

    async def update_status(
        self,
        task_log_id: UUID,
        status: str,
        error_message: Optional[str] = None,
    ) -> Optional[TaskLog]:
        res = await self._session.execute(
            select(TaskLog).where(TaskLog.task_log_id == task_log_id)
        )
        entry = res.scalar_one_or_none()
        if entry is not None:
            entry.status = status
            entry.error_message = error_message
            _guard_item_size(entry)
            await self._session.flush()
        return entry


class SQLAlchemyStore(Store):
    """Unit of work backed by an async SQLAlchemy session.

    Wraps the given async session factory — which may point at the ``simple``
    or the ``app`` database (both share ``Base``) — and exposes lightweight
    repository objects over a single session for the store's lifetime.
    """

    def __init__(self, session_factory: SessionFactory) -> None:
        self._session_factory = session_factory
        self._session: AsyncSession = session_factory()
        self._feeds = _FeedRepository(self._session)
        self._episodes = _EpisodeRepository(self._session)
        self._insights = _InsightRepository(self._session)
        self._tags = _TagRepository(self._session)
        self._users = _UserRepository(self._session)
        self._playlists = _PlaylistRepository(self._session)
        self._progress = _ProgressRepository(self._session)
        self._task_logs = _TaskLogRepository(self._session)

    @classmethod
    def from_url(cls, url: str, **engine_kwargs) -> "SQLAlchemyStore":
        """Build a store against the given async SQLAlchemy URL.

        Convenience constructor for tests, CLIs, and one-off scripts —
        mirrors the session wiring in ``backend/persistence/database.py``
        (including the SQLite foreign-key pragma).
        """
        engine = create_async_engine(url, echo=False, future=True, **engine_kwargs)
        if url.startswith("sqlite"):
            @event.listens_for(engine.sync_engine, "connect")
            def _set_sqlite_pragma(dbapi_connection, connection_record):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()

        factory = async_sessionmaker(
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autocommit=False,
            autoflush=False,
        )
        return cls(factory)

    # -- Store properties ---------------------------------------------------

    @property
    def feeds(self) -> FeedRepository:
        return self._feeds

    @property
    def episodes(self) -> EpisodeRepository:
        return self._episodes

    @property
    def insights(self) -> InsightRepository:
        return self._insights

    @property
    def tags(self) -> TagRepository:
        return self._tags

    @property
    def users(self) -> UserRepository:
        return self._users

    @property
    def playlists(self) -> PlaylistRepository:
        return self._playlists

    @property
    def progress(self) -> ProgressRepository:
        return self._progress

    @property
    def task_logs(self) -> TaskLogRepository:
        return self._task_logs

    # -- Unit of work -------------------------------------------------------

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()

    async def close(self) -> None:
        await self._session.close()
