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

Write semantics: ``save()`` = upsert-by-primary-key (``session.merge()``,
matching DynamoDB's ``PutItem``) + ``await session.flush()`` — NO commit.
The store is the unit of work; callers commit explicitly via
``await store.commit()``. ``save()`` returns the managed (merged)
instance, which is the one carrying populated PKs/defaults.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Optional, TypeVar
from uuid import UUID

from sqlalchemy import and_, event, func, or_, select
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
    MissingParentError,
    PlaylistEpisodeEntry,
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


_T = TypeVar("_T")


def _apply_write_defaults(entity: Any) -> None:
    """Coerce write-path defaults the DB schema requires (XIN-122).

    Mirrors ``dynamodb.codec.apply_defaults``: ``Episode.episode_type`` is
    NOT NULL with default ``"full"``, so an unset/None value is coerced
    before the flush instead of relying on the column default.
    """
    if isinstance(entity, Episode) and entity.episode_type is None:
        entity.episode_type = "full"


async def _persist(session: AsyncSession, entity: Any) -> Any:
    """Shared merge + flush for every repository ``save()`` (XIN-120/122).

    Uses ``session.merge()`` — a true upsert by primary key, matching
    DynamoDB's ``PutItem`` (verified: unset Python-side defaults still fire
    on merge, and a detached instance carrying an existing PK updates the
    row instead of raising ``IntegrityError``). Never commits — the store
    is the unit of work. Returns the managed (merged) instance, which is
    the one carrying populated PKs/defaults.

    The item-size guard is deliberately NOT applied here: it stays at the
    public write entry points (``save()``/``save_many()``/
    ``update_status()``), so ``publish``/``add_episode_tag`` never trigger
    it (XIN-122).
    """
    _apply_write_defaults(entity)
    managed = await session.merge(entity)
    await session.flush()
    return managed


async def _get_by_unique(
    session: AsyncSession, model: type[_T], column: Any, value: Any
) -> Optional[_T]:
    """Single-row lookup by a unique column (XIN-122).

    Collapses the near-identical ``get_by_id`` / ``get_by_email`` /
    ``get_by_rss_url`` / ``get_by_slug`` bodies.
    """
    res = await session.execute(select(model).where(column == value))
    return res.scalar_one_or_none()


def _constraint_name(orig: Any) -> Optional[str]:
    """Violated-constraint name from a DBAPI error, when exposed.

    psycopg 2/3 surface it as ``orig.diag.constraint_name``; asyncpg as
    ``orig.constraint_name``; sqlite3 does not expose it at all (callers
    fall back to matching the table/column in the message text).
    """
    diag = getattr(orig, "diag", None)
    name = getattr(diag, "constraint_name", None)
    return name or getattr(orig, "constraint_name", None)


def _matches_unique_violation(exc: IntegrityError, constraint: str, column: str) -> bool:
    """True when ``exc`` is the named unique-constraint violation (XIN-119).

    Matches the constraint name explicitly where the driver exposes it;
    on sqlite3 (which reports only ``table.column``) falls back to the
    message text.
    """
    name = _constraint_name(exc.orig)
    if name is not None:
        return name == constraint
    text = str(exc.orig).lower()
    return constraint in text or (
        "unique constraint failed" in text and column in text
    )


def _is_slug_conflict(exc: IntegrityError) -> bool:
    """True when ``exc`` is the ``uq_curated_playlists_slug`` violation."""
    return _matches_unique_violation(
        exc, "uq_curated_playlists_slug", "curated_playlists.slug"
    )


def _is_episode_guid_conflict(exc: IntegrityError) -> bool:
    """True when ``exc`` is the ``uq_episode_feed_guid`` violation."""
    return _matches_unique_violation(
        exc, "uq_episode_feed_guid", "episodes.guid"
    )


class _FeedRepository(FeedRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, feed_id: UUID) -> Optional[Feed]:
        return await _get_by_unique(self._session, Feed, Feed.feed_id, feed_id)

    async def get_by_rss_url(self, rss_url: str) -> Optional[Feed]:
        return await _get_by_unique(self._session, Feed, Feed.rss_url, rss_url)

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
        # Coarse SQL pre-filter from FeedSyncService.sync_all_pending_feeds
        # (XIN-34). Callers still re-check the exact per-feed backoff window.
        # Error rows with a NULL last_fetched_at are included (XIN-128): they
        # can arise from direct inserts/migrations and must not be stranded.
        stmt = (
            select(Feed)
            .where(
                and_(
                    Feed.sync_status == "error",
                    Feed.error_count < max_attempts,
                    or_(
                        Feed.last_fetched_at.is_(None),
                        Feed.last_fetched_at <= cutoff,
                    ),
                )
            )
            .order_by(Feed.created_at.asc())
        )
        res = await self._session.execute(stmt)
        return list(res.scalars().all())

    async def save(self, feed: Feed) -> Feed:
        _guard_item_size(feed)
        return await _persist(self._session, feed)

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
        return await _get_by_unique(
            self._session, Episode, Episode.episode_id, episode_id
        )

    async def list_guids_by_feed(self, feed_id: UUID) -> set[str]:
        # From FeedSyncService.sync_podcast_episodes_by_feed (guid dedup step).
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
        # From backend.insights.pipeline.get_unprocessed_episodes. SQL applies the
        # WHERE clause before LIMIT, so the limit is a hard cap on matching
        # rows — the contract the DynamoDB implementation must reproduce by
        # paginating.
        stmt = select(Episode).where(Episode.processed.is_(False))
        if feed_id is not None:
            stmt = stmt.where(Episode.feed_id == feed_id)
        stmt = stmt.order_by(Episode.published_at.desc().nullslast()).limit(limit)
        res = await self._session.execute(stmt)
        return list(res.scalars().all())

    async def save(self, episode: Episode) -> Episode:
        _guard_item_size(episode)
        return await _persist(self._session, episode)

    async def save_many(self, episodes: list[Episode]) -> list[Episode]:
        # Protocol: conflicting duplicates are dropped and reported via
        # the return value, which contains only the episodes that were
        # actually persisted, in input order — mirroring the DynamoDB
        # transactional guid-dedup write (XIN-120).
        if not episodes:
            return []
        # Within-batch dedup first: keep the first episode per
        # (feed_id, guid), like the DynamoDB backend. Post-XIN-68 the model
        # enforces guid NOT NULL, so every episode is dedupable; the
        # ``episode.guid`` guard below is only defensive for in-memory
        # objects constructed before validation.
        seen: set[tuple[str, str]] = set()
        candidates: list[Episode] = []
        for episode in episodes:
            if episode.guid:
                key = (str(episode.feed_id), episode.guid)
                if key in seen:
                    continue
                seen.add(key)
            candidates.append(episode)
        persisted: list[Episode] = []
        for episode in candidates:
            _guard_item_size(episode)
            try:
                # Per-row SAVEPOINT: a (feed_id, guid) conflict with an
                # already-stored episode drops only that row — one
                # duplicate guid no longer aborts the whole sync batch.
                async with self._session.begin_nested():
                    persisted.append(
                        await _persist(self._session, episode)
                    )
            except IntegrityError as exc:
                if not _is_episode_guid_conflict(exc):
                    raise
        return persisted

    async def mark_processed(
        self, episode_id: UUID, processed: bool = True
    ) -> Optional[Episode]:
        # From backend.insights.pipeline.mark_episode_processed (query logic only —
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
            select(func.count(Episode.episode_id)).where(Episode.processed.is_(False))
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
        return await _persist(self._session, insight)

    async def save_many(self, insights: list[Insight]) -> list[Insight]:
        # Plain per-row upserts (DynamoDB's batch-put is PutItem per item);
        # insights carry no non-PK unique constraints, so no savepoint or
        # dedup is needed.
        persisted: list[Insight] = []
        for insight in insights:
            _guard_item_size(insight)
            persisted.append(
                await _persist(self._session, insight)
            )
        return persisted


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
        try:
            async with self._session.begin_nested():
                self._session.add(tag)
                await self._session.flush()
        except IntegrityError:
            # Lost a check-then-insert race with a concurrent transaction:
            # roll back only to the savepoint (the rest of the unit of
            # work survives) and return the row the winner committed, per
            # the ABC's "at most one tag visible afterwards" contract.
            # (The partial unique indexes from XIN-121 are the backstop —
            # including for NULL categories.)
            existing = await self.get_by_name_category(name, category)
            if existing is None:
                raise
            return existing
        return tag

    async def add_episode_tag(self, episode_id: UUID, tag_id: UUID) -> None:
        res = await self._session.execute(
            select(EpisodeTag).where(
                EpisodeTag.episode_id == episode_id,
                EpisodeTag.tag_id == tag_id,
            )
        )
        if res.scalar_one_or_none() is None:
            try:
                async with self._session.begin_nested():
                    self._session.add(
                        EpisodeTag(episode_id=episode_id, tag_id=tag_id)
                    )
                    await self._session.flush()
            except IntegrityError:
                # Check-then-insert race: re-check after the savepoint
                # rollback. If the link exists now, the duplicate add is
                # the protocol-promised no-op; otherwise this was a
                # different integrity failure — re-raise it.
                res = await self._session.execute(
                    select(EpisodeTag).where(
                        EpisodeTag.episode_id == episode_id,
                        EpisodeTag.tag_id == tag_id,
                    )
                )
                if res.scalar_one_or_none() is None:
                    raise

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
        return await _get_by_unique(self._session, User, User.user_id, user_id)

    async def get_by_email(self, email: str) -> Optional[User]:
        return await _get_by_unique(self._session, User, User.email, email)

    async def save(self, user: User) -> User:
        _guard_item_size(user)
        return await _persist(self._session, user)


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
        return await _get_by_unique(
            self._session, CuratedPlaylist, CuratedPlaylist.playlist_id, playlist_id
        )

    async def get_by_slug(self, slug: str) -> Optional[CuratedPlaylist]:
        return await _get_by_unique(
            self._session, CuratedPlaylist, CuratedPlaylist.slug, slug
        )

    async def save(self, playlist: CuratedPlaylist) -> CuratedPlaylist:
        _guard_item_size(playlist)
        return await self._save_playlist(playlist)

    async def _save_playlist(self, playlist: CuratedPlaylist) -> CuratedPlaylist:
        # The unguarded playlist write: SAVEPOINT-scoped slug-conflict
        # handling shared by save() and the _mutate_playlist helpers.
        # The slug is captured BEFORE any rollback can expire the
        # instance (XIN-119).
        slug = playlist.slug
        try:
            # Only the playlist flush rolls back on conflict — every other
            # flushed-but-uncommitted write in the caller's unit of work
            # (episodes, tags, progress) survives (XIN-119).
            async with self._session.begin_nested():
                return await _persist(self._session, playlist)
        except IntegrityError as exc:
            if _is_slug_conflict(exc):
                raise SlugConflictError(
                    f"playlist slug {slug!r} is already taken"
                ) from exc
            raise

    async def _mutate_playlist(
        self,
        playlist_id: UUID,
        mutate: Callable[[CuratedPlaylist], None],
    ) -> Optional[CuratedPlaylist]:
        """Load-mutate-save skeleton shared by publish/unpublish/rotate_token.

        Returns the saved playlist, or ``None`` when the id does not
        exist. Goes through the unguarded ``_save_playlist`` (not the
        public ``save()``): these operations only touch small fixed-size
        fields, and the entity was size-checked when first saved — so
        ``publish`` (like ``add_episode_tag``) never triggers the
        item-size guard (XIN-122).
        """
        playlist = await self.get_by_id(playlist_id)
        if playlist is None:
            return None
        mutate(playlist)
        return await self._save_playlist(playlist)

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
        try:
            await self._session.flush()
        except IntegrityError as exc:
            # FK parity with DynamoDB (Linear: XIN-124 — Chester's call):
            # surface the backend-agnostic MissingParentError when a
            # parent is missing. Only foreign-key violations map — any
            # other integrity error re-raises untouched.
            if "foreign key" in str(exc.orig).lower():
                raise MissingParentError(
                    f"playlist {playlist_id} or episode {episode_id} "
                    "does not exist"
                ) from exc
            raise

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

    async def list_entries(
        self, playlist_id: UUID
    ) -> list[PlaylistEpisodeEntry]:
        # Same join as list_episodes, but keep the link row so the RSS
        # endpoint gets position + added_at per episode. SQLite returns
        # naive datetimes; normalize to tz-aware UTC at the boundary.
        res = await self._session.execute(
            select(Episode, PlaylistEpisode)
            .join(
                PlaylistEpisode,
                PlaylistEpisode.episode_id == Episode.episode_id,
            )
            .where(PlaylistEpisode.playlist_id == playlist_id)
            .order_by(PlaylistEpisode.position.asc(), Episode.episode_id.asc())
        )
        entries: list[PlaylistEpisodeEntry] = []
        for episode, link in res.all():
            added_at = link.added_at
            if added_at is not None and added_at.tzinfo is None:
                added_at = added_at.replace(tzinfo=timezone.utc)
            entries.append(
                PlaylistEpisodeEntry(
                    episode=episode,
                    position=link.position,
                    added_at=added_at,
                )
            )
        return entries

    async def publish(
        self, playlist_id: UUID, visibility: str
    ) -> Optional[CuratedPlaylist]:
        validate_visibility(visibility)

        def _apply(playlist: CuratedPlaylist) -> None:
            playlist.visibility = visibility
            if playlist.slug is None:
                playlist.slug = generate_slug(playlist.title)
            if playlist.token is None:
                playlist.token = generate_token()

        return await self._mutate_playlist(playlist_id, _apply)

    async def unpublish(self, playlist_id: UUID) -> Optional[CuratedPlaylist]:
        return await self._mutate_playlist(
            playlist_id, lambda p: setattr(p, "visibility", VISIBILITY_UNLISTED)
        )

    async def rotate_token(self, playlist_id: UUID) -> Optional[str]:
        new_token = generate_token()

        def _rotate(playlist: CuratedPlaylist) -> None:
            playlist.token = new_token
            playlist.token_revoked_at = _now_utc()

        saved = await self._mutate_playlist(playlist_id, _rotate)
        return new_token if saved is not None else None


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
        # Genuine upsert by the composite primary key: the ABC contract for
        # save() is "upsert by primary key".
        _guard_item_size(progress)
        return await _persist(self._session, progress)


class _TaskLogRepository(TaskLogRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, task_log: TaskLog) -> TaskLog:
        _guard_item_size(task_log)
        return await _persist(self._session, task_log)

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

    async def get_by_type_and_episode(
        self,
        task_type: str,
        episode_id: UUID,
    ) -> Optional[TaskLog]:
        # XIN-45: idempotency lookup for the task outbox.
        res = await self._session.execute(
            select(TaskLog).where(
                TaskLog.task_type == task_type,
                TaskLog.episode_id == episode_id,
            )
        )
        return res.scalar_one_or_none()


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
