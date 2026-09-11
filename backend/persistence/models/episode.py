import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, List, Optional
from sqlalchemy import Index, String, Text, DateTime, Integer, Boolean, ForeignKey, UniqueConstraint, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship
from backend.persistence.models.base import Base

if TYPE_CHECKING:
    from backend.persistence.models.feed import Feed
    from backend.persistence.models.user import User
    from backend.persistence.models.insight import Insight
    from backend.persistence.models.tag import EpisodeTag
    from backend.persistence.models.playlist import PlaylistEpisode

class Episode(Base):
    __tablename__ = "episodes"
    __table_args__ = (
        UniqueConstraint("feed_id", "guid", name="uq_episode_feed_guid"),
        # XIN-46: get_unprocessed_episodes filters on (feed_id, processed);
        # the composite index keeps it off full table scans at scale.
        Index("ix_episodes_feed_processed", "feed_id", "processed"),
    )

    episode_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    feed_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("feeds.feed_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    # XIN-68: never null. The (feed_id, guid) dedup unique constraint treats
    # NULLs as distinct, so a nullable guid would permit unlimited
    # (feed_id, NULL) duplicates and re-insertion on every sync. The parser
    # already guarantees a non-empty guid; legacy NULLs were backfilled with
    # a deterministic fallback in migration 0f3a4b5c6d7e.
    guid: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    # XIN-47: real-world podcast titles and enclosure URLs routinely exceed
    # the old String(512)/String(1024) caps, which raise DataError on
    # Postgres instead of truncating. Text is unbounded on both dialects.
    title: Mapped[str] = mapped_column(Text, nullable=False)
    audio_url: Mapped[str] = mapped_column(Text, nullable=False)
    duration: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    published_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    content_html: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    transcript: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    transcript_url: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    chapters_url: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    image_url: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    # Never null: the write path coerces an unset/None value to "full"
    # (parity with the DynamoDB backend's codec.apply_defaults), matching
    # migration 742ddc0a7799 (XIN-122).
    episode_type: Mapped[str] = mapped_column(
        String(50), default="full", server_default="full", nullable=False
    )  # full, trailer, bonus
    episode_number: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    season_number: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    explicit: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    processed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False
    )

    # Relationships
    feed: Mapped["Feed"] = relationship("Feed", back_populates="episodes")
    insights: Mapped[List["Insight"]] = relationship("Insight", back_populates="episode", cascade="all, delete-orphan")
    tags: Mapped[List["EpisodeTag"]] = relationship("EpisodeTag", back_populates="episode", cascade="all, delete-orphan")
    user_progress: Mapped[List["UserEpisodeProgress"]] = relationship("UserEpisodeProgress", back_populates="episode", cascade="all, delete-orphan")
    playlist_episodes: Mapped[List["PlaylistEpisode"]] = relationship("PlaylistEpisode", back_populates="episode", cascade="all, delete-orphan")


class UserEpisodeProgress(Base):
    __tablename__ = "user_episode_progress"

    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.user_id", ondelete="CASCADE"),
        primary_key=True
    )
    episode_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("episodes.episode_id", ondelete="CASCADE"),
        primary_key=True
    )
    position_seconds: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    completed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    last_played_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False
    )

    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="episode_progress")
    episode: Mapped["Episode"] = relationship("Episode", back_populates="user_progress")
