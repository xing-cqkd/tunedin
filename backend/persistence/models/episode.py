import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, List, Optional
from sqlalchemy import String, Text, DateTime, Integer, Boolean, ForeignKey, UniqueConstraint, Uuid
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
    guid: Mapped[Optional[str]] = mapped_column(String(512), nullable=True, index=True)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    audio_url: Mapped[str] = mapped_column(String(1024), nullable=False)
    duration: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    published_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    transcript: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
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
