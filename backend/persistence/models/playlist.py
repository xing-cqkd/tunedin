import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, List, Optional
from sqlalchemy import String, Text, DateTime, Integer, ForeignKey, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship
from backend.persistence.models.base import Base

if TYPE_CHECKING:
    from backend.persistence.models.user import User
    from backend.persistence.models.episode import Episode

class CuratedPlaylist(Base):
    __tablename__ = "curated_playlists"

    playlist_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.user_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    query_prompt: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False
    )

    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="playlists")
    episodes: Mapped[List["PlaylistEpisode"]] = relationship("PlaylistEpisode", back_populates="playlist", cascade="all, delete-orphan")


class PlaylistEpisode(Base):
    __tablename__ = "playlist_episodes"

    playlist_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("curated_playlists.playlist_id", ondelete="CASCADE"),
        primary_key=True
    )
    episode_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("episodes.episode_id", ondelete="CASCADE"),
        primary_key=True
    )
    position: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Relationships
    playlist: Mapped["CuratedPlaylist"] = relationship("CuratedPlaylist", back_populates="episodes")
    episode: Mapped["Episode"] = relationship("Episode", back_populates="playlist_episodes")
