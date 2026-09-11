import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, List, Optional
from sqlalchemy import String, Text, DateTime, Integer, ForeignKey, UniqueConstraint, Uuid, text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from backend.persistence.models.base import Base

if TYPE_CHECKING:
    from backend.persistence.models.user import User
    from backend.persistence.models.episode import Episode

class CuratedPlaylist(Base):
    __tablename__ = "curated_playlists"
    __table_args__ = (
        # Named to match migration a49b13bc7cab, so create_all test DBs and
        # migrated DBs agree — and so slug-conflict detection can match the
        # constraint name instead of message text (XIN-119).
        UniqueConstraint("slug", name="uq_curated_playlists_slug"),
    )

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

    # Publish state (Linear: XIN-97). ``visibility`` is 'unlisted' (default)
    # or 'public' — validated in ``PlaylistRepository.publish`` on both
    # backends. ``slug`` is unique and URL-safe; ``token`` is a nullable
    # 256-bit URL-safe secret for unlisted share URLs. Rotating the token
    # replaces the value (single-field change kills old URLs) and stamps
    # ``token_revoked_at``. ``frozen_at`` supports the freeze-version
    # toggle (set by a later issue).
    visibility: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="unlisted",
        # Matches the server_default from migration a49b13bc7cab, so
        # create_all test DBs agree with migrated DBs (XIN-122).
        server_default="unlisted",
    )
    slug: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )
    token: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    token_revoked_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    frozen_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

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

    # When the episode was added to the playlist (Linear: XIN-98). Drives
    # the RSS <pubDate> ("new" badges fire on curator adds). Set once on
    # insert; re-adding an existing link updates ``position`` only, so the
    # original added date is preserved.
    added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        # Matches migration 742ddc0a7799 (XIN-122): the column is NOT NULL
        # with a server default, so create_all test DBs agree with
        # migrated DBs.
        server_default=text("CURRENT_TIMESTAMP"),
        nullable=False,
    )

    # Relationships
    playlist: Mapped["CuratedPlaylist"] = relationship("CuratedPlaylist", back_populates="episodes")
    episode: Mapped["Episode"] = relationship("Episode", back_populates="playlist_episodes")
