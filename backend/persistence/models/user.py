import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, List
from sqlalchemy import String, DateTime, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship
from backend.persistence.models.base import Base

if TYPE_CHECKING:
    from backend.persistence.models.playlist import CuratedPlaylist
    from backend.persistence.models.episode import UserEpisodeProgress

class User(Base):
    __tablename__ = "users"

    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False
    )

    # Relationships
    playlists: Mapped[List["CuratedPlaylist"]] = relationship("CuratedPlaylist", back_populates="user", cascade="all, delete-orphan")
    episode_progress: Mapped[List["UserEpisodeProgress"]] = relationship("UserEpisodeProgress", back_populates="user", cascade="all, delete-orphan")
