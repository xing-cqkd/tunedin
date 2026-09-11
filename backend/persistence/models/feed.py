import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, List, Optional
from sqlalchemy import Boolean, DateTime, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship
from backend.persistence.models.base import Base

if TYPE_CHECKING:
    from backend.persistence.models.episode import Episode

class Feed(Base):
    __tablename__ = "feeds"

    feed_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    # XIN-47: unbounded — real feed URLs and titles exceed the old
    # String(1024)/String(512) caps and Postgres raises DataError on
    # overflow instead of truncating.
    rss_url: Mapped[str] = mapped_column(Text, unique=True, nullable=False, index=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    author: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    image_url: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    category: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    language: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    website_url: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    feed_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)  # episodic vs serial
    podcast_guid: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    explicit: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    
    # Ingestion sync metadata
    etag: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    last_modified: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    last_fetched_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    # XIN-46: sync_all_pending_feeds / batch_runner / crawler all filter
    # Feed by sync_status; the index avoids full table scans at scale.
    sync_status: Mapped[str] = mapped_column(String(50), default="pending", nullable=False, index=True)
    error_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False
    )

    # Relationships
    episodes: Mapped[List["Episode"]] = relationship("Episode", back_populates="feed", cascade="all, delete-orphan")
