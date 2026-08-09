import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional
from sqlalchemy import String, Text, DateTime, Integer, ForeignKey, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship
from backend.persistence.models.base import Base

if TYPE_CHECKING:
    from backend.persistence.models.episode import Episode

class Insight(Base):
    __tablename__ = "insights"

    insight_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    episode_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("episodes.episode_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    timestamp_seconds: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    detail: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    insight_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False
    )

    # Relationships
    episode: Mapped["Episode"] = relationship("Episode", back_populates="insights")
