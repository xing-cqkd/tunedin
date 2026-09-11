"""Feed template persistence models (XIN-136).

Templates are versioned by revision: every learn/drift event appends a new
row; history is never overwritten (INSERT OR REPLACE would destroy it).
Rows are keyed (feed_id, episode_type, rev); the live template is the max
rev per (feed_id, episode_type). labeler_version tags which labeler produced
the labels so XIN-139 re-runs can invalidate and re-learn templates.
Drift decisions (XIN-141) are recorded with their rationale.
"""
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    DateTime,
    Float,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column

from backend.persistence.models.base import Base


class FeedTemplateRecord(Base):
    __tablename__ = "feed_templates"

    template_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    feed_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("feeds.feed_id"), nullable=False, index=True
    )
    episode_type: Mapped[str] = mapped_column(String(32), nullable=False, default="full")
    rev: Mapped[int] = mapped_column(Integer, nullable=False)
    labeler_version: Mapped[str] = mapped_column(String(64), nullable=False, default="crude-0")
    template_json: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    learned_from: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("feed_id", "episode_type", "rev",
                         name="uq_feed_template_rev"),
    )


class DriftDecision(Base):
    __tablename__ = "drift_decisions"

    decision_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    feed_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("feeds.feed_id"), nullable=False, index=True
    )
    episode_type: Mapped[str] = mapped_column(String(32), nullable=False, default="full")
    template_rev: Mapped[int] = mapped_column(Integer, nullable=False)
    decision: Mapped[str] = mapped_column(
        String(32), nullable=False
    )  # versioned | one_off | escalated
    rationale: Mapped[str] = mapped_column(Text, nullable=False)
    decided_by: Mapped[str] = mapped_column(
        String(64), nullable=False, default="agent"
    )  # agent | code
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
