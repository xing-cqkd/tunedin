import uuid
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import Index, String, Text, DateTime, Uuid
from sqlalchemy.orm import Mapped, mapped_column
from backend.persistence.models.base import Base

class TaskLog(Base):
    __tablename__ = "task_logs"
    __table_args__ = (
        # XIN-45: idempotency key for the task outbox. (task_type, episode_id)
        # pairs are unique so a re-enqueue of the same episode's task is a
        # no-op instead of a duplicate row. NULL episode_ids (tasks not tied
        # to an episode) never conflict: both Postgres and SQLite treat NULLs
        # as distinct in unique indexes.
        Index(
            "uq_task_log_type_episode",
            "task_type",
            "episode_id",
            unique=True,
        ),
    )

    task_log_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    task_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    # XIN-45: FK-ish linkage for the durable task outbox — the episode this
    # task was queued for. Nullable: not every task type ties to an episode.
    episode_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        Uuid(as_uuid=True), nullable=True, index=True
    )
    payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(50), default="pending", nullable=False, index=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False
    )
