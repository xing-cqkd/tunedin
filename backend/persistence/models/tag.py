import uuid
from typing import TYPE_CHECKING, List, Optional
from sqlalchemy import String, ForeignKey, UniqueConstraint, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship
from backend.persistence.models.base import Base

if TYPE_CHECKING:
    from backend.persistence.models.episode import Episode

class Tag(Base):
    __tablename__ = "tags"
    __table_args__ = (
        UniqueConstraint("name", "category", name="uq_tag_name_category"),
    )

    tag_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    category: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Relationships
    episodes: Mapped[List["EpisodeTag"]] = relationship("EpisodeTag", back_populates="tag", cascade="all, delete-orphan")


class EpisodeTag(Base):
    __tablename__ = "episode_tags"

    episode_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("episodes.episode_id", ondelete="CASCADE"),
        primary_key=True
    )
    tag_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tags.tag_id", ondelete="CASCADE"),
        primary_key=True
    )

    # Relationships
    episode: Mapped["Episode"] = relationship("Episode", back_populates="tags")
    tag: Mapped["Tag"] = relationship("Tag", back_populates="episodes")
