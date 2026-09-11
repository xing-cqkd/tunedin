"""Template store: persistence operations for feed templates (XIN-136).

Sync SQLAlchemy session API (scaffolding): the async ingestion path can run
these in a threadpool at XIN-140 wiring time. Every learn/drift event appends
a new revision; history is never overwritten.
"""
import uuid
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from backend.insights.feed_template import FeedTemplate
from backend.persistence.models.feed_template import DriftDecision, FeedTemplateRecord


def save_template(session: Session, feed_id: uuid.UUID,
                  template: FeedTemplate) -> FeedTemplateRecord:
    """Append a new revision of the template for (feed_id, episode_type)."""
    max_rev = session.scalar(
        select(func.max(FeedTemplateRecord.rev)).where(
            FeedTemplateRecord.feed_id == feed_id,
            FeedTemplateRecord.episode_type == template.episode_type,
        )
    )
    rec = FeedTemplateRecord(
        feed_id=feed_id,
        episode_type=template.episode_type,
        rev=(max_rev or 0) + 1,
        labeler_version=template.labeler_version,
        template_json=template.to_json(),
        confidence=template.confidence,
        learned_from=list(template.learned_from),
        notes=template.notes or None,
    )
    session.add(rec)
    session.commit()
    session.refresh(rec)
    return rec


def load_template(session: Session, feed_id: uuid.UUID,
                  episode_type: str = "full") -> Optional[FeedTemplate]:
    """Load the latest revision for (feed_id, episode_type), or None."""
    rec = session.scalar(
        select(FeedTemplateRecord)
        .where(FeedTemplateRecord.feed_id == feed_id,
               FeedTemplateRecord.episode_type == episode_type)
        .order_by(FeedTemplateRecord.rev.desc())
        .limit(1)
    )
    if rec is None:
        return None
    return FeedTemplate.from_json(str(feed_id), rec.template_json)


def list_history(session: Session, feed_id: uuid.UUID,
                 episode_type: str = "full") -> list[FeedTemplateRecord]:
    """All revisions for (feed_id, episode_type), oldest first."""
    return list(session.scalars(
        select(FeedTemplateRecord)
        .where(FeedTemplateRecord.feed_id == feed_id,
               FeedTemplateRecord.episode_type == episode_type)
        .order_by(FeedTemplateRecord.rev.asc())
    ))


def record_drift_decision(session: Session, feed_id: uuid.UUID,
                          episode_type: str, template_rev: int,
                          decision: str, rationale: str,
                          decided_by: str = "agent") -> DriftDecision:
    """Persist an XIN-141 drift decision with its rationale."""
    if decision not in ("versioned", "one_off", "escalated"):
        raise ValueError(f"unknown drift decision: {decision}")
    rec = DriftDecision(
        feed_id=feed_id,
        episode_type=episode_type,
        template_rev=template_rev,
        decision=decision,
        rationale=rationale,
        decided_by=decided_by,
    )
    session.add(rec)
    session.commit()
    session.refresh(rec)
    return rec
