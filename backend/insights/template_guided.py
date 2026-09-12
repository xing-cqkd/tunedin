"""Template-guided insight extraction (XIN-144).

The depth half of the pipeline: text gives coverage, template-guided audio
gives the right minutes. For one episode:

    template -> align(sections) -> plan_samples() -> transcribe windows
        -> extract insights per window -> timestamped insights

This module is orchestration only. Two functions are injected:

* ``fetch_transcribe_fn(audio_url, start_s, dur_s) -> (text, words)`` —
  audio I/O plus ASR (see backend.eval.audio.fetch_and_transcribe).
* ``extractor_fn(transcript, window_start_s, words, episode_meta)
  -> list[dict]`` — the insight extraction itself, one dict per insight
  with keys ``insight_type``/``title``/``detail``/``timestamp_rel``
  (seconds into the window, or None). Production use is manual
  application of backend/insights/EXTRACT.md's insight procedure to the
  transcript chunk (standing rule: no outside model); a future LLM call
  slots into the same contract. Tests stub it.

Timestamps: relative word/ASR timestamps become absolute via
``timestamp_seconds = window_start_s + timestamp_rel``. This is what the
text-only pipeline cannot produce — and the single biggest weakness
Chester found in the Chinese Lore extraction (insights restating the
summary with NULL timestamps).

Gating: extraction runs regardless of template confidence, but
``low_confidence`` is flagged on the result so the backfill (and the
Tier 2 analysis) can condition on it. Confidence gating policy itself
belongs to XIN-140, not here.
"""
from dataclasses import dataclass, field
from uuid import UUID
import uuid as _uuid

from backend.insights.feed_template import (
    FeedTemplate,
    align,
    plan_samples,
)

# Below this, the template is a guess more than knowledge; extraction still
# runs (Tier 2 needs the confidence/yield relationship), but the result is
# flagged.
LOW_CONFIDENCE_THRESHOLD = 0.3


@dataclass
class GuidedInsight:
    insight_type: str
    title: str
    detail: str
    timestamp_seconds: int | None
    window_start: float
    window_end: float
    purpose: str  # plan_samples purpose, e.g. 'deep:content'


@dataclass
class GuidedExtraction:
    episode_id: str
    windows: list = field(default_factory=list)  # [(start, end, purpose)]
    transcripts: list = field(default_factory=list)  # [text] per window
    insights: list = field(default_factory=list)  # [GuidedInsight]
    alignment_score: float = 0.0
    template_confidence: float = 0.0
    low_confidence: bool = False
    notes: str = ""


def plan_guided_windows(template: FeedTemplate, sections: list[tuple],
                        duration: float) -> list[tuple]:
    """Transcription windows for one episode (XIN-144; also used by Tier 2).

    Skips ad/close/boundary slots, maps rundown/unknown slots with 30s,
    goes deep (90s) on the two longest content sections. Segment-level
    skip wins: a section the labeler calls skippable is never deep-sampled
    even inside a content slot span.
    """
    alignment = align(sections, duration, template)
    return plan_samples(template, alignment, duration)


def extract_template_guided(episode_id: str, episode_meta: dict,
                            template: FeedTemplate, sections: list[tuple],
                            duration: float,
                            fetch_transcribe_fn,
                            extractor_fn) -> GuidedExtraction:
    """Run template-guided extraction for one episode."""
    result = GuidedExtraction(
        episode_id=episode_id,
        template_confidence=template.confidence,
        low_confidence=template.confidence < LOW_CONFIDENCE_THRESHOLD,
    )
    if result.low_confidence:
        result.notes = (
            f"template confidence {template.confidence} < "
            f"{LOW_CONFIDENCE_THRESHOLD}: windows are best-effort.")
    alignment = align(sections, duration, template)
    result.alignment_score = alignment.score
    windows = plan_samples(template, alignment, duration)
    result.windows = windows
    audio_url = episode_meta.get("audio_url")
    if not audio_url:
        if windows:
            result.notes += " no audio_url: windows planned but not transcribed."
        return result
    for start, end, purpose in windows:
        text, words = fetch_transcribe_fn(audio_url, start, end - start)
        result.transcripts.append(text)
        if not text.strip():
            continue
        for raw in extractor_fn(text, start, words, episode_meta) or []:
            rel = raw.get("timestamp_rel")
            ts = None
            if rel is not None:
                # Clamp to the window: the insight came from this audio.
                ts = int(round(min(max(start + rel, start), end)))
            result.insights.append(GuidedInsight(
                insight_type=raw.get("insight_type", "takeaway"),
                title=raw.get("title", ""),
                detail=raw.get("detail", ""),
                timestamp_seconds=ts,
                window_start=start,
                window_end=end,
                purpose=purpose,
            ))
    return result


def _norm_title(title: str) -> str:
    return "".join(c for c in title.lower() if c.isalnum())


def _dedup_key(title: str) -> str | None:
    """Normalized title for dedup, or None when the title is empty.

    Empty titles carry no identity: two untitled insights must never
    collapse into each other (they used to — the dict key "" silently
    dropped all but one).
    """
    key = _norm_title(title or "")
    return key or None


def merge_insights(text_insights: list[dict],
                   guided_insights: list[GuidedInsight]) -> list[dict]:
    """Merge text-first and template-guided insights for one episode.

    Dedupes on normalized title. On collision the guided (audio-verified,
    timestamped) insight wins — it is the more specific record. Text
    insights keep their relative order; surviving guided insights append
    after. Insights with empty titles never dedupe (no identity to match
    on); repeated guided titles keep first occurrence order.
    """
    guided_by_title: dict[str, list[GuidedInsight]] = {}
    guided_untitled: list[GuidedInsight] = []
    for g in guided_insights:
        key = _dedup_key(g.title)
        if key is None:
            guided_untitled.append(g)
        else:
            guided_by_title.setdefault(key, []).append(g)
    merged = []
    for t in text_insights:
        key = _dedup_key(t.get("title", ""))
        bucket = guided_by_title.get(key) if key else None
        if bucket:
            merged.append(_guided_to_dict(bucket.pop(0)))
            if not bucket:
                del guided_by_title[key]
        else:
            merged.append(dict(t))
    for bucket in guided_by_title.values():
        merged.extend(_guided_to_dict(g) for g in bucket)
    merged.extend(_guided_to_dict(g) for g in guided_untitled)
    return merged


def _guided_to_dict(g: GuidedInsight) -> dict:
    return {
        "insight_type": g.insight_type,
        "title": g.title,
        "detail": g.detail,
        "timestamp_seconds": g.timestamp_seconds,
        "source": "template_guided",
        "window": [g.window_start, g.window_end],
    }


# Deterministic IDs make guided extraction idempotent: a rerun produces the
# same primary keys, so repository.save() upserts instead of duplicating.
GUIDED_INSIGHT_NAMESPACE = _uuid.uuid5(_uuid.NAMESPACE_URL,
                                       "tunedin:xin-144:template-guided")


def guided_insight_id(episode_id: str, title: str,
                      timestamp_seconds: int | None,
                      window_start: float, index: int) -> UUID:
    """Stable primary key for one guided insight.

    (window_start, index) disambiguate empty or repeated titles, which
    carry no usable identity on their own.
    """
    key = _dedup_key(title) or "untitled"
    return _uuid.uuid5(
        GUIDED_INSIGHT_NAMESPACE,
        f"{episode_id}:{key}:{timestamp_seconds}:{window_start}:{index}")


@dataclass
class GuidedPersistReport:
    episode_id: str
    inserted: int = 0
    promoted: int = 0  # existing text row upgraded in place (timestamp added)
    skipped: int = 0   # already present
    insight_ids: list[UUID] = field(default_factory=list)


async def persist_guided_extraction(insight_repo, episode_id,
                                    guided_insights: list[GuidedInsight],
                                    ) -> GuidedPersistReport:
    """Persist guided insights idempotently via an InsightRepository.

    Semantics (the Insight table has no source column, so provenance is
    not stored on the row — it lives in the GuidedExtraction result):

    * Each guided insight gets a deterministic UUID5 id. Reruns upsert
      the same rows instead of duplicating them.
    * If a text-pipeline row with the same title already exists but has
      no timestamp, it is *promoted* in place: timestamp/detail are
      filled from the guided insight and its original id is kept. The
      guided (timestamped) record is the more specific one — same rule
      as merge_insights.
    * A text row that already has the same title AND timestamp is left
      alone (skipped).

    Returns a report of what happened; never deletes rows.
    """
    from backend.persistence.models.insight import Insight

    report = GuidedPersistReport(episode_id=str(episode_id))
    existing = await insight_repo.list_by_episode(episode_id)
    # (dedup_key, timestamp) -> row, for rows with usable titles
    by_key: dict[tuple[str, int | None], object] = {}
    for row in existing:
        key = _dedup_key(row.title or "")
        if key is not None:
            by_key.setdefault((key, row.timestamp_seconds), row)

    to_save = []
    for i, g in enumerate(guided_insights):
        key = _dedup_key(g.title)
        if key is not None and (key, g.timestamp_seconds) in by_key:
            report.skipped += 1
            report.insight_ids.append(by_key[(key, g.timestamp_seconds)].insight_id)
            continue
        promoted = None
        if key is not None and g.timestamp_seconds is not None:
            # Promote an untimestamped text row with the same title.
            cand = by_key.get((key, None))
            if cand is not None:
                promoted = cand
        if promoted is not None:
            promoted.timestamp_seconds = g.timestamp_seconds
            if g.detail:
                promoted.detail = g.detail
            if g.insight_type:
                promoted.insight_type = g.insight_type
            to_save.append(promoted)
            report.promoted += 1
            report.insight_ids.append(promoted.insight_id)
            # keep the lookup fresh: the row no longer sits at (key, None),
            # so a later same-titled guided insight inserts instead of
            # re-promoting (which would clobber this timestamp).
            del by_key[(key, None)]
            by_key[(key, g.timestamp_seconds)] = promoted
        else:
            row = Insight(
                insight_id=guided_insight_id(str(episode_id), g.title,
                                            g.timestamp_seconds,
                                            g.window_start, i),
                episode_id=episode_id,
                insight_type=g.insight_type,
                title=g.title,
                detail=g.detail,
                timestamp_seconds=g.timestamp_seconds,
            )
            to_save.append(row)
            report.inserted += 1
            report.insight_ids.append(row.insight_id)
            if key is not None:
                by_key.setdefault((key, g.timestamp_seconds), row)
    if to_save:
        await insight_repo.save_many(to_save)
    return report
