"""XIN-143 Tier 1: template transfer accuracy (machinery gate).

For each held-out episode of a feed, the learned template predicts region
labels (slot spans scaled to the episode duration) and sample placements
(plan_samples on the aligned sections). Both are compared against the
actual sections — publisher chapter marks (eval.chapters), an independent
ground truth, never the labeler's own output.

Metrics:
  per-label precision/recall in seconds (1s resolution):
      precision(L) = seconds correctly predicted as L / seconds predicted as L
      recall(L)    = seconds correctly predicted as L / seconds actually L
  placement_precision: fraction of deep_90s planned seconds that fall inside
      actual content-labeled seconds (are we transcribing the right stuff?)
  ad_avoidance: fraction of actual ad-labeled seconds NOT covered by any
      planned window (are we skipping ads?). None when no ad ground truth.

Production-ready bar (TIER1_PASS), defined here per the issue:
  content precision/recall >= 0.75, placement_precision >= 0.80,
  ad precision >= 0.80 whenever the template CLAIMS ad positions (predicts
  ad seconds). A template that claims no ad positions gets an explicit n/a
  — ad-safety in production rests on per-episode labeling (XIN-139/XIN-148),
  which Tier 1 cannot validate without circularity. A feed with no ad
  chapters at all likewise reports n/a rather than failing.

Low-confidence templates (< 0.5) that pass the accuracy bars are flagged
as weak passes, not failed: Tier 1 measures transfer accuracy, and
confidence gating is XIN-140 policy.

What this does NOT measure (documented seam): the runtime section labeler
(XIN-139) is not built, so transfer is measured given chapter-quality
sections. Re-run Tier 1 when XIN-139 lands; the harness is labeler-agnostic.
"""
from dataclasses import dataclass, field

from backend.eval.chapters import chapters_to_sections
from backend.insights.feed_template import FeedTemplate, align, plan_samples

# Production-ready bar, Tier 1. Set conservatively: a template that cannot
# place content windows at 80% precision wastes transcription budget, and
# false ad-skips (low ad precision) are the costly error direction.
TIER1_PASS = {
    "content_precision": 0.75,
    "content_recall": 0.75,
    "placement_precision": 0.80,
    "ad_precision": 0.80,
}

_SCORED_LABELS = ("ad", "content", "intro-music", "outro-music")


@dataclass
class LabelScores:
    label: str
    precision: float | None  # None when the template never predicts the label
    recall: float | None     # None when the ground truth has no such seconds
    pred_seconds: int
    actual_seconds: int


@dataclass
class Tier1Report:
    feed_id: str
    n_heldout: int
    label_scores: list[LabelScores] = field(default_factory=list)
    placement_precision: float | None = None
    ad_avoidance: float | None = None
    planned_seconds: float = 0.0
    n_windows: int = 0
    passed: bool = False
    notes: str = ""

    def score(self, label: str) -> LabelScores | None:
        return next((s for s in self.label_scores if s.label == label), None)


def _predict_label(template: FeedTemplate, rel_pos: float) -> str | None:
    """Template-predicted label at a relative position.

    Slots are regions; pick the covering slot whose midpoint is nearest
    (deterministic tie-break on slot order). None when no slot covers.
    """
    best, best_key = None, None
    for i, sl in enumerate(template.slots):
        if sl.pos_start - 0.001 <= rel_pos <= sl.pos_end + 0.001:
            mid = (sl.pos_start + sl.pos_end) / 2
            key = (abs(mid - rel_pos), i)
            if best_key is None or key < best_key:
                best, best_key = sl.label, key
    return best


def _actual_label(sections: list[tuple], t: float) -> str | None:
    for s, e, lab, _ in sections:
        if s <= t < e:
            return lab
    return None


def tier1_transfer_accuracy(feed_id: str, template: FeedTemplate,
                            heldout: list[tuple[list[tuple], float]]
                            ) -> Tier1Report:
    """Run Tier 1 for one feed.

    heldout: [(ground_truth_sections, duration)] — sections as
    (start_s, end_s, canonical_label, density) tuples, e.g. from
    eval.chapters.chapters_to_sections.
    """
    rep = Tier1Report(feed_id=feed_id, n_heldout=len(heldout))
    if not template.slots:
        rep.notes = "template has no slots: nothing to transfer."
        return rep
    if not heldout:
        rep.notes = "no held-out episodes."
        return rep

    # per-label second counts, aggregated over held-out episodes
    pred_hit = {L: 0 for L in _SCORED_LABELS}
    pred_tot = {L: 0 for L in _SCORED_LABELS}
    act_tot = {L: 0 for L in _SCORED_LABELS}

    deep_planned_in_content = 0.0
    deep_planned_total = 0.0
    ad_seconds_total = 0.0
    ad_seconds_planned = 0.0

    for sections, duration in heldout:
        if duration <= 0:
            continue
        n = int(duration)
        for t in range(n):
            rel = t / duration
            p = _predict_label(template, rel)
            a = _actual_label(sections, t)
            if a in act_tot:
                act_tot[a] += 1
            if p in pred_tot:
                pred_tot[p] += 1
                if p == a:
                    pred_hit[p] += 1
        # sample placement on the aligned ground-truth sections
        alignment = align(sections, duration, template)
        windows = plan_samples(template, alignment, duration)
        rep.n_windows += len(windows)
        for ws, we, purpose in windows:
            wlen = max(0.0, we - ws)
            rep.planned_seconds += wlen
            # per-second check against ground truth labels
            for t in range(int(ws), int(we)):
                a = _actual_label(sections, t)
                if purpose.startswith("deep:"):
                    deep_planned_total += 1
                    if a == "content":
                        deep_planned_in_content += 1
                if a == "ad":
                    ad_seconds_planned += 1
        for t in range(n):
            if _actual_label(sections, t) == "ad":
                ad_seconds_total += 1

    for L in _SCORED_LABELS:
        rep.label_scores.append(LabelScores(
            label=L,
            precision=(round(pred_hit[L] / pred_tot[L], 3)
                       if pred_tot[L] else None),
            recall=(round(pred_hit[L] / act_tot[L], 3)
                    if act_tot[L] else None),
            pred_seconds=pred_tot[L],
            actual_seconds=act_tot[L],
        ))
    rep.placement_precision = (round(deep_planned_in_content /
                                     deep_planned_total, 3)
                               if deep_planned_total else None)
    rep.ad_avoidance = (round(1 - ad_seconds_planned / ad_seconds_total, 3)
                        if ad_seconds_total else None)

    content = rep.score("content")
    ad = rep.score("ad")
    checks = [
        content is not None and content.precision is not None
        and content.precision >= TIER1_PASS["content_precision"],
        content is not None and content.recall is not None
        and content.recall >= TIER1_PASS["content_recall"],
        rep.placement_precision is not None
        and rep.placement_precision >= TIER1_PASS["placement_precision"],
    ]
    # ad bar: applies only when the template CLAIMS ad positions
    # (predicts ad seconds). Rationale, measured not assumed: on DarkHorse
    # the learned ad slot scored P=0.188 — mid-roll ad positions do not
    # transfer positionally. Ad-safety in production rests on per-episode
    # labeling (XIN-139/XIN-148 segment-level skip), which Tier 1 cannot
    # validate without circularity (its sections ARE the ground truth).
    # A template that claims no ad positions gets an explicit n/a — not a
    # silent pass, not a spurious fail.
    if ad is not None and ad.pred_seconds > 0:
        checks.append(ad.precision is not None
                      and ad.precision >= TIER1_PASS["ad_precision"])
    elif ad is not None and ad.actual_seconds > 0:
        rep.notes += ("template claims no ad positions though ad ground "
                      "truth exists: ad bar n/a; ad-safety rests on "
                      "per-episode labeling (XIN-139/XIN-148). ")
    else:
        rep.notes += "no ad-labeled ground truth: ad bar n/a. "
    # A degenerate single-slot template can pass the accuracy bars while
    # learning nothing structural; flag it rather than fail it — Tier 1
    # measures transfer accuracy, confidence gating is XIN-140 policy.
    if template.confidence < 0.5:
        rep.notes += (f"weak pass: template confidence "
                      f"{template.confidence} < 0.5. ")
    rep.passed = all(checks)
    if not rep.passed:
        rep.notes += "below production-ready bar (see TIER1_PASS)."
    return rep
