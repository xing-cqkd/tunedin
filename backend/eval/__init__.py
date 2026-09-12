"""Evaluation harness for feed-template transfer and template-guided extraction.

XIN-143 (eval) and XIN-144 (template-guided insight extraction) share this
package. Two tiers, two different questions:

Tier 1 — machinery gate (``tier1.py``):
    Does a learned template transfer to a held-out episode? Compares
    template-predicted region labels and sample placements against actual
    sections, reporting precision/recall per label. Ground truth comes from
    the publisher's own chapter marks (``chapters.py``) — an independent
    source, never the labeler's own output (the XIN-141 agent review flow is
    the planned long-term ground truth; chapters are the stand-in).

Tier 2 — product gate (``tier2.py``):
    End-to-end bake-off on ~20-30 episodes across feeds: text-only,
    first-4-min, uniform 2x90s, template-guided (XIN-144), full-transcription.
    Metric is insight yield per transcription-minute. Template-guided must
    beat text-only by a stated margin and approach full-transcription yield
    at ~10% of its cost.

Production-ready is defined quantitatively in ``tier1.py`` (TIER1_PASS) and
``tier2.py`` (TIER2_PASS). Tier 2 passing is the gate for XIN-144.

Known seams (documented, not hidden):
- XIN-139 (real audio classifiers) is not built: Tier 1 measures template
  transfer given chapter-quality sections. Re-run Tier 1 when XIN-139 lands.
- XIN-141 (agent review flow) is not built: chapter marks stand in as the
  independent ground truth.
- XIN-140 (sample planner) exists as ``plan_samples`` in
  ``backend.insights.feed_template`` (merged, PR #38); XIN-144 consumes it.
"""
