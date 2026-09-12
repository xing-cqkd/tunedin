# Eval harness: XIN-143 (Tier 1 + Tier 2)

Two tiers, two questions. Both are deterministic code; the only human
judgment in the loop is the Tier 2 grading rubric.

## Tier 1 — machinery gate (`tier1.py`)

Does a learned template transfer to held-out episodes of the same feed?

- Learn: `propose_template` on chapter-derived sections of N>=3 episodes
  (`chapters.parse_chapters` + `chapters_to_sections`).
- Held-out: every other chaptered episode of the feed.
- Predicted: template slot spans scaled to episode duration
  (`_predict_label`); sample placements via `align` + `plan_samples`.
- Actual: chapter sections with canonical labels (`chapter_label`).

Metrics: per-label precision/recall in seconds; `placement_precision`
(fraction of deep_90s planned seconds inside actual content);
`ad_avoidance` (fraction of actual ad seconds no window touches).

Production-ready bar (`TIER1_PASS`): content precision/recall >= 0.75,
placement_precision >= 0.80, ad precision >= 0.80 when the template claims
positional ad slots (n/a otherwise — a missing bar is reported, never
silently passed). Low-confidence (< 0.5) templates that pass the accuracy
bars are flagged as weak passes; confidence gating is XIN-140 policy.

## Tier 2 — product gate (`tier2.py`)

Five strategies head-to-head on ~20-30 episodes across feeds:

| strategy | audio transcribed |
|---|---|
| text_only | none |
| first_4min | [0, 240s] |
| uniform_2x90s | 90s at 1/3 and 2/3 |
| template_guided | XIN-144: `plan_guided_windows` |
| full | whole episode |

Grading (by hand, one rater per episode across all five strategies):
usable verbatim quotes, timestamped takeaways (verified/corrected),
tone-mismatch catches (0/1). `ad_skip_precision` is computed from chapter
ground truth, not judged.

- U(s) = quotes + takeaways; C(s) = transcription minutes.
- Bar A: U(template_guided) >= 1.5 * U(text_only)
- Bar B: U(tg)/C(tg) >= 0.7 * U(full)/C(full) AND C(tg) <= 0.15 * C(full)

Tier 2 passing is the production-ready gate and gates XIN-144.

## Ground truth (`chapters.py`)

Publisher chapter marks (`(2:30) Title`) parsed from shownotes, mapped to
the provisional closed label vocabulary by keyword rules. Independent of
our section labeler by construction — the issue requires ground truth
from a source other than the labeler's own output.

Two hard-won keyword rules: matching is whole-word (substring matching
once turned "breaking point" into a silence label), and "break" is never
a silence marker — a 60k-episode sample showed every "break" chapter
title is sports-content language ("breakdown", "breakout", "break down
the trade"), never a boundary marker.

## Audio (`audio.py`)

Byte-range fetching + ffmpeg decode + faster-whisper transcription with
word timestamps. The model object is injected; the module never imports
it. Network failures return "", [] (best-effort, same policy as the
100-episode audio scan).

## Running it

Tier 1 is fully automatic:

```python
from backend.eval.chapters import parse_chapters, chapters_to_sections
from backend.eval.tier1 import tier1_transfer_accuracy
from backend.insights.feed_template import propose_template

learn = {eid: (chapters_to_sections(parse_chapters(s, h, d)), d) ...}
template = propose_template(feed_id, learn)
heldout = [(chapters_to_sections(parse_chapters(s, h, d)), d) ...]
report = tier1_transfer_accuracy(feed_id, template, heldout)
```

Tier 2 needs transcripts first (see the runner notes in the XIN-143
issue), then `plan_strategy_windows` per strategy, hand grading into
`GradeCard`s, and `summarize_tier2(cards)`.

## Known seams

- XIN-139 (real audio classifiers) not built: Tier 1 measures transfer
  given chapter-quality sections. Re-run when it lands.
- XIN-141 (agent review flow) not built: chapters stand in as the
  independent ground truth.
- XIN-140's planner exists as `plan_samples` (merged PR #38); XIN-144
  consumes it via `plan_guided_windows`.
