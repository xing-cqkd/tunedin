"""Deterministic episode-type classification (XIN-134).

The extraction prompt (EXTRACT.md, dimension 10) asks the LLM to classify
every episode as full / trailer / bonus / preview / rebroadcast, but the
``episode_type`` metadata field is unreliable in the wild and LLM judgment
alone let a trailer slip into a "commute episodes" feed during the
10-question retrieval eval. This module is the cheap, testable backstop:
title patterns + duration heuristics the extraction pipeline and the
retrieval layer can apply without an LLM call.

Rules mirror EXTRACT.md strict rule 2 (title-pattern backstop):
- an explicit publisher ``episode_type`` of trailer/bonus is trusted;
- title patterns for trailer / preview / bonus / rebroadcast are checked
  next, with a duration / promotional-notes confirmation for trailer-type
  matches (an episode *about* movie trailers is a full episode);
- under 5 minutes + promotional language always means trailer;
- under 5 minutes alone does NOT (daily news briefs are full episodes).

Known limitation: a full episode with "bonus" incidentally in the title
(e.g. "The Bonus Army") is only caught when phrased as "bonus episode"
or parenthesized/bracketed. Prefer the LLM classification when available;
this module is the floor, not the ceiling.
"""

from __future__ import annotations

import re

FULL = "full"
TRAILER = "trailer"
BONUS = "bonus"
PREVIEW = "preview"
REBROADCAST = "rebroadcast"

_SHORT_SECONDS = 300  # under 5 minutes: trailer-length until proven otherwise

_TRAILER_RES = [
    re.compile(p, re.IGNORECASE)
    for p in (r"\btrailer\b", r"\bteaser\b", r"sneak\s*peek", r"coming\s+soon", r"first\s+look")
]
_PREVIEW_RES = [re.compile(r"\bpreview\b", re.IGNORECASE)]
# "bonus" only when clearly a label: "Bonus Episode", "(Bonus)", "[Bonus]",
# leading "Bonus: ...". A bare "bonus" inside a normal title ("The Bonus
# Army") is left for the LLM to decide.
_BONUS_RES = [
    re.compile(p, re.IGNORECASE)
    for p in (r"\bbonus\s+episode\b", r"[\(\[]bonus[\)\]]", r"^\s*bonus\s*[:\-]")
]
_REBROADCAST_RES = [
    re.compile(p, re.IGNORECASE)
    for p in (
        r"\brebroadcast\b",
        r"\bencore\b",
        r"from the archives",
        r"originally aired",
        r"\bbest of\b",
        r"\bthrowback\b",
        r"\bflashback\b",
        r"\bfbf\b",
        r"from the vault",
        r"we revisit",
        r"\bclassics?\b",
    )
]
_PROMO_RES = [
    re.compile(p, re.IGNORECASE)
    for p in (r"subscribe", r"don'?t miss", r"coming soon", r"sneak peek", r"\bteaser\b")
]


def _matches(text: str | None, patterns: list[re.Pattern]) -> bool:
    if not text:
        return False
    return any(p.search(text) for p in patterns)


def _promo_context(title: str | None, notes: str | None, duration_seconds: float | None) -> bool:
    """True when the surrounding evidence says 'promotional', not substantive."""
    if _matches(notes, _PROMO_RES):
        return True
    if duration_seconds is not None and duration_seconds < _SHORT_SECONDS:
        return True
    return False


def classify_episode_type(
    title: str | None,
    duration_seconds: float | None = None,
    episode_type_meta: str | None = None,
    notes: str | None = None,
) -> str:
    """Classify an episode as full/trailer/bonus/preview/rebroadcast.

    Args:
        title: episode title (required for pattern matching).
        duration_seconds: episode duration; None when unknown.
        episode_type_meta: raw ``episode_type`` feed metadata, if present.
        notes: summary/shownotes text, used only for the promo-language check.
    """
    meta = (episode_type_meta or "").strip().lower()
    if meta == "trailer":
        return TRAILER
    if meta == "bonus":
        return BONUS

    if _matches(title, _REBROADCAST_RES):
        return REBROADCAST
    if _matches(title, _BONUS_RES):
        return BONUS
    if _matches(title, _TRAILER_RES):
        # "Movie Trailer Breakdown" (60 min, substantive) is a full episode;
        # a 2-minute "Show Trailer" is not. Duration unknown -> trust the title.
        if duration_seconds is None or _promo_context(title, notes, duration_seconds):
            return TRAILER
    if _matches(title, _PREVIEW_RES):
        if duration_seconds is None or _promo_context(title, notes, duration_seconds):
            return PREVIEW
    # Docstring rule: under 5 minutes + promotional language always means
    # trailer, even with a generic title ("Welcome to the show") that
    # matches no title pattern above. Under 5 minutes alone does NOT
    # (daily news briefs are full episodes) — promotional language is
    # required here, unlike the title-pattern branches where short
    # duration alone confirms the trailer label.
    if (
        duration_seconds is not None
        and duration_seconds < _SHORT_SECONDS
        and _matches(notes, _PROMO_RES)
    ):
        return TRAILER
    return FULL


def is_trailer_like(
    title: str | None,
    duration_seconds: float | None = None,
    episode_type_meta: str | None = None,
    notes: str | None = None,
) -> bool:
    """True for trailers, previews, and bonus episodes: not full-episode content."""
    return classify_episode_type(title, duration_seconds, episode_type_meta, notes) in (
        TRAILER,
        PREVIEW,
        BONUS,
    )
