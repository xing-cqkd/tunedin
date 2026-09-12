"""Independent ground truth from publisher chapter marks (XIN-143 Tier 1).

Chapter marks are the publisher's own structural annotations
(e.g. "(2:30) LSU smashes Clemson"). They are independent of our section
labeler, which is exactly what Tier 1 needs: the issue requires ground
truth from a source other than the labeler's own output (long-term: the
XIN-141 agent review flow; chapters are the stand-in until it exists).

Two timestamp spellings are accepted:
  M:SS      -> minutes:seconds   (e.g. "(2:30)")
  H:MM:SS   -> hours:minutes:seconds (e.g. "(1:02:10)")

Chapter titles map to the provisional closed label vocabulary owned by
XIN-148 (see backend.insights.feed_template.CANONICAL_LABELS) via
keyword rules. Titles that say nothing structural are 'content'.
"""
import html
import re
from dataclasses import dataclass

from backend.insights.feed_template import canonicalize_label

# Parenthesised timestamps only: bare "2:30" appears in prose ("tip-off at
# 2:30") far too often to trust. Publishers that mean chapters parenthesise.
_TIMESTAMP_RE = re.compile(r"\((\d{1,3}):(\d{2})(?::(\d{2}))?\)")
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")

_INTRO_TITLES = {
    "intro", "introduction", "opening", "theme", "theme song", "cold open",
    "preview", "teaser",
}


@dataclass
class Chapter:
    start_s: float
    end_s: float  # next chapter's start, or the episode duration
    title: str
    label: str  # canonical label from chapter_label()


def _tok_match(tokens: set[str], *kws: str) -> bool:
    # Whole-word matching (plurals allowed). Substring matching once turned
    # the chapter "Is there a breaking point for fans?" into a 'silence'
    # label via "break" ⊂ "breaking" — caught by the Tier 1 run, not by eye.
    for kw in kws:
        if kw == "advertis":  # deliberate stem: advertisement/advertising
            if any(w.startswith(kw) for w in tokens):
                return True
        elif any(w == kw or w == kw + "s" or w == kw + "es"
                 for w in tokens):
            return True
    return False


def _to_seconds(m: re.Match) -> float:
    g1, g2, g3 = m.group(1), m.group(2), m.group(3)
    if g3 is None:
        return int(g1) * 60 + int(g2)
    return int(g1) * 3600 + int(g2) * 60 + int(g3)


def chapter_label(title: str) -> str:
    """Map a chapter title to the provisional closed label vocabulary."""
    t = _WS_RE.sub(" ", title.strip().lower())
    if not t:
        return "content"
    tokens = set(re.findall(r"[a-z0-9]+", t))
    if _tok_match(tokens, "sponsor", "advertis", "promo", "commercial",
                  "ad", "ads"):
        return canonicalize_label("ad")
    if _tok_match(tokens, "outro", "closing", "credit"):
        return canonicalize_label("outro-music")
    if t in _INTRO_TITLES:
        return canonicalize_label("intro-music")
    if _tok_match(tokens, "intermission", "interlude", "jingle"):
        return canonicalize_label("silence")
    # NOTE: "break" is deliberately NOT a silence keyword. A 60k-episode
    # sample showed every "break" chapter title is sports-content language
    # ("breakdown", "breakout", "break down the trade") — never an actual
    # boundary marker. Labeling them 'silence' once turned 29% of a Ringer
    # episode into a silence slot and failed its Tier 1 transfer.
    return canonicalize_label("content")


def parse_chapters(summary: str | None, content_html: str | None,
                  duration: float) -> list[Chapter]:
    """Extract chapter sections from shownotes text.

    Returns [] when fewer than two usable marks are found (one mark makes
    no sections). Marks must be strictly increasing and within the episode
    duration; the last chapter runs to ``duration``.
    """
    if not duration or duration <= 0:
        return []
    raw = html.unescape(_TAG_RE.sub("\n", (summary or "") + "\n" +
                                    (content_html or "")))
    marks: list[tuple[float, str]] = []
    for m in _TIMESTAMP_RE.finditer(raw):
        sec = _to_seconds(m)
        if duration > 0 and sec > duration:
            continue
        line_end = raw.find("\n", m.end())
        title = raw[m.end():line_end if line_end >= 0 else m.end() + 100]
        # Chapters often share one line ("(2:30) Intro (5:00) Main"): cut
        # the title at the next timestamp, else the next chapter's title
        # (e.g. "Ad read") pollutes this chapter's label.
        nxt = _TIMESTAMP_RE.search(title)
        if nxt:
            title = title[:nxt.start()]
        title = _WS_RE.sub(" ", title.strip(" \t-–—:")).strip()[:100]
        marks.append((sec, title))
    marks.sort(key=lambda x: x[0])
    # strictly increasing: drop duplicates and out-of-order marks
    clean: list[tuple[float, str]] = []
    for sec, title in marks:
        if clean and sec <= clean[-1][0]:
            continue
        clean.append((sec, title))
    if len(clean) < 2:
        return []
    chapters = []
    for i, (sec, title) in enumerate(clean):
        end = clean[i + 1][0] if i + 1 < len(clean) else duration
        if end <= sec:
            continue
        chapters.append(Chapter(start_s=round(sec, 1), end_s=round(end, 1),
                                title=title, label=chapter_label(title)))
    return chapters


def chapters_to_sections(chapters: list[Chapter]) -> list[tuple]:
    """Chapter list -> section tuples for the template machinery.

    Section format matches propose_template/align input:
    (start_s, end_s, label, density). Density is 1.0: chapters are
    human-authored, so every section is fully trusted.
    """
    return [(c.start_s, c.end_s, c.label, 1.0) for c in chapters]
