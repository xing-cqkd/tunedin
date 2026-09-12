"""Tests for backend.eval.chapters (XIN-143 ground truth)."""
from backend.eval.chapters import (
    Chapter,
    chapter_label,
    chapters_to_sections,
    parse_chapters,
)


def test_m_ss_vs_h_mm_ss():
    # "(2:30)" is 150 seconds, NOT 2h30m. This exact bug appeared in the
    # Tier 1 prototype (Russillo chapters rendered as 2:30:00).
    ch = parse_chapters("(2:30) LSU smashes Clemson\n(8:20) Oregon survives",
                        None, 3600.0)
    assert [c.start_s for c in ch] == [150.0, 500.0]
    ch2 = parse_chapters("(1:02:10) Arch Manning\n(1:25:00) Life advice",
                         None, 7200.0)
    assert [c.start_s for c in ch2] == [3730.0, 5100.0]


def test_title_and_label_extraction():
    ch = parse_chapters(
        "(0:00) Intro\n(5:03) Astronaut Training\n(1:23:45) Outro\n"
        "(1:30:00) A word from our sponsors",
        None, 6000.0)
    assert [c.title for c in ch] == ["Intro", "Astronaut Training", "Outro",
                                     "A word from our sponsors"]
    assert [c.label for c in ch] == ["intro-music", "content", "outro-music",
                                     "ad"]
    # last chapter runs to duration
    assert ch[-1].end_s == 6000.0
    assert ch[0].end_s == 303.0


def test_chapter_label_keywords():
    assert chapter_label("Sponsors") == "ad"
    assert chapter_label("Advertisement") == "ad"
    assert chapter_label("Promo: new show") == "ad"
    assert chapter_label("Outro") == "outro-music"
    assert chapter_label("Closing credits") == "outro-music"
    assert chapter_label("Intro") == "intro-music"
    assert chapter_label("LSU smashes Clemson") == "content"
    assert chapter_label("") == "content"
    # "Introducing the guest" is not an intro-music chapter
    assert chapter_label("Introducing the guest") == "content"


def test_html_stripped_and_unescaped():
    ch = parse_chapters("<p>(2:30) LSU &amp; Clemson</p><p>(8:20) Next</p>",
                        None, 3600.0)
    assert ch[0].title == "LSU & Clemson"


def test_needs_two_marks():
    assert parse_chapters("(2:30) Only one", None, 3600.0) == []
    assert parse_chapters("no timestamps here", None, 3600.0) == []


def test_out_of_order_and_beyond_duration_dropped():
    ch = parse_chapters("(8:20) B\n(2:30) A\n(2:30) dup\n(99:00) too far",
                        None, 600.0)
    assert [c.start_s for c in ch] == [150.0, 500.0]


def test_breaking_point_is_not_a_break():
    # Regression: "break" ⊂ "breaking" once labeled this chapter 'silence'.
    # Stronger rule now: "break" is never a silence marker (60k-episode
    # sample showed it is always sports-content language).
    assert chapter_label("Is there a breaking point for fans?") == "content"
    assert chapter_label("Mid-roll break") == "content"
    assert chapter_label("Sponsors") == "ad"
    chs = [Chapter(0.0, 60.0, "Intro", "intro-music"),
           Chapter(60.0, 600.0, "Talk", "content")]
    secs = chapters_to_sections(chs)
    assert secs == [(0.0, 60.0, "intro-music", 1.0),
                    (60.0, 600.0, "content", 1.0)]


def test_break_is_never_a_silence_marker():
    # A 60k-episode sample: every "break" title is sports-content language
    # ("breakdown", "breakout", "break down") — never a boundary marker.
    assert chapter_label("We break down the top prospects") == "content"
    assert chapter_label("Is there a breaking point for fans?") == "content"
    assert chapter_label("Quick break") == "content"
    assert chapter_label("Break") == "content"
    assert chapter_label("Mid-roll break") == "content"


def test_title_truncated_at_next_timestamp_on_shared_line():
    # "(2:30) Intro (5:00) Ad read": the first chapter's title must not
    # absorb the next chapter's title (which once mislabeled it 'ad').
    chs = parse_chapters("(2:30) Intro (5:00) Ad read (10:00) Main", None,
                         3600)
    assert len(chs) == 3
    assert chs[0].title == "Intro"
    assert chs[0].label == "intro-music"  # exact title in _INTRO_TITLES
    assert chs[1].title == "Ad read"
    assert chs[1].label == "ad"
    assert chs[2].title == "Main"


def test_duration_none_or_zero_returns_no_chapters():
    assert parse_chapters("(1:00) A (2:00) B", None, None) == []
    assert parse_chapters("(1:00) A (2:00) B", None, 0) == []
