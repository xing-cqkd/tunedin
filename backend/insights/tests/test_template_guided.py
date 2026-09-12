"""Tests for backend.insights.template_guided (XIN-144)."""
import asyncio
import uuid

from backend.insights.feed_template import LABEL_POLICY, FeedTemplate, Slot
from backend.insights.template_guided import (
    GuidedInsight,
    extract_template_guided,
    guided_insight_id,
    merge_insights,
    persist_guided_extraction,
    plan_guided_windows,
)


def _slot(name, label, a, b):
    role, plan = LABEL_POLICY[label]
    return Slot(name=name, label=label, role=role, sample_plan=plan,
                pos_start=a, pos_end=b, pos_std=0.01, support=3, n_episodes=3)


def _template(confidence=0.9):
    return FeedTemplate(feed_id="f", slots=[
        _slot("intro_music", "intro-music", 0.0, 0.05),
        _slot("content", "content", 0.05, 0.95),
        _slot("outro_music", "outro-music", 0.95, 1.0),
    ], confidence=confidence)


def _sections():
    return [(0.0, 100.0, "intro-music", 1.0),
            (100.0, 1800.0, "content", 1.0),
            (1800.0, 2000.0, "outro-music", 1.0)]


def test_plan_guided_windows_skips_music_slots():
    wins = plan_guided_windows(_template(), _sections(), 2000.0)
    # intro/outro are anchor plans -> skipped; one deep window on content
    assert len(wins) == 1
    s, e, purpose = wins[0]
    assert purpose == "deep:content"
    assert e - s == 90.0
    assert 100.0 <= s and e <= 1800.0


def _fetch(url, start, dur):
    _fetch.calls.append((url, start, dur))
    return ("transcript text here", [("hello", 1.0, 1.5)])


_fetch.calls = []


def _extract(transcript, window_start, words, meta):
    return [{"insight_type": "quote", "title": "A quote",
             "detail": transcript, "timestamp_rel": 1.0},
            {"insight_type": "takeaway", "title": "No timestamp",
             "detail": "d", "timestamp_rel": None}]


def test_extract_template_guided_end_to_end():
    meta = {"audio_url": "http://x/y.mp3"}
    res = extract_template_guided("ep1", meta, _template(), _sections(),
                                  2000.0, _fetch, _extract)
    assert len(res.windows) == 1
    assert res.transcripts == ["transcript text here"]
    assert len(res.insights) == 2
    q = res.insights[0]
    # relative timestamp becomes absolute: window_start + 1.0
    assert q.timestamp_seconds == int(round(res.windows[0][0] + 1.0))
    assert res.insights[1].timestamp_seconds is None
    assert not res.low_confidence
    assert res.alignment_score > 0


def test_low_confidence_flagged_not_blocked():
    res = extract_template_guided("ep1", {"audio_url": "u"}, _template(0.1),
                                  _sections(), 2000.0, _fetch, _extract)
    assert res.low_confidence
    assert "best-effort" in res.notes
    assert len(res.insights) == 2  # still runs: Tier 2 needs the data


def test_empty_transcript_skips_extractor():
    def _empty(url, start, dur):
        return ("", [])
    called = []
    res = extract_template_guided("ep1", {"audio_url": "u"}, _template(),
                                  _sections(), 2000.0, _empty,
                                  lambda *a: called.append(1))
    assert called == []
    assert res.insights == []


def _g(title, ts):
    return GuidedInsight("quote", title, "d", ts, 0.0, 90.0, "deep:content")


def test_merge_prefers_guided_on_collision():
    text = [{"insight_type": "story", "title": "A Quote!",
             "detail": "old", "timestamp_seconds": None},
            {"insight_type": "story", "title": "Text only",
             "detail": "t", "timestamp_seconds": None}]
    merged = merge_insights(text, [_g("a quote", 42)])
    assert len(merged) == 2
    assert merged[0]["timestamp_seconds"] == 42  # guided won
    assert merged[0]["source"] == "template_guided"
    assert merged[1]["title"] == "Text only"


def test_merge_empty_titles_never_dedupe():
    # Two untitled guided insights are distinct records; the old dict-keyed
    # merge silently dropped all but one.
    guided = [_g("", 10), _g("", 20)]
    merged = merge_insights([], guided)
    assert len(merged) == 2
    assert {m["timestamp_seconds"] for m in merged} == {10, 20}


def test_merge_repeated_guided_titles_keep_order():
    guided = [_g("Same", 10), _g("Same", 20)]
    merged = merge_insights([{"title": "Same", "detail": "t",
                              "timestamp_seconds": None}], guided)
    assert len(merged) == 2
    assert merged[0]["timestamp_seconds"] == 10  # first guided wins collision
    assert merged[1]["timestamp_seconds"] == 20  # second survives


def test_guided_insight_id_stable_and_unique():
    a = guided_insight_id("ep1", "Q", 42, 0.0, 0)
    b = guided_insight_id("ep1", "Q", 42, 0.0, 0)
    assert a == b and isinstance(a, uuid.UUID)
    # empty / repeated titles still get distinct ids via (window, index)
    c = guided_insight_id("ep1", "", 42, 0.0, 0)
    d = guided_insight_id("ep1", "", 42, 0.0, 1)
    assert c != d
    assert guided_insight_id("ep1", "Q", 43, 0.0, 0) != a


class _FakeInsight:
    def __init__(self, insight_id, title, timestamp_seconds=None,
                 detail="d", insight_type="quote"):
        self.insight_id = insight_id
        self.title = title
        self.timestamp_seconds = timestamp_seconds
        self.detail = detail
        self.insight_type = insight_type


class _FakeRepo:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.saved = []

    async def list_by_episode(self, episode_id):
        return list(self.rows)

    async def save_many(self, insights):
        self.saved.extend(insights)
        for ins in insights:
            for i, r in enumerate(self.rows):
                if r.insight_id == ins.insight_id:
                    self.rows[i] = ins
                    break
            else:
                self.rows.append(ins)
        return insights


def _run(coro):
    return asyncio.run(coro)


def test_persist_inserts_and_is_idempotent():
    repo = _FakeRepo()
    r1 = _run(persist_guided_extraction(repo, "ep1", [_g("Q", 42)]))
    assert (r1.inserted, r1.promoted, r1.skipped) == (1, 0, 0)
    assert len(repo.rows) == 1
    # rerun: same deterministic id -> upsert, no duplicate
    r2 = _run(persist_guided_extraction(repo, "ep1", [_g("Q", 42)]))
    assert (r2.inserted, r2.promoted, r2.skipped) == (0, 0, 1)
    assert len(repo.rows) == 1
    assert r1.insight_ids == r2.insight_ids


def test_persist_promotes_untimestamped_text_row():
    text_row = _FakeInsight(uuid.uuid4(), "A Quote", None)
    repo = _FakeRepo([text_row])
    r = _run(persist_guided_extraction(repo, "ep1", [_g("a quote", 42)]))
    assert (r.inserted, r.promoted, r.skipped) == (0, 1, 0)
    assert len(repo.rows) == 1  # no duplicate row
    assert repo.rows[0].insight_id == text_row.insight_id  # id kept
    assert repo.rows[0].timestamp_seconds == 42  # timestamp filled


def test_persist_repeated_titles_dont_clobber_promote():
    # One untitled-text row "Same" + two guided "Same"@10/@20: the first
    # promotes the text row, the second must insert (not re-promote and
    # clobber the first timestamp).
    text_row = _FakeInsight(uuid.uuid4(), "Same", None)
    repo = _FakeRepo([text_row])
    gs = [GuidedInsight("quote", "Same", "d1", 10, 0.0, 90.0, "deep:content"),
          GuidedInsight("quote", "Same", "d2", 20, 90.0, 180.0, "deep:content")]
    r = _run(persist_guided_extraction(repo, "ep1", gs))
    assert (r.inserted, r.promoted, r.skipped) == (1, 1, 0)
    assert len(repo.rows) == 2
    by_ts = sorted(row.timestamp_seconds for row in repo.rows)
    assert by_ts == [10, 20]
    # rerun is stable
    r2 = _run(persist_guided_extraction(repo, "ep1", gs))
    assert (r2.inserted, r2.promoted, r2.skipped) == (0, 0, 2)
    assert len(repo.rows) == 2


def test_persist_leaves_timestamped_text_row_alone():
    text_row = _FakeInsight(uuid.uuid4(), "Q", 99)
    repo = _FakeRepo([text_row])
    r = _run(persist_guided_extraction(repo, "ep1", [_g("Q", 42)]))
    # different timestamp -> distinct insight, inserted alongside
    assert (r.inserted, r.promoted, r.skipped) == (1, 0, 0)
    assert len(repo.rows) == 2


def test_extract_clamps_out_of_window_timestamp():
    def _bad_rel(transcript, window_start, words, meta):
        return [{"insight_type": "takeaway", "title": "T", "detail": "d",
                 "timestamp_rel": 500.0}]  # window is only 90s
    res = extract_template_guided("ep1", {"audio_url": "u"}, _template(),
                                  _sections(), 2000.0, _fetch, _bad_rel)
    s, e, _ = res.windows[0]
    assert res.insights[0].timestamp_seconds == int(round(e))


def test_extract_notes_missing_audio_url():
    res = extract_template_guided("ep1", {}, _template(), _sections(),
                                  2000.0, _fetch,
                                  lambda *a: [{"title": "T",
                                               "timestamp_rel": None}])
    assert "no audio_url" in res.notes
