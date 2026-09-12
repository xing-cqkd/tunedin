"""Tests for backend.eval.tier1 (XIN-143 machinery gate)."""
from backend.eval.tier1 import TIER1_PASS, tier1_transfer_accuracy
from backend.insights.feed_template import (
    LABEL_POLICY,
    FeedTemplate,
    Slot,
)


def _slot(name, label, a, b):
    role, plan = LABEL_POLICY[label]
    return Slot(name=name, label=label, role=role, sample_plan=plan,
                pos_start=a, pos_end=b, pos_std=0.01, support=3, n_episodes=3)


def _template():
    return FeedTemplate(feed_id="f", slots=[
        _slot("intro_music", "intro-music", 0.0, 0.05),
        _slot("content", "content", 0.05, 0.9),
        _slot("outro_music", "outro-music", 0.9, 1.0),
    ], confidence=0.9)


def _gt(duration=1000.0):
    # ground truth sections matching the template exactly
    return ([(0.0, 50.0, "intro-music", 1.0),
             (50.0, 900.0, "content", 1.0),
             (900.0, 1000.0, "outro-music", 1.0)], duration)


def test_perfect_transfer_passes():
    rep = tier1_transfer_accuracy("f", _template(), [_gt(), _gt()])
    assert rep.n_heldout == 2
    content = rep.score("content")
    # 1s resolution + nearest-midpoint tie-break in slot overlaps costs a
    # couple of boundary seconds; the metric is exact, the test allows it.
    assert content.precision >= 0.99
    assert content.recall >= 0.99
    assert rep.placement_precision == 1.0
    assert rep.passed


def test_shifted_template_fails_recall():
    # template predicts content only in the middle; GT content is wider
    t = FeedTemplate(feed_id="f", slots=[
        _slot("content", "content", 0.4, 0.6)], confidence=0.9)
    rep = tier1_transfer_accuracy("f", t, [_gt()])
    content = rep.score("content")
    assert content.recall < TIER1_PASS["content_recall"]
    assert not rep.passed


def test_blank_template_reports_no_slots():
    t = FeedTemplate(feed_id="f", slots=[], confidence=0.0)
    rep = tier1_transfer_accuracy("f", t, [_gt()])
    assert not rep.passed
    assert "no slots" in rep.notes


def test_no_heldout():
    rep = tier1_transfer_accuracy("f", _template(), [])
    assert not rep.passed
    assert "no held-out" in rep.notes


def test_ad_bar_na_without_ad_ground_truth():
    # no ad-labeled GT seconds: ad bar must be n/a, not a failure
    rep = tier1_transfer_accuracy("f", _template(), [_gt()])
    assert rep.score("ad").actual_seconds == 0
    assert "ad bar n/a" in rep.notes
    assert rep.passed  # content bars carry it


def test_ad_precision_scored_when_ad_exists():
    t = FeedTemplate(feed_id="f", slots=[
        _slot("content", "content", 0.0, 0.8),
        _slot("ad", "ad", 0.8, 1.0),
    ], confidence=0.9)
    gt = ([(0.0, 800.0, "content", 1.0),
           (800.0, 1000.0, "ad", 1.0)], 1000.0)
    rep = tier1_transfer_accuracy("f", t, [gt])
    assert rep.score("ad").precision >= 0.99  # boundary-second noise, as above
    assert rep.ad_avoidance == 1.0  # no window touches the ad region
    assert rep.passed


def test_ad_bar_na_when_template_claims_no_ad_positions():
    # Template never predicts ad while ad GT exists: explicit n/a (not a
    # fail) — ad-safety rests on per-episode labeling. Segment-level skip
    # still protects the planned windows (PR #38 review fix, preserved).
    t = FeedTemplate(feed_id="f", slots=[
        _slot("content", "content", 0.0, 1.0)], confidence=0.9)
    gt = ([(0.0, 800.0, "content", 1.0),
           (800.0, 1000.0, "ad", 1.0)], 1000.0)
    rep = tier1_transfer_accuracy("f", t, [gt])
    assert rep.score("ad").precision is None  # never predicted
    assert "claims no ad positions" in rep.notes
    assert rep.ad_avoidance == 1.0
    assert rep.passed


def test_ad_bar_fails_when_template_claims_ad_badly():
    # Template DOES claim ad positions but lands them on content:
    # ad precision 0.4 < 0.8 -> fail.
    t = FeedTemplate(feed_id="f", slots=[
        _slot("content", "content", 0.0, 0.5),
        _slot("ad", "ad", 0.5, 1.0),
    ], confidence=0.9)
    gt = ([(0.0, 800.0, "content", 1.0),
           (800.0, 1000.0, "ad", 1.0)], 1000.0)
    rep = tier1_transfer_accuracy("f", t, [gt])
    assert rep.score("ad").precision < TIER1_PASS["ad_precision"]
    assert not rep.passed


def test_weak_pass_flagged_on_low_confidence():
    t = FeedTemplate(feed_id="f", slots=[
        _slot("content", "content", 0.0, 1.0)], confidence=0.1)
    gt = ([(0.0, 1000.0, "content", 1.0)], 1000.0)
    rep = tier1_transfer_accuracy("f", t, [gt])
    assert rep.passed  # accuracy bars carry it
    assert "weak pass" in rep.notes
