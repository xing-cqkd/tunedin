"""Tests for backend.eval.tier2 (XIN-143 product gate)."""
import pytest

from backend.eval.tier2 import (
    STRATEGIES,
    TIER2_PASS,
    GradeCard,
    plan_strategy_windows,
    summarize_tier2,
)
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
        _slot("content", "content", 0.0, 1.0)], confidence=0.9)


def test_window_plans():
    assert plan_strategy_windows("text_only", duration=3600) == []
    assert plan_strategy_windows("first_4min", duration=3600) == [
        (0.0, 240.0, "first_4min")]
    assert plan_strategy_windows("first_4min", duration=100) == [
        (0.0, 100.0, "first_4min")]
    uni = plan_strategy_windows("uniform_2x90s", duration=3600)
    assert len(uni) == 2
    assert uni[0][0] == round(1200 - 45, 1)
    assert uni[1][0] == round(2400 - 45, 1)
    assert all(e - s == 90.0 for s, e, _ in uni)
    assert plan_strategy_windows("full", duration=3600) == [
        (0.0, 3600.0, "full")]
    with pytest.raises(ValueError):
        plan_strategy_windows("nope", duration=3600)
    with pytest.raises(ValueError):
        plan_strategy_windows("template_guided", duration=3600)


def test_uniform_2x90s_short_episode_dedupes():
    # 60s episode: both fractions land on the same (0, 60) window —
    # one window, not two identical ones (double transcription cost).
    wins = plan_strategy_windows("uniform_2x90s", duration=60)
    assert wins == [(0.0, 60.0, "uniform_2x90s")]


def test_template_guided_uses_xin144_planner():
    # single content slot: two longest sections get deep windows
    sections = [(0.0, 1800.0, "content", 1.0),
                (1800.0, 3600.0, "content", 1.0)]
    wins = plan_strategy_windows("template_guided", duration=3600.0,
                                 template=_template(), sections=sections)
    assert len(wins) == 2
    assert all(p.startswith("deep:") for _, _, p in wins)
    assert all(e - s == 90.0 for s, e, _ in wins)


def _card(ep, strategy, quotes, takes, minutes):
    return GradeCard(episode_id=ep, feed_id="f", strategy=strategy,
                     transcription_minutes=minutes,
                     usable_quotes=quotes, timestamped_takeaways=takes)


def test_bars_pass():
    # tg: 30 useful in 7 min; text: 10 useful in 0; full: 60 useful in 200 min
    cards = []
    for i in range(10):
        cards.append(_card(f"e{i}", "text_only", 1, 0, 0.0))
        cards.append(_card(f"e{i}", "template_guided", 2, 1, 0.7))
        cards.append(_card(f"e{i}", "full", 4, 2, 20.0))
    rep = summarize_tier2(cards)
    assert rep.n_episodes == 10
    assert rep.bar_a_ratio == 3.0
    # yield/min: tg 30/7=4.29, full 60/200=0.3 -> ratio 14.3; cost 7/200=0.035
    # yield/min rounds to 3dp before the ratio: tg 4.286, full 0.3
    assert rep.bar_b_yield_ratio == round(round(30 / 7, 3) / round(60 / 200, 3), 3)
    assert rep.bar_b_cost_ratio == round(7 / 200, 3)
    assert rep.passed


def test_bar_a_fails_when_tg_matches_text():
    cards = []
    for i in range(5):
        cards.append(_card(f"e{i}", "text_only", 1, 1, 0.0))
        cards.append(_card(f"e{i}", "template_guided", 1, 1, 0.7))
        cards.append(_card(f"e{i}", "full", 4, 2, 20.0))
    rep = summarize_tier2(cards)
    assert rep.bar_a_ratio == 1.0 < TIER2_PASS["beat_text_margin"]
    assert not rep.passed
    assert "Bar A" in rep.notes


def test_bar_a_degenerate_text_baseline():
    # text_only yields nothing: denominator floors at 1, so one lucky
    # guided quote (1.0 < 1.5) fails but two useful insights pass.
    def rep_for(tg_quotes):
        return summarize_tier2([
            _card("e0", "text_only", 0, 0, 0.0),
            _card("e0", "template_guided", tg_quotes, 0, 0.7),
            _card("e0", "full", 4, 2, 20.0),
        ])
    rep = rep_for(1)
    assert rep.bar_a_ratio == 1.0
    assert not rep.passed
    rep2 = rep_for(2)
    assert rep2.bar_a_ratio == 2.0


def test_missing_strategies_not_silent():
    rep = summarize_tier2([_card("e1", "text_only", 1, 0, 0.0)])
    assert not rep.passed
    assert "missing strategies" in rep.notes


def test_all_strategies_known():
    assert set(STRATEGIES) == {"text_only", "first_4min", "uniform_2x90s",
                               "template_guided", "full"}
