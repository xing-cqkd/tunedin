"""Tests for backend.insights.feed_template (XIN-136/XIN-137).

Ports the tunedin-extract prototype tests and adds the expert-panel
(2026-09-11) regression tests: learning floor, medoid tie-break stability,
episode-type stratification, no rundown auto-assign, slot-coverage drift
signal, duration weighting, label-disagreement confidence, drift hysteresis,
and the template store.
"""
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from backend.insights import feed_template as ft
from backend.insights.feed_template import (
    FeedTemplate,
    align,
    canonicalize_label,
    is_drift,
    learn_templates,
    plan_samples,
    propose_template,
)
from backend.insights.template_store import (
    list_history,
    load_template,
    record_drift_decision,
    save_template,
)
from backend.persistence.models import Base, Feed, FeedTemplateRecord


def ep(dur, segs):
    return ([(s, e, lab, 0.0) for s, e, lab in segs], dur)


def skeleton(dur=3600, ad_at=(120, 210)):
    a0, a1 = ad_at
    return ep(dur, [(0, 60, 'intro-music'), (60, a0, 'content'),
                    (a0, a1, 'ad-like'), (a1, dur - 300, 'content'),
                    (dur - 300, dur, 'outro-music')])


# --- ported prototype tests -------------------------------------------------

def test_variable_length_perfect_labels():
    A = skeleton(3600)
    B = skeleton(1800)
    C = skeleton(5400)
    t = propose_template('sim', {'A': A, 'B': B, 'C': C})
    labels = [s.label for s in t.slots]
    assert labels == ['intro-music', 'content', 'ad', 'content',
                      'outro-music'], labels
    assert t.confidence >= 0.8, t.confidence
    ad = next(s for s in t.slots if s.label == 'ad')
    assert ad.role == 'ad' and ad.sample_plan == 'skip', ad
    assert all(s.support == 3 for s in t.slots)
    assert '[auto]' not in t.notes, t.notes


def test_uniform_labels_honest_collapse():
    A = ep(3600, [(0, 1200, 'content'), (1200, 2400, 'content'),
                  (2400, 3600, 'content')])
    B = ep(1800, [(0, 900, 'content'), (900, 1800, 'content')])
    t = propose_template('sim', {'A': A, 'B': B})
    assert len(t.slots) == 1, [s.name for s in t.slots]
    assert t.confidence < 0.3, t.confidence
    assert 'one label' in t.notes and 'content' in t.notes, t.notes


def test_insertion_does_not_pollute():
    base = [(0, 60, 'intro-music'), (60, 1200, 'content'),
            (1200, 1500, 'content'), (1500, 1560, 'outro-music')]
    A = ep(1560, base)
    B = ep(1560, base)
    C = ep(1650, [(0, 60, 'intro-music'), (60, 1200, 'content'),
                  (1200, 1290, 'ad-like'), (1290, 1590, 'content'),
                  (1590, 1650, 'outro-music')])
    t = propose_template('sim', {'A': A, 'B': B, 'C': C})
    core = [s for s in t.slots if s.label in ('intro-music', 'outro-music')]
    assert len(core) == 2 and all(s.support >= 2 for s in core), \
        [(s.label, s.support) for s in t.slots]
    # the extra ad (support 1 < need 2) must be dropped, not kept half-hearted
    assert not [s for s in t.slots if s.label == 'ad'], \
        [(s.label, s.support) for s in t.slots]
    assert t.confidence > 0.3, t.confidence


def test_align_held_out():
    A = skeleton(3600)
    B = skeleton(3600)
    t = propose_template('sim', {'A': A})
    # n=1: slots still induced, but confidence capped with an honest note
    assert len(t.slots) == 5
    assert t.confidence < 0.5 and '[auto]' in t.notes, (t.confidence, t.notes)
    a = align(*B, t)
    assert a.score == 1.0, (a.score, a.segments)
    assert a.slot_coverage == 1.0
    assert a.unmatched_slots == []
    assert [s[3] for s in a.segments] == [s.name for s in t.slots]


def test_determinism():
    A = skeleton(3600)
    B = skeleton(1800)
    j1 = propose_template('sim', {'A': A, 'B': B}).to_json()
    j2 = propose_template('sim', {'A': A, 'B': B}).to_json()
    import json
    d1, d2 = json.loads(j1), json.loads(j2)
    d1.pop('updated_at'); d2.pop('updated_at')
    assert d1 == d2, 'non-deterministic induction'
    t = FeedTemplate.from_json('sim', j1)
    assert [s.label for s in t.slots] == ['intro-music', 'content', 'ad',
                                         'content', 'outro-music']


def test_v1_compat():
    import json
    v1 = json.dumps({'feed_id': 'x', 'version': 1, 'learned_from': ['a'],
                     'confidence': 0.3, 'notes': '',
                     'updated_at': 't',
                     'slots': [{'name': 'rundown_candidate', 'role': 'rundown',
                                'sample_plan': 'map_30s', 'pos_start': 0.0,
                                'pos_end': 1.0, 'pos_std': 0.05,
                                'support': 2, 'n_episodes': 2}]})
    t = FeedTemplate.from_json('x', v1)
    assert t.slots[0].label == 'content', t.slots[0]


def test_canonicalize_label():
    assert canonicalize_label('ad-like') == 'ad'
    assert canonicalize_label('sponsor_read') == 'ad'
    assert canonicalize_label('jingle') == 'boundary'
    assert canonicalize_label('content') == 'content'
    assert canonicalize_label('nonsense') == 'unknown'


# --- expert-panel regression tests ------------------------------------------

def test_learning_floor_caps_confidence():
    A, B = skeleton(3600), skeleton(3600)
    t1 = propose_template('sim', {'A': A})
    t2 = propose_template('sim', {'A': A, 'B': B})
    t3 = propose_template('sim', {'A': A, 'B': B, 'C': skeleton(3600)})
    assert t1.confidence <= 1 / 3 + 1e-3
    assert t2.confidence <= 2 / 3 + 1e-3
    assert t3.confidence > t2.confidence
    assert 'minimum 3' in t1.notes


def test_medoid_tie_break_stable_under_reorder():
    # two identical episodes: totals tie; induction must not depend on dict order
    A = skeleton(3600)
    B = skeleton(3600)
    j1 = propose_template('sim', {'A': A, 'B': B}).to_json()
    j2 = propose_template('sim', {'B': B, 'A': A}).to_json()
    import json
    d1, d2 = json.loads(j1), json.loads(j2)
    d1.pop('updated_at'); d2.pop('updated_at')
    d1.pop('learned_from'); d2.pop('learned_from')  # insertion order differs
    assert d1 == d2


def test_mixed_episode_types_learned_separately():
    full = {f'F{i}': skeleton(3600) for i in range(3)}
    # trailer-type: short, no ad slot
    trail = {f'T{i}': ep(300, [(0, 30, 'intro-music'), (30, 270, 'content'),
                               (270, 300, 'outro-music')]) for i in range(3)}
    types = {e: 'full' for e in full} | {e: 'trailer' for e in trail}
    ts = learn_templates('sim', {**full, **trail}, episode_types=types)
    assert set(ts) == {'full', 'trailer'}
    assert ts['full'].episode_type == 'full'
    assert ts['trailer'].episode_type == 'trailer'
    assert not [s for s in ts['trailer'].slots if s.label == 'ad']
    assert set(ts['full'].learned_from).isdisjoint(ts['trailer'].learned_from)


def test_rundown_not_auto_assigned():
    # intro-led show: the only content slot must keep deep_90s; the agent
    # assigns `rundown` at XIN-141 review, never the inducer.
    eps = {f'E{i}': ep(3600, [(0, 60, 'intro-music'), (60, 3300, 'content'),
                              (3300, 3600, 'outro-music')]) for i in range(3)}
    t = propose_template('sim', eps)
    content = [s for s in t.slots if s.label == 'content']
    assert len(content) == 1
    assert content[0].role == 'content', content[0]
    assert content[0].sample_plan == 'deep_90s', content[0]
    a = align(*eps['E0'], t)
    plans = plan_samples(t, a, 3600)
    assert any(p[2].startswith('deep:') for p in plans), plans


def test_align_penalizes_missing_slot():
    t = propose_template('sim', {f'E{i}': skeleton(3600) for i in range(3)})
    # held-out episode with the ad slot dropped (format change)
    no_ad = ep(3600, [(0, 60, 'intro-music'), (60, 3300, 'content'),
                      (3300, 3600, 'outro-music')])
    a = align(*no_ad, t)
    assert a.score < 1.0, a.score
    assert a.slot_coverage < 1.0, a.slot_coverage
    assert 'ad' in a.unmatched_slots, a.unmatched_slots
    assert a.seg_quality > a.score, (a.seg_quality, a.score)


def test_align_duration_weighted():
    t = propose_template('sim', {f'E{i}': skeleton(3600) for i in range(3)})
    # one 10s jingle mismatches; the 40-min content block dominates
    segs = [(0, 60, 'intro-music'), (60, 120, 'content'),
            (120, 210, 'ad-like'), (210, 3300, 'content'),
            (3300, 3310, 'silence'),  # 10s jingle where outro-music expected
            (3310, 3600, 'outro-music')]
    a = align(([(s, e, l, 0.0) for s, e, l in segs]), 3600, t)
    assert a.seg_quality > 0.95, a.seg_quality
    # unweighted mean would be dragged down by the tiny mismatch; the
    # duration-weighted score must stay high
    assert a.score > 0.9, a.score


def test_confidence_penalizes_label_disagreement():
    A = skeleton(3600)
    B = skeleton(3600)
    # C mislabels the ad slot as content
    C = ep(3600, [(0, 60, 'intro-music'), (60, 120, 'content'),
                  (120, 210, 'content'), (210, 3300, 'content'),
                  (3300, 3600, 'outro-music')])
    t = propose_template('sim', {'A': A, 'B': B, 'C': C})
    ad = next(s for s in t.slots if s.label == 'ad')
    assert ad.agreement == pytest.approx(2 / 3, abs=1e-3), ad.agreement
    assert t.confidence < 1.0, t.confidence


def test_is_drift_hysteresis():
    assert not is_drift([0.9, 0.5])
    assert not is_drift([0.5, 0.4, 0.9])      # one-off recovery: no drift
    assert is_drift([0.9, 0.5, 0.4, 0.3])      # 3 consecutive lows: drift
    assert not is_drift([0.9, 0.5, 0.4, 0.3], n_consecutive=5)  # need 5
    assert is_drift([0.1, 0.2, 0.3], threshold=0.6, n_consecutive=3)


# --- template store ----------------------------------------------------------

@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


def _feed(session):
    f = Feed(rss_url="https://example.com/feed.xml", title="Example")
    session.add(f)
    session.commit()
    session.refresh(f)
    return f.feed_id


def test_store_roundtrip(session):
    feed_id = _feed(session)
    t = propose_template('x', {f'E{i}': skeleton(3600) for i in range(3)})
    r1 = save_template(session, feed_id, t)
    assert r1.rev == 1
    loaded = load_template(session, feed_id)
    assert loaded is not None
    assert [s.label for s in loaded.slots] == [s.label for s in t.slots]
    assert loaded.labeler_version == t.labeler_version
    # second save appends rev 2; history preserved
    t2 = propose_template('x', {f'E{i}': skeleton(3600) for i in range(3)},
                          notes='agent: v2')
    r2 = save_template(session, feed_id, t2)
    assert r2.rev == 2
    hist = list_history(session, feed_id)
    assert [r.rev for r in hist] == [1, 2]
    assert load_template(session, feed_id).notes == 'agent: v2'
    # episode-type variants are independent
    assert load_template(session, feed_id, episode_type='trailer') is None


def test_store_drift_decision(session):
    feed_id = _feed(session)
    t = propose_template('x', {f'E{i}': skeleton(3600) for i in range(3)})
    rec = save_template(session, feed_id, t)
    d = record_drift_decision(session, feed_id, 'full', rec.rev,
                              'one_off', 'live crossover episode, keep v1')
    assert d.decision == 'one_off'
    assert d.template_rev == rec.rev
    with pytest.raises(ValueError):
        record_drift_decision(session, feed_id, 'full', rec.rev,
                              'bogus', 'nope')
    recs = session.query(FeedTemplateRecord).all()
    assert len(recs) == 1  # drift decision does not clobber the template


def test_align_many_segments_share_one_slot():
    # PFT case from the walking skeleton: all labels are 'content', so the
    # template is a single content slot spanning the episode; a chaptered
    # episode's 8 content blocks all fall inside it and must all match.
    t = propose_template('pft', {'E0': ([(s, e, 'content', 0.0)
        for s, e in [(0, 35), (35, 605), (605, 800), (800, 985), (985, 1165),
                     (1165, 1565), (1565, 2004)]], 2004)})
    assert len(t.slots) == 1 and t.slots[0].pos_start == 0.0
    assert t.slots[0].pos_end == 1.0
    held = ([(s, e, 'content', 0.0)
        for s, e in [(0, 130), (130, 245), (245, 921), (921, 1951),
                     (1951, 2222), (2222, 2422), (2422, 2897), (2897, 3136)]],
            3136)
    a = align(*held, t)
    assert a.score == 1.0, a
    assert a.unmatched_slots == [], a.unmatched_slots


# --- code-review (2026-09-11) regression tests --------------------------------

def test_zero_duration_episodes_do_not_crash():
    # A zero/negative-duration episode used to raise ZeroDivisionError in
    # _segments (division ran before the duration guard).
    t = propose_template('z', {
        'A': ([(0, 60, 'content', 0.0)], 0),
        'B': ([(0, 60, 'content', 0.0)], 0),
        'C': ([(0, 60, 'content', 0.0)], 0),
    })
    assert t.slots == [] and t.confidence == 0.0


def test_gap_group_slots_stay_positional():
    # Gap groups (segments the medoid lacks) are keyed by medoid-relative
    # position, but their members' actual positions can disagree with that.
    # Slots must still come out in episode order, or align() sees a shuffled
    # template and the XIN-141 reviewer reads nonsense.
    def ep(segs, dur):
        return ([(s, e, lab, 0.0) for s, e, lab in segs], dur)
    eps = {
        'E0': ep([(0, 881.3, 'ad'), (881.3, 2288.4, 'content'),
                  (2288.4, 2749.4, 'intro-music'), (2749.4, 3207.7, 'content'),
                  (3207.7, 3467.1, 'content')], 3600),
        'E1': ep([(0, 1382.7, 'intro-music'), (1382.7, 2673.8, 'content'),
                  (2673.8, 2942.2, 'content')], 3600),
        'E2': ep([(0, 269.9, 'ad'), (269.9, 1310.1, 'intro-music'),
                  (1310.1, 1920.6, 'content'), (1920.6, 2202.7, 'content')],
                 3600),
        'E3': ep([(0, 33.5, 'content'), (33.5, 219.5, 'intro-music'),
                  (219.5, 408.2, 'content')], 600),
        'E4': ep([(0, 687.4, 'ad'), (687.4, 1230.7, 'content'),
                  (1230.7, 1329.1, 'ad'), (1329.1, 1823.3, 'intro-music'),
                  (1823.3, 2264.7, 'content')], 1800),
    }
    t = propose_template('f', eps)
    starts = [s.pos_start for s in t.slots]
    assert starts == sorted(starts), [(s.name, s.pos_start) for s in t.slots]


def test_planner_never_deep_samples_skippable_label():
    # A mid-roll ad inside the content region must not get a deep_90s window
    # even though the slot it lands in says deep_90s: the segment's own label
    # policy wins for skip decisions.
    held = ([(0, 60, 'ad', 0.0), (60, 1800, 'content', 0.0),
             (1800, 1900, 'ad', 0.0), (1900, 3600, 'content', 0.0)], 3600)
    t = propose_template('g', {f'E{i}': skeleton(3600) for i in range(3)})
    a = align(*held, t)
    plans = plan_samples(t, a, 3600)
    ad_windows = [p for p in plans if 1790 <= p[0] <= 1910]
    assert all(not p[2].startswith('deep:') for p in ad_windows), plans


def test_save_template_retries_rev_collision(session, monkeypatch):
    # Two concurrent saves can read the same max rev; the loser's INSERT hits
    # uq_feed_template_rev and must retry with the next rev, not blow up.
    from sqlalchemy.exc import IntegrityError
    feed_id = _feed(session)
    t = propose_template('x', {f'E{i}': skeleton(3600) for i in range(3)})
    calls = {'n': 0}
    real_commit = Session.commit

    def flaky_commit(self):
        calls['n'] += 1
        if calls['n'] == 1:
            raise IntegrityError("duplicate key", params=None, orig=None)
        return real_commit(self)

    monkeypatch.setattr(Session, 'commit', flaky_commit)
    rec = save_template(session, feed_id, t)
    assert rec.rev == 1
    assert calls['n'] == 2
