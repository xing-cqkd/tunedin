#!/usr/bin/env python3
"""Feed template learning — deterministic template machinery (XIN-136/XIN-137).

Ported from tunedin-extract/feed_template.py with the 2026-09-11 expert-panel
fixes applied. Pure functions: no I/O, no LLM. Persistence lives in
template_store.py; the SQLAlchemy models live in
backend.persistence.models.feed_template.

Design contract (from the XIN-135 plan):
  learn:  episode section profiles -> propose_template() -> agent review
          -> template_store.save_template()
  apply:  template_store.load_template() -> align() -> plan_samples()

Everything geometric is code. The agent (XIN-141) handles semantics only:
confirming slot meanings from transcripts, naming slots, approving v1, and
deciding drift-vs-one-off. In particular, NO semantic role assignment happens
in this module — ``rundown`` is agent-assigned at review time, never inferred.

Induction is SEQUENCE alignment, not fixed-bin voting. Each learning episode
is an ordered sequence of labeled segments; learning episodes are aligned to
each other (star alignment on the medoid episode, Needleman-Wunsch) and
consensus columns become slots. Variable-length episodes no longer collapse
the vote because positions are medians, not bins.

Label vocabulary: the closed vocabulary is owned by XIN-148 (multi-signal
section labeler). CANONICAL_LABELS below is the PROVISIONAL set (acoustic
labels from the crude labeler + text-side mappings) so the aligner has one
set to rely on until XIN-148 lands.
"""
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from statistics import median, pstdev

# --- provisional closed label vocabulary (XIN-148 owns the real one) ---------
# Acoustic labels the (crude) section labeler emits today, plus the text-side
# labels XIN-148's text-cue detectors will emit, mapped into the acoustic set
# the aligner understands. Every labeler must map into this set.
TEXT_LABEL_MAP = {
    'sponsor_read': 'ad',      # host-read ad phrase, text-detected
    'promo': 'ad',             # promo/trailer segment, text-detected
    'jingle': 'boundary',      # station ID / jingle, text or chapter-derived
    'chapter_mark': 'boundary',
}
# Legacy acoustic label spellings from the crude labeler.
LEGACY_LABEL_MAP = {
    'ad-like': 'ad',
}
_LABEL_ALIASES = {**LEGACY_LABEL_MAP, **TEXT_LABEL_MAP}
CANONICAL_LABELS = frozenset({
    'ad', 'intro-music', 'outro-music', 'music-bed', 'silence',
    'content', 'unknown',
})

MIN_LEARNING_EPISODES = 3  # below this, confidence is capped, not 1.0

# label -> (role, sample_plan)
LABEL_POLICY = {
    'ad':           ('ad',        'skip'),
    'intro-music':  ('open',      'anchor'),
    'outro-music':  ('close',     'skip'),
    'music-bed':    ('interlude', 'skip'),
    'silence':      ('boundary',  'skip'),
    'content':      ('content',   'deep_90s'),
    'unknown':      ('unknown',   'map_30s'),
}

# Pairwise alignment scores. Label agreement dominates; relative-position
# proximity keeps repeated labels (e.g. several 'content' blocks) matched to
# the right counterpart; gaps are cheaper than label mismatches so insertions
# (one episode's extra ad) align to gaps instead of polluting a column.
_S_MATCH = 2.0
_S_MISMATCH = -2.0
_S_GAP = -1.5
_S_POS_W = 3.0

# align() score = _W_SEG * seg_quality + _W_COV * slot_coverage
_W_SEG = 0.7
_W_COV = 0.3


@dataclass
class Slot:
    name: str
    label: str           # consensus section label, e.g. 'ad'
    role: str            # ad|open|rundown|content|close|boundary|interlude|unknown
    sample_plan: str     # skip|anchor|map_30s|deep_90s
    pos_start: float     # median relative position 0..1
    pos_end: float
    pos_std: float        # stdev of member midpoints
    support: int         # learning episodes where this slot appeared
    n_episodes: int
    agreement: float = 1.0  # fraction of members carrying the consensus label


@dataclass
class FeedTemplate:
    feed_id: str
    episode_type: str = 'full'   # episode-type variant (XIN-134 classifier)
    version: int = 2
    slots: list = field(default_factory=list)
    learned_from: list = field(default_factory=list)  # episode_ids
    labeler_version: str = 'crude-0'  # which labeler produced the labels
    confidence: float = 0.0
    notes: str = ''      # agent-written semantics, e.g. "rundown names guests"
    updated_at: str = ''

    def to_json(self):
        return json.dumps(asdict(self), indent=1)

    @staticmethod
    def from_json(feed_id, raw):
        d = json.loads(raw)
        slots = []
        for s in d.get('slots', []):
            if 'label' not in s:
                s = dict(s, label=_label_from_name(s['name']))
            slots.append(Slot(**{k: v for k, v in s.items()
                                 if k in Slot.__dataclass_fields__}))
        return FeedTemplate(
            feed_id=feed_id,
            episode_type=d.get('episode_type', 'full'),
            version=d.get('version', 2),
            slots=slots,
            learned_from=d.get('learned_from', []),
            labeler_version=d.get('labeler_version', 'crude-0'),
            confidence=d.get('confidence', 0.0),
            notes=d.get('notes', ''),
            updated_at=d.get('updated_at', ''),
        )


@dataclass
class Alignment:
    """Result of aligning one episode's sections to a template."""
    segments: list  # [(start_s, end_s, label, slot_name, quality)]
    score: float         # composite: _W_SEG*seg_quality + _W_COV*slot_coverage
    seg_quality: float   # duration-weighted mean segment match quality
    slot_coverage: float  # fraction of template slots with >=1 aligned segment
    unmatched_slots: list  # slot names no segment aligned to (drift signal)


def _label_from_name(name):
    """Recover a consensus label for v1 templates (which stored name only)."""
    if name == 'rundown_candidate' or name.startswith('rundown_candidate_'):
        return 'content'
    return name.replace('_', '-').rstrip('-0123456789')


def canonicalize_label(label):
    """Map any labeler output into the provisional closed vocabulary."""
    if label in CANONICAL_LABELS:
        return label
    return _LABEL_ALIASES.get(label, 'unknown')


def _segments(episodes_sections):
    """episode_id -> ordered [(label, rel_start, rel_end)]."""
    out = {}
    for eid, (sections, duration) in episodes_sections.items():
        seq = []
        if duration > 0:
            for s, e, lab, _ in sections:
                rs, re = s / duration, e / duration
                if re > rs:
                    seq.append((canonicalize_label(lab), rs, re))
        out[eid] = seq
    return out


def _pair_score(a, b):
    """Alignment score for segment a=(label,rs,re) against b=(label,rs,re)."""
    (la, rsa, rea), (lb, rsb, reb) = a, b
    pos_pen = _S_POS_W * abs((rsa + rea) / 2 - (rsb + reb) / 2)
    if la == lb:
        da, db = rea - rsa, reb - rsb
        dur_sim = 1.0 - abs(da - db) / max(da, db) if max(da, db) > 0 else 0.0
        return _S_MATCH + dur_sim - pos_pen
    return _S_MISMATCH - pos_pen


def _align_pair(A, B):
    """Global (Needleman-Wunsch) alignment of two segment sequences.

    Returns [(i|None, j|None)] in order. Traceback is deterministic:
    on ties prefer diagonal, then gap-in-B, then gap-in-A.
    """
    n, m = len(A), len(B)
    S = [[0.0] * (m + 1) for _ in range(n + 1)]
    for i in range(1, n + 1):
        S[i][0] = i * _S_GAP
    for j in range(1, m + 1):
        S[0][j] = j * _S_GAP
    for i in range(1, n + 1):
        Ai = A[i - 1]
        Si, Si1 = S[i], S[i - 1]
        for j in range(1, m + 1):
            Si[j] = max(Si1[j - 1] + _pair_score(Ai, B[j - 1]),
                        Si1[j] + _S_GAP,
                        Si[j - 1] + _S_GAP)
    i, j = n, m
    aln = []
    while i > 0 or j > 0:
        move, best = None, None
        if i > 0 and j > 0:
            move, best = 'd', S[i - 1][j - 1] + _pair_score(A[i - 1], B[j - 1])
        if i > 0:
            v = S[i - 1][j] + _S_GAP
            if best is None or v > best:
                move, best = 'u', v
        if j > 0:
            v = S[i][j - 1] + _S_GAP
            if best is None or v > best:
                move, best = 'l', v
        if move == 'd':
            aln.append((i - 1, j - 1)); i -= 1; j -= 1
        elif move == 'u':
            aln.append((i - 1, None)); i -= 1
        else:
            aln.append((None, j - 1)); j -= 1
    aln.reverse()
    return aln


def _alignment_score(A, B):
    """Total pairwise score (for medoid selection)."""
    n, m = len(A), len(B)
    S = [[0.0] * (m + 1) for _ in range(n + 1)]
    for i in range(1, n + 1):
        S[i][0] = i * _S_GAP
    for j in range(1, m + 1):
        S[0][j] = j * _S_GAP
    for i in range(1, n + 1):
        Ai = A[i - 1]
        Si, Si1 = S[i], S[i - 1]
        for j in range(1, m + 1):
            Si[j] = max(Si1[j - 1] + _pair_score(Ai, B[j - 1]),
                        Si1[j] + _S_GAP,
                        Si[j - 1] + _S_GAP)
    return S[n][m]


def _majority_label(members, ref_label):
    counts = {}
    for _, lab, _, _ in members:
        counts[lab] = counts.get(lab, 0) + 1
    top = max(counts.values())
    cands = sorted(l for l, c in counts.items() if c == top)
    return ref_label if ref_label in cands else cands[0]


def _nepisodes(members):
    return len({eid for eid, _, _, _ in members})


def propose_template(feed_id, episodes_sections, episode_type='full',
                     labeler_version='crude-0', notes=''):
    """Deterministic slot induction via sequence alignment.

    episodes_sections: {episode_id: (sections, duration)} with sections as
    (start_s, end_s, label, density) tuples.
    """
    eps = list(episodes_sections.keys())
    n = len(eps)
    seqs = _segments(episodes_sections)
    need = n // 2 + 1  # strict majority of learning episodes

    def _blank(extra):
        return FeedTemplate(
            feed_id=feed_id, episode_type=episode_type,
            labeler_version=labeler_version, slots=[], learned_from=eps,
            confidence=0.0,
            notes=((notes + ' ' if notes else '') + extra),
            updated_at=datetime.now(timezone.utc).isoformat())

    auto = ''
    if n == 0 or all(len(s) == 0 for s in seqs.values()):
        return _blank('[auto] no usable sections in learning episodes.')

    # medoid = episode with the best total pairwise alignment score.
    # Tie-break on episode_id (not input order): induction must be
    # order-independent for the same episode set.
    totals = {e: sum(_alignment_score(seqs[e], seqs[o]) for o in eps if o != e)
              for e in eps}
    ref = max(eps, key=lambda e: (totals[e], e))
    ref_seq = seqs[ref]

    # star alignment: every other episode against the medoid
    aligns = {e: _align_pair(ref_seq, seqs[e]) for e in eps if e != ref}

    # consensus columns, one per medoid segment; members are (eid, lab, rs, re)
    col_members = {k: [(ref,) + ref_seq[k]] for k in range(len(ref_seq))}
    # gap-groups: unaligned segments keyed by (ref position, label)
    gap_groups = {}
    for e, aln in aligns.items():
        pos = 0  # medoid segments consumed so far
        for ri, ej in aln:
            if ri is not None:
                pos = ri + 1
                if ej is not None:
                    col_members[ri].append((e,) + seqs[e][ej])
            else:
                lab = seqs[e][ej][0]
                gap_groups.setdefault((pos, lab), []).append((e,) + seqs[e][ej])

    order = []  # (label, members), in medoid order
    for k in range(len(ref_seq)):
        members = col_members[k]
        if _nepisodes(members) < need:
            continue
        lab = _majority_label(members, ref_seq[k][0])
        order.append((lab, members))
    for (pos, lab), members in gap_groups.items():
        if _nepisodes(members) < need:
            continue
        # gap groups are label-homogeneous by key construction; pass the key
        # label as the reference (previously passed an episode id — inert but
        # wrong).
        order.append((_majority_label(members, lab), members))
    # Positional order before merging: gap groups are keyed by medoid-relative
    # position (pos-0.5), but their members' actual relative positions can
    # disagree with that — episodes vary in timing, and a surviving gap group
    # means the data did exactly that. Slots are regions in episode order, so
    # order the columns by actual span and merge same-label runs there.
    col_slots = []  # [(label, Slot, members)]
    for lab, members in order:
        col_slots.append((lab, _make_slot(lab, members, n), members))
    col_slots.sort(key=lambda item: (item[1].pos_start, item[1].pos_end))
    slots = []
    i = 0
    while i < len(col_slots):
        lab = col_slots[i][0]
        j = i
        while j + 1 < len(col_slots) and col_slots[j + 1][0] == lab:
            j += 1
        members = [m for k in range(i, j + 1) for m in col_slots[k][2]]
        s = _make_slot(lab, members, n)
        # The merged slot spans the union of the run: it starts where the
        # run's first column starts and ends where the last column ends
        # (median-of-starts would collapse a merged run toward the episode
        # middle). min/max keeps the union exact when spans overlap.
        s.pos_start = min(col_slots[k][1].pos_start for k in range(i, j + 1))
        s.pos_end = max(col_slots[k][1].pos_end for k in range(i, j + 1))
        slots.append(s)
        i = j + 1

    # name de-dup
    seen = {}
    for s in slots:
        seen[s.name] = seen.get(s.name, 0) + 1
        if seen[s.name] > 1:
            s.name = f"{s.name}_{seen[s.name]}"

    # NOTE (expert panel): no rundown heuristic here. Assigning `rundown` is a
    # semantic judgment and belongs to the agent at XIN-141 review. The old
    # auto-rename silently downgraded deep_90s -> map_30s on the main content
    # slot of intro-led shows.

    support_ratio = (sum(s.support for s in slots) / max(len(slots), 1)
                     / max(n, 1) if slots else 0.0)
    agreement = (sum(s.agreement for s in slots) / max(len(slots), 1)
                 if slots else 0.0)
    pos_factor = (sum(max(0.0, 1.0 - s.pos_std / 0.5) for s in slots)
                  / max(len(slots), 1) if slots else 0.0)
    conf = support_ratio * agreement * pos_factor

    labels_seen = {lab for seq in seqs.values() for lab, _, _ in seq}
    if not slots:
        auto = ('[auto] no consensus columns reached majority support: learning '
                'episodes share no stable segment sequence.')
        conf = 0.0
    elif len(labels_seen) == 1:
        # Honest single slot: the labels carry no boundary information at all.
        auto = ('[auto] all sections share one label '
                f"('{next(iter(labels_seen))}'): labels carry no boundary "
                'information to induce slots; needs informative labels (real '
                'speech/music/ad classifiers or text segmentation) before this '
                'template is trustworthy.')
        conf *= 0.3
    elif len(slots) == 1 and max(s.pos_end - s.pos_start for s in slots) > 0.8:
        auto = ('[auto] diverse section labels but no stable cross-episode '
                'sequence found; single fallback slot.')
        conf *= 0.3
    if n < MIN_LEARNING_EPISODES:
        # A cold-start template from 1-2 episodes is a guess, not knowledge.
        auto += (f' [auto] only {n} learning episode(s) '
                 f'(minimum {MIN_LEARNING_EPISODES}): confidence capped, '
                 'not production-ready.')
        conf *= n / MIN_LEARNING_EPISODES
    if auto:
        notes = (notes + ' ' if notes else '') + auto.strip()
    return FeedTemplate(feed_id=feed_id, episode_type=episode_type,
                        labeler_version=labeler_version, slots=slots,
                        learned_from=eps, confidence=round(conf, 3), notes=notes,
                        updated_at=datetime.now(timezone.utc).isoformat())


def _make_slot(lab, members, n):
    role, plan = LABEL_POLICY.get(lab, ('unknown', 'map_30s'))
    counts = {}
    for _, l, _, _ in members:
        counts[l] = counts.get(l, 0) + 1
    agreement = counts.get(lab, 0) / max(len(members), 1)
    rs = [s for _, _, s, _ in members]
    re_ = [e for _, _, _, e in members]
    mids = [(s + e) / 2 for _, _, s, e in members]
    support = len({eid for eid, _, _, _ in members})
    return Slot(name=lab.replace('-', '_'), label=lab, role=role,
                sample_plan=plan,
                pos_start=round(float(median(rs)), 3),
                pos_end=round(float(median(re_)), 3),
                pos_std=round(float(pstdev(mids)) if len(mids) > 1 else 0.0, 3),
                support=support, n_episodes=n, agreement=round(agreement, 3))


# A segment belongs to the slot whose span contains its midpoint, with this
# tolerance at span edges (slots partition [0,1] approximately; medians leave
# small gaps).
_ALIGN_TOL = 0.05


def align(sections, duration, template):
    """Align a new episode's segment sequence to the template's slot spans.

    Each segment is assigned to the slot whose span contains its midpoint —
    many segments may share one slot, because slots are *regions*, not
    segments (a chaptered episode can have eight content blocks inside one
    content slot). Assignment prefers label match, then positional distance;
    a segment farther than _ALIGN_TOL outside every slot span is 'unmatched'.

    Returns an Alignment. score blends duration-weighted segment match quality
    with slot coverage: template slots that NO segment aligns to are penalized,
    so a format change that *drops* structure (ads removed, segments cut)
    lowers the score instead of scoring a perfect 1.0. Persistent low score
    is the drift signal (see is_drift); unmatched_slots names the dropped
    structure for the XIN-141 agent review.
    """
    out = []
    matched_slots = set()
    total_dur = 0.0
    weighted_q = 0.0
    for s, e, lab, _ in sections:
        if duration <= 0 or e <= s:
            continue
        rs, re_ = s / duration, e / duration
        mid = (rs + re_) / 2
        lab = canonicalize_label(lab)
        dur = (re_ - rs) * duration
        total_dur += dur
        best, best_key = None, None
        for j, sl in enumerate(template.slots):
            if mid < sl.pos_start:
                dist = sl.pos_start - mid
            elif mid > sl.pos_end:
                dist = mid - sl.pos_end
            else:
                dist = 0.0
            key = (0 if sl.label == lab else 1, dist)
            if best_key is None or key < best_key:
                best, best_key = j, key
        if best is None or best_key[1] > _ALIGN_TOL:
            out.append((round(s, 1), round(e, 1), lab, 'unmatched', 0.0))
            continue
        sl = template.slots[best]
        matched_slots.add(best)
        q = 1.0 if sl.label == lab else 0.5
        weighted_q += q * dur
        out.append((round(s, 1), round(e, 1), lab, sl.name, q))
    seg_quality = weighted_q / total_dur if total_dur > 0 else 0.0
    n_slots = len(template.slots)
    slot_coverage = len(matched_slots) / n_slots if n_slots else 1.0
    unmatched_slots = [template.slots[j].name for j in range(n_slots)
                       if j not in matched_slots]
    score = _W_SEG * seg_quality + _W_COV * slot_coverage
    return Alignment(segments=out, score=round(score, 3),
                     seg_quality=round(seg_quality, 3),
                     slot_coverage=round(slot_coverage, 3),
                     unmatched_slots=unmatched_slots)


def is_drift(scores, threshold=0.6, n_consecutive=3):
    """Hysteresis rule for the XIN-141 drift signal: True when the last
    n_consecutive alignment scores all fall below threshold. A single bad
    episode (one-off) never trips it."""
    if len(scores) < n_consecutive:
        return False
    return all(s < threshold for s in scores[-n_consecutive:])


def plan_samples(template, alignment, duration):
    """Decide transcription windows from template roles.

    Returns list of (start_s, end_s, purpose). Skips ad/close/boundary slots,
    maps rundown slots with 30s, goes deep (90s) on the two longest content
    slots.

    NOTE (expert panel): "two longest content slots" is a placeholder pending
    the textual substance gate (XIN-140): 30s probes -> score substance from
    words -> deep windows on top scorers. The adaptive-sampling experiment
    disproved duration as a density proxy. Also, the `anchor` plan is currently
    skipped — XIN-138 must define its output contract into align() first.
    """
    plans = []
    content_cands = []
    for st, en, lab, slot_name, q in alignment.segments:
        slot = next((s for s in template.slots if s.name == slot_name), None)
        plan = slot.sample_plan if slot else 'map_30s'
        seg_plan = LABEL_POLICY.get(lab, ('unknown', 'map_30s'))[1]
        # A segment the labeler calls skippable (ad, outro-music, music-bed,
        # silence) is never deep-sampled, even when it lands inside a
        # non-skip slot span (e.g. a mid-roll ad inside the content region).
        if plan == 'skip' or plan == 'anchor' or seg_plan == 'skip':
            continue
        if plan == 'map_30s':
            c = (st + en) / 2
            w = 30
            plans.append((round(max(st, c - w / 2), 1),
                          round(min(en, c + w / 2), 1), f'map:{slot_name}'))
        elif plan == 'deep_90s':
            content_cands.append((en - st, st, en, slot_name))
    content_cands.sort(reverse=True)
    for _, st, en, slot_name in content_cands[:2]:
        c = (st + en) / 2
        w = 90
        plans.append((round(max(st, c - w / 2), 1),
                      round(min(en, c + w / 2), 1), f'deep:{slot_name}'))
    return sorted(plans)


def learn_templates(feed_id, episodes_sections, episode_types=None,
                    labeler_version='crude-0', notes=''):
    """Learn one template per episode type (XIN-134 classifier output).

    episode_types: {episode_id: episode_type}; missing entries default to
    'full'. Learning NEVER mixes episode types — a banter-first "Hour 2"
    must not pollute the solo template. Returns {episode_type: FeedTemplate}.
    """
    episode_types = episode_types or {}
    groups = {}
    for eid in episodes_sections:
        et = episode_types.get(eid, 'full')
        groups.setdefault(et, {})[eid] = episodes_sections[eid]
    return {et: propose_template(feed_id, eps, episode_type=et,
                                 labeler_version=labeler_version, notes=notes)
            for et, eps in groups.items()}
