"""XIN-143 Tier 2: insight-yield bake-off (product gate).

Five strategies run head-to-head on a fixed set of ~20-30 episodes across
feeds. The metric is insight yield per transcription-minute:

    text_only        title/summary/shownotes -> insights (0 audio minutes)
    first_4min       transcribe [0, 240s] -> insights
    uniform_2x90s    transcribe two 90s windows at 1/3 and 2/3 -> insights
    template_guided  XIN-144: template -> align -> plan_samples -> insights
    full             transcribe the whole episode -> insights

Grading rubric (applied by hand, one rater per episode for all five
strategies — paired design, so rater variance cancels in the comparison):
    usable_quotes:          verbatim voice quotes worth keeping (count)
    timestamped_takeaways:  takeaways with verified or corrected timestamps
                            (count; text-only takeaways score here only if
                            a timestamp could be verified against audio)
    tone_mismatch:          0/1 — did the strategy catch a tone mismatch the
                            text-only pass missed?
    ad_skip_precision:      computed, not judged — fraction of actual
                            ad-labeled seconds (chapter ground truth) the
                            strategy avoided transcribing.

U(strategy) = usable_quotes + timestamped_takeaways  ("useful insights")
C(strategy) = transcription minutes.

Production-ready bar (TIER2_PASS), defined here per the issue:
    Bar A (beats text-only):   U(template_guided) >= 1.5 * U(text_only)
    Bar B (near full yield at ~10% cost):
        U(tg)/C(tg) >= 0.7 * U(full)/C(full)  and  C(tg) <= 0.15 * C(full)

Text-only uses zero transcription minutes, so its yield-per-minute is
undefined — Bar A compares absolute useful-insight counts instead.

Tier 2 passing is the production-ready gate, and it gates XIN-144.
A Tier 1 pass with a Tier 2 fail means perfect machinery placing windows
on energetic-but-empty promos: the templates would be geometrically right
and productively useless.
"""
from dataclasses import dataclass, field

STRATEGIES = ("text_only", "first_4min", "uniform_2x90s", "template_guided",
              "full")

# Production-ready bar, Tier 2.
TIER2_PASS = {
    "beat_text_margin": 1.5,  # Bar A: U(tg) >= 1.5 * U(text_only)
    "yield_ratio": 0.7,       # Bar B: yield/min(tg) >= 0.7 * yield/min(full)
    "cost_ratio": 0.15,       # Bar B: C(tg) <= 0.15 * C(full)
}


def plan_strategy_windows(strategy: str, *, duration: float,
                          template=None, sections=None) -> list[tuple]:
    """Transcription windows for one strategy.

    Returns [(start_s, end_s, purpose)]. text_only returns [] (no audio).
    template_guided needs the feed template and the episode's sections.
    """
    if strategy == "text_only":
        return []
    if strategy == "first_4min":
        end = min(240.0, duration)
        return [(0.0, round(end, 1), "first_4min")] if end > 0 else []
    if strategy == "uniform_2x90s":
        wins = []
        for frac in (1 / 3, 2 / 3):
            c = duration * frac
            s = max(0.0, min(c - 45.0, duration - 90.0))
            e = min(s + 90.0, duration)
            if e > s and (round(s, 1), round(e, 1)) not in \
                    [(w[0], w[1]) for w in wins]:
                wins.append((round(s, 1), round(e, 1), "uniform_2x90s"))
        return wins
    if strategy == "template_guided":
        if template is None or sections is None:
            raise ValueError("template_guided needs template and sections")
        # XIN-144's window planner; tier2 never reimplements it.
        from backend.insights.template_guided import plan_guided_windows
        return plan_guided_windows(template, sections, duration)
    if strategy == "full":
        return [(0.0, round(duration, 1), "full")] if duration > 0 else []
    raise ValueError(f"unknown strategy: {strategy}")


@dataclass
class GradeCard:
    """One rater's grades for one episode x one strategy."""
    episode_id: str
    feed_id: str
    strategy: str
    transcription_minutes: float
    usable_quotes: int = 0
    timestamped_takeaways: int = 0
    ad_skip_precision: float | None = None
    tone_mismatch: int = 0
    notes: str = ""

    @property
    def useful(self) -> int:
        return self.usable_quotes + self.timestamped_takeaways


@dataclass
class StrategyTotals:
    strategy: str
    n_episodes: int = 0
    useful: int = 0
    transcription_minutes: float = 0.0
    tone_mismatches: int = 0

    @property
    def yield_per_min(self) -> float | None:
        if self.transcription_minutes <= 0:
            return None
        return round(self.useful / self.transcription_minutes, 3)


@dataclass
class Tier2Report:
    n_episodes: int
    totals: dict = field(default_factory=dict)  # strategy -> StrategyTotals
    bar_a_ratio: float | None = None   # U(tg) / U(text_only)
    bar_b_yield_ratio: float | None = None  # yield/min(tg) / yield/min(full)
    bar_b_cost_ratio: float | None = None   # C(tg) / C(full)
    passed: bool = False
    notes: str = ""

    def total(self, strategy: str) -> StrategyTotals | None:
        return self.totals.get(strategy)


def summarize_tier2(cards: list[GradeCard]) -> Tier2Report:
    """Aggregate grade cards into the Tier 2 verdict."""
    rep = Tier2Report(n_episodes=len({c.episode_id for c in cards}))
    for c in cards:
        t = rep.totals.setdefault(c.strategy, StrategyTotals(c.strategy))
        t.n_episodes += 1
        t.useful += c.useful
        t.transcription_minutes += c.transcription_minutes
        t.tone_mismatches += c.tone_mismatch
    for t in rep.totals.values():
        t.transcription_minutes = round(t.transcription_minutes, 2)

    tg = rep.total("template_guided")
    tx = rep.total("text_only")
    full = rep.total("full")
    if tg is None or tx is None or full is None:
        rep.notes = "missing strategies: need text_only, template_guided, full."
        return rep

    # Bar A: beat text-only by 1.5x. The denominator floors at 1: when the
    # text baseline yields nothing, the ratio would be inf/undefined — a
    # single lucky guided quote must not pass. Flooring means guided needs
    # >= 2 useful insights to beat a degenerate baseline.
    denom = max(tx.useful, 1)
    rep.bar_a_ratio = round(tg.useful / denom, 3)
    y_tg, y_full = tg.yield_per_min, full.yield_per_min
    rep.bar_b_yield_ratio = (round(y_tg / y_full, 3)
                             if y_tg is not None and y_full else None)
    rep.bar_b_cost_ratio = (round(tg.transcription_minutes /
                                  full.transcription_minutes, 3)
                            if full.transcription_minutes else None)

    bar_a = (rep.bar_a_ratio is not None
             and rep.bar_a_ratio >= TIER2_PASS["beat_text_margin"])
    bar_b = (rep.bar_b_yield_ratio is not None
             and rep.bar_b_yield_ratio >= TIER2_PASS["yield_ratio"]
             and rep.bar_b_cost_ratio is not None
             and rep.bar_b_cost_ratio <= TIER2_PASS["cost_ratio"])
    rep.passed = bar_a and bar_b
    if not rep.passed:
        failed = []
        if not bar_a:
            failed.append(
                f"Bar A: U(tg)/U(text)={rep.bar_a_ratio} "
                f"< {TIER2_PASS['beat_text_margin']}")
        if not bar_b:
            failed.append(
                f"Bar B: yield-ratio={rep.bar_b_yield_ratio} "
                f"(need >= {TIER2_PASS['yield_ratio']}), "
                f"cost-ratio={rep.bar_b_cost_ratio} "
                f"(need <= {TIER2_PASS['cost_ratio']})")
        rep.notes = "below production-ready bar: " + "; ".join(failed)
    return rep
