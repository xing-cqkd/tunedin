"""Check that two database backends hold the same data (Linear: XIN-95).

Usage:
    python -m backend.parity_check --source simple --target dynamodb [--sample 25]

Compares, per table: row counts, feed counts by ``sync_status``, episode
unprocessed counts, and payload spot-checks on a deterministic sample of
rows (normalized so UUID/datetime representations compare equal across
backends -- including the DynamoDB codec's ``""`` <-> ``None`` equivalence
contract), plus missing-table (source-only) and target-only table
detection. Exit code 0 when everything matches, 1 on any mismatch, 2 when
source and target resolve to the same database.

Sampling note: the payload spot-check sample is a deterministic
*hash-spread* sample -- rows are ordered by sha256 of their primary key
and the first ``--sample`` taken -- so the sample covers the whole key
range rather than just the head of pk order. This is still a spot-check,
not a proof: a targeted single-row corruption outside the sampled rows
will not be seen. Raise ``--sample`` for wider coverage on large tables.
Missing/extra rows, by contrast, are always computed over the FULL pk
sets, so no row is ever silently dropped from the existence check.

Backends resolve exactly like ``backend.migrate_data`` (same env vars), so
``--target dynamodb`` against real AWS needs the standard AWS credential
chain; tests inject a moto-backed backend directly into :func:`compare`
instead.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple
from uuid import UUID

from backend.migrate_data import (
    Backend,
    SameBackendError,
    assert_distinct_backends,
    get_backend,
    reconcile_tables,
)
from backend.persistence.models import Base

UTC = timezone.utc


def _norm(value: Any) -> Any:
    """Normalize one column value for cross-backend comparison."""
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            # SQLite returns naive datetimes; the DynamoDB codec normalizes
            # naive to UTC on write, so assume UTC here too.
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat()
    if isinstance(value, str):
        # Mirror the DynamoDB codec's "" <-> None equivalence contract
        # (keys.sanitize_str maps "" -> attribute-omitted -> None on read):
        # a SQL row with title="" migrates to title=None BY DESIGN, so the
        # two must compare equal (XIN-131).
        if value == "":
            return None
        return value
    if isinstance(value, (dict, list)):
        return _norm_container(value)
    return value


def _norm_container(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _norm(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    return [_norm(v) for v in value]


def _norm_row(row: Dict[str, Any]) -> Tuple[Tuple[str, Any], ...]:
    return tuple(sorted(((k, _norm(v)) for k, v in row.items()), key=lambda kv: kv[0]))


def _pk_columns(table_name: str) -> Tuple[str, ...]:
    table = Base.metadata.tables[table_name]
    return tuple(c.name for c in table.primary_key.columns)


def _sort_key(row: Dict[str, Any], pk: Tuple[str, ...]) -> Tuple:
    return tuple(str(_norm(row.get(c))) for c in pk)


@dataclass
class TableParity:
    table: str
    source_rows: int
    target_rows: int
    # Only populated for feeds / episodes.
    source_by_status: Dict[str, int] = field(default_factory=dict)
    target_by_status: Dict[str, int] = field(default_factory=dict)
    source_unprocessed: int = 0
    target_unprocessed: int = 0
    # (kind, detail) mismatches found in the payload sample.
    sample_mismatches: List[Tuple[str, str]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return (
            self.source_rows == self.target_rows
            and self.source_by_status == self.target_by_status
            and self.source_unprocessed == self.target_unprocessed
            and not self.sample_mismatches
        )


@dataclass
class ParityReport:
    source: str
    target: str
    tables: List[TableParity] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(t.ok for t in self.tables)


async def compare(
    source: Backend, target: Backend, *, sample_size: int = 25
) -> ParityReport:
    """Compare two backends table by table. Does not close either backend.

    Memory characteristic: each backend's ``read_table`` loads whole tables
    into memory (the same pattern the migration CLI uses), so on a
    production-scale catalog this needs RAM proportional to the largest
    table. Sampling (``sample_size``) only bounds the payload diff work,
    not the rows held in memory.
    """
    report = ParityReport(source=source.name, target=target.name)
    source_tables = list(source.table_names)
    target_tables = list(target.table_names)
    common, _source_only, target_only = reconcile_tables(source_tables, target_tables)
    common_set = set(common)
    for table_name in source_tables:
        if table_name not in common_set:
            tp = TableParity(table=table_name, source_rows=-1, target_rows=-1)
            tp.sample_mismatches.append(
                ("missing-table", f"table {table_name!r} absent from target backend")
            )
            report.tables.append(tp)
            continue
        s_rows = await source.read_table(table_name)
        t_rows = await target.read_table(table_name)
        tp = TableParity(
            table=table_name, source_rows=len(s_rows), target_rows=len(t_rows)
        )
        if table_name == "feeds":
            tp.source_by_status = _count_by(s_rows, "sync_status")
            tp.target_by_status = _count_by(t_rows, "sync_status")
        if table_name == "episodes":
            tp.source_unprocessed = sum(1 for r in s_rows if not r.get("processed"))
            tp.target_unprocessed = sum(1 for r in t_rows if not r.get("processed"))
        tp.sample_mismatches = _sample_diff(table_name, s_rows, t_rows, sample_size)
        report.tables.append(tp)
    # Target-only tables: a stale/extra table in the target (e.g. left over
    # from an earlier partial backfill) is invisible when iterating only
    # source tables, so flag it explicitly. Only the count is needed --
    # a count-only read avoids materializing a table we are not diffing.
    for table_name in target_only:
        n = await target.count_rows(table_name)
        tp = TableParity(
            table=table_name, source_rows=-1, target_rows=n
        )
        tp.sample_mismatches.append(
            (
                "target-only-table",
                f"table {table_name!r} present in target ({n} rows) "
                f"but absent from source — possible leftover from a partial backfill",
            )
        )
        report.tables.append(tp)
    return report


def _count_by(rows: List[Dict[str, Any]], column: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows:
        key = str(row.get(column))
        counts[key] = counts.get(key, 0) + 1
    return counts


def _hash_spread_sample(
    rows: List[Dict[str, Any]], pk: Tuple[str, ...], sample_size: int
) -> List[Dict[str, Any]]:
    """Deterministic sample spread across the whole key space.

    Rows are ordered by sha256 of their (normalized) primary key and the
    first ``sample_size`` are taken. Head-of-pk-order sampling could never
    see corruption confined to a high pk range (e.g. a partial backfill
    overwriting a pk-range slice); hash order spreads the sample across
    the full range instead.
    """

    def _digest(row: Dict[str, Any]) -> bytes:
        key = "\x00".join(_sort_key(row, pk))
        return hashlib.sha256(key.encode("utf-8")).digest()

    return sorted(rows, key=_digest)[:sample_size]


def _sample_diff(
    table_name: str,
    s_rows: List[Dict[str, Any]],
    t_rows: List[Dict[str, Any]],
    sample_size: int,
) -> List[Tuple[str, str]]:
    """Diff rows; return (kind, detail) mismatches.

    Missing/extra rows are computed over the FULL pk sets -- truncating to
    a head window before set-diffing mislabeled rows when counts differed
    (window-shift "extra-row", XIN-131). The 25-row sample is used solely
    for payload diffs, drawn as a deterministic hash-spread sample.
    """
    pk = _pk_columns(table_name)
    s_map = {_sort_key(r, pk): _norm_row(r) for r in s_rows}
    t_map = {_sort_key(r, pk): _norm_row(r) for r in t_rows}
    mismatches: List[Tuple[str, str]] = []
    for key in s_map:
        if key not in t_map:
            mismatches.append(
                ("missing-row", f"{table_name} row pk={key} present in source, absent in target")
            )
    for key in t_map:
        if key not in s_map:
            mismatches.append(
                ("extra-row", f"{table_name} row pk={key} absent in source, present in target")
            )
    if sample_size > 0:
        for row in _hash_spread_sample(s_rows, pk, sample_size):
            key = _sort_key(row, pk)
            if key in t_map and s_map[key] != t_map[key]:
                s_dict, t_dict = dict(s_map[key]), dict(t_map[key])
                differing = sorted(
                    k
                    for k in set(s_dict) | set(t_dict)
                    if s_dict.get(k) != t_dict.get(k)
                )
                mismatches.append(
                    (
                        "payload-diff",
                        f"{table_name} row pk={key} differs in columns: {', '.join(differing)}",
                    )
                )
    return mismatches


def print_report(report: ParityReport) -> None:
    print(f"parity: {report.source} -> {report.target}")
    print(f"{'table':<28}{'source':>10}{'target':>10}{'verdict':>12}")
    print("-" * 62)
    for t in report.tables:
        verdict = "OK" if t.ok else "MISMATCH"
        s = str(t.source_rows) if t.source_rows >= 0 else "n/a"
        g = str(t.target_rows) if t.target_rows >= 0 else "n/a"
        print(f"{t.table:<28}{s:>10}{g:>10}{verdict:>12}")
        if t.source_by_status or t.target_by_status:
            print(f"    by_status src={t.source_by_status} tgt={t.target_by_status}")
        if t.table == "episodes":
            print(
                f"    unprocessed src={t.source_unprocessed} tgt={t.target_unprocessed}"
            )
        for kind, detail in t.sample_mismatches[:10]:
            print(f"    [{kind}] {detail}")
        if len(t.sample_mismatches) > 10:
            print(f"    ... and {len(t.sample_mismatches) - 10} more")
    print("-" * 62)
    print("PARITY OK" if report.ok else "PARITY MISMATCH")


async def _run(source_name: str, target_name: str, sample_size: int) -> int:
    source = get_backend(source_name)
    target = get_backend(target_name)
    try:
        try:
            # Same refusal as migrate(): never compare a database with itself.
            assert_distinct_backends(source, target)
        except SameBackendError as e:
            print(f"error: {e}")
            return 2
        print(f"{source.identity} -> {target.identity}")
        report = await compare(source, target, sample_size=sample_size)
    finally:
        await source.close()
        await target.close()
    print_report(report)
    return 0 if report.ok else 1


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check that two database backends hold the same data."
    )
    parser.add_argument("--source", required=True, choices=("simple", "app", "dynamodb"))
    parser.add_argument("--target", required=True, choices=("simple", "app", "dynamodb"))
    parser.add_argument(
        "--sample",
        type=int,
        default=25,
        help="Rows sampled per table for payload spot-checks (default: 25).",
    )
    args = parser.parse_args(argv)
    return asyncio.run(_run(args.source, args.target, args.sample))


if __name__ == "__main__":
    raise SystemExit(main())
