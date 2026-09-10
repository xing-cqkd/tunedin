# DynamoDB backfill runbook (SQLite → DynamoDB, free-tier)

Operational knowledge for the one-time bulk migration of the TuneIn corpus
from `simple.db` (SQLite) into the `tunedin` DynamoDB table (`us-east-1`).
This documents what was learned running it, so the same mistakes are not
repeated. For cutover procedure and parity checks, see
`docs/dynamodb-cutover.md`. (The *insights extraction* backfill is a
separate thing; see `backend/insights/BACKFILL.md`.)

## Corpus and target

- Source: `simple.db`, ~2.2 GB SQLite. 1,053 feeds, 556,517 episodes,
  average ~2.8 KB text payload per episode (max measured ~197 KB).
  Transcripts are 100% empty in this corpus.
- Target: DynamoDB table `tunedin`, `us-east-1`, provisioned mode,
  exactly at the always-free tier: 12 RCU / 12 WCU base,
  gsi1 6/6, gsi2 1/1, gsi3 6/6 (25 RCU / 25 WCU aggregate). No public
  resource policy; IAM/CLI access only. PITR stays **OFF** for the free tier.
- Single-table design (see `backend/persistence/dynamodb/`): feeds
  `FEED#<id>`/`META`, episodes `FEED#<id>`/`EP#<published>#<id>`,
  GUID dedup markers `FEED#<id>`/`GUID#<guid>`, plus insights/tags/task-logs.

## GSI cost model (why the migration is slow)

All GSIs project `ALL`, so every episode write fans out:

| Resource | Provisioned | Per-episode cost | Sustained target (~60%) |
|---|---|---|---|
| Base table | 12 WCU | ~4 WCU | 7 WCU/s |
| gsi1 | 6 WCU | ~3 WCU | 3.5 WCU/s |
| gsi2 | 1 WCU | feeds/task-logs only | 0.5 WCU/s |
| gsi3 | 6 WCU | ~3 WCU | 3.5 WCU/s |

`gsi1`/`gsi3` are the binding constraint for episodes (~1.1 rows/s
sustained, ~21 s per 25-item batch). Feeds are gsi2-bound (~0.5 WCU/s
target on a 1-WCU GSI → roughly an hour for all 1,053 feeds). Total
backfill ETA at the measured pace: ~5–6 days. This is a one-time cost;
steady-state ingest and insights fit comfortably inside the free tier
forever, at $0.

## How it runs

- `~/workspace/tunedin_chunked_migrate.py` (ops script, not shipped):
  chunked SQLAlchemy reads (500 rows at a time — the stock
  `backend.migrate_data` CLI OOMs materializing the whole episodes table,
  see XIN-111), reuses the adapter's item builders / 400 KiB guard /
  25-item batch writer, adds adaptive pacing and resumability.
- Pacing: each batch's sleep is derived from the *actual* serialized item
  sizes (`_item_wcu`), holding sustained consumption near the per-resource
  targets above. A leftover throttle backs off exponentially and retries
  (up to 60 attempts); throttles are expected occasionally and are not
  fatal.
- Resumability: progress checkpoints to `--state-file` as
  `{"table": ..., "offset": ...}` **after every 500-row chunk**. All writes
  are idempotent (last-writer-wins PutRequests), so a resumed run redoes
  at most one chunk.
- A cron schedule (`tunedin-dynamodb-backfill`, every 3h) supervises:
  each run launches **one detached session** of up to 10,000 rows
  (~2.5 h at the measured pace, inside the 3 h timeout) and exits.
  `flock -n` on the lock file serializes runs — if a previous session is
  still alive, the new run is a no-op.

## Launch pattern (read this before touching the schedule)

Backgrounded processes are **reaped when the agent/worker turn ends**.
Two sessions were silently killed this way with zero durable progress
before the pattern below was adopted. Sessions must be fully detached:

```bash
setsid nohup flock -n /path/to/tunedin-migration.lock \
  env AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=us-east-1 \
  /path/to/venv/bin/python -u /path/to/tunedin_chunked_migrate.py \
  --max-rows 10000 --state-file /path/to/tunedin-migration-state.json \
  >> /path/to/tunedin-migration.log 2>&1 < /dev/null &
```

Verify with `ps -ef`: the `flock` process must be reparented (PPID 1),
not a child of your shell. Do **not** wait on it in the launching turn.

## Failure modes, found the hard way

1. **Fixed-sleep pacing → throttle death spiral.** Constant sleeps leave
   zero headroom on the indexes; item-size variance throttles, the token
   bucket never refills, retries pile up. Fixed by per-batch sleeps sized
   from real item WCU cost (see "Pacing" above).
2. **Turn-teardown reaping.** See "Launch pattern". A session that is not
   `setsid`-detached dies with its parent turn — no traceback, no exit
   line, no state saved.
3. **No per-chunk checkpoint.** The first version saved state only after a
   whole table finished, so a killed/timed-out session redid up to a full
   session of episode rows. Now: checkpoint per 500-row chunk.
4. **Full-request throttling vs UnprocessedItems.** `BatchWriteItem` can
   raise `ProvisionedThroughputExceededException` for the whole request,
   not just return `UnprocessedItems`. The retry wrapper must catch the
   exception, not only the response field.
5. **`DynamoDBBackend.init()` re-enables PITR.** `init()` →
   `ensure_table()` turned PITR back on once. During migration, get a
   client via `_ensure_client()` and never call `init()`. PITR stays OFF.
6. **Session budget vs timeout.** 18,000 rows at ~1.1 rows/s ≈ 4.3 h >
   3 h cron timeout. Keep `--max-rows` at 10,000 (≈2.5 h) so sessions
   finish inside the timeout; with per-chunk checkpoints a timeout is
   merely a pause, not lost work.

## Monitoring

- Log: `tunedin-migration.log` — progress lines
  `<table>: <n> rows written (offset <o>)`, `throttled, backing off …`
  lines (routine), `DONE total_rows=<n>` at the very end.
- State: `tunedin-migration-state.json` — resume point.
- Liveness: `ps -ef | grep tunedin_chunked` and `fuser` on the lock file.
- The migration is complete **only** when the log tail shows `DONE`.
  Anything else (no `DONE`, no recent progress, no live process) means a
  session died — check for tracebacks, then relaunch with the pattern
  above; it resumes from the state file.

## Completion checklist

When the log shows `DONE`:

1. Delete the `tunedin-dynamodb-backfill` schedule.
2. Run count/parity verification, including GUID marker items.
3. Run the deterministic sample parity (sample size 100) per
   `docs/dynamodb-cutover.md`.
4. Report results. **Do not cut the application over** without separate
   authorization.
5. Rotate the AWS credentials (they were embedded in the schedule body
   during the migration — a known transient-use violation — and must be
   replaced when AWS work finishes).
