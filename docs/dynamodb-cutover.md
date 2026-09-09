# DynamoDB cutover runbook

How to backfill the TuneIn catalog from SQLite (`simple` / `app`) into
DynamoDB, verify parity, cut traffic over, and roll back. Written to be
executed top to bottom by an operator.

> **Scope note:** every step below is executable today. Steps that touch
> real AWS are marked **[AWS]**; everything else runs locally. The
> kill-9 recovery test (step 4) is fully local and should be run *before*
> any AWS step.

Related Linear task: XIN-95.

## 0. Prerequisites

- The repo at a commit containing PRs #3 (migration CLI), #11 (settings),
  and #12 (DynamoDB migration adapter).
- Python deps installed (`pip install -r backend/requirements.txt`;
  `moto` + `boto3` needed only for the local tests, not for the runbook).
- **[AWS]** An AWS account with credentials in the standard chain
  (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars,
  `~/.aws/credentials`, or an IAM role). TuneIn never takes AWS
  credentials from settings — only table/region/endpoint.

Configure the DynamoDB target (same variables the migration CLI and the
settings system read):

```bash
export DATABASE_DYNAMODB_TABLE_NAME=tunedin
export DATABASE_DYNAMODB_REGION=us-east-1
# Leave DATABASE_DYNAMODB_ENDPOINT_URL *unset* for real AWS.
# (Set it to http://localhost:8000 only for DynamoDB Local testing.)
```

Source backends resolve exactly like the app modules do:

| backend    | env var                | default                                  |
|------------|------------------------|------------------------------------------|
| `simple`   | `INGESTION_DATABASE_URL` | `<repo>/backend/ingestion/simple.db`   |
| `app`      | `DATABASE_URL`           | `sqlite+aiosqlite:///./tunedin.db`     |

## 1. Provision the DynamoDB table **[AWS]**

```bash
DATABASE_BACKEND=dynamodb python -m backend.ingestion.cli init-db
```

This runs `ensure_table()`: creates the single table on on-demand billing,
the three GSIs (waiting for each to become ACTIVE — AWS allows only one
GSI creation per `UpdateTable` call), and enables point-in-time recovery.
It is idempotent; re-running it against an existing table only adds
missing indexes.

Sanity check:

```bash
DATABASE_BACKEND=dynamodb python -m backend.ingestion.cli status
```

Expect zero counts on a fresh table.

## 2. Dry-run the backfill (no writes)

```bash
python -m backend.migrate_data --source simple --target dynamodb --dry-run
```

The CLI prints the resolved identities first
(`sqlalchemy:sqlite:///... -> dynamodb:us-east-1:tunedin`) and then a
per-table row-count report. Confirm the source row counts look right and
no table is reported `skipped`.

## 3. Run the backfill **[AWS]**

```bash
python -m backend.migrate_data --source simple --target dynamodb
```

Properties you can rely on:

- **Idempotent.** Rows are upserted by primary key, so re-running the
  migration never duplicates data. If it is interrupted, just run it
  again.
- **FK-safe order.** Tables migrate parents-before-children.
- **Internal state is rebuilt, not copied.** Episode GUID-dedup markers
  and tag claim rows are transient coordination state; the adapter
  regenerates GUID markers from the episode rows using the same rule the
  repository write path uses, and skips tag claims.
- **Oversized rows fail loudly.** The shared 400 KiB item-size guard
  (`backend/persistence/validation.py`) is enforced on the migration
  write path and raises `ItemTooLargeError` naming the table — the same
  error both backends raise on the application write path, so a row the
  source accepted can never fail here silently.

For large catalogs, run it under `nohup` / `tmux`; BatchWrite retries
with exponential backoff and fails loudly after the cap instead of
spinning forever on throttling.

## 4. Kill-9 recovery test (local, no AWS)

Proves the "next run converges" property before you depend on it.
Run against two throwaway SQLite files so no AWS is involved:

```bash
# Seed a scratch source DB (or copy your real simple.db aside first).
cp backend/ingestion/simple.db /tmp/cutover-src.db
rm -f /tmp/cutover-tgt.db

# Start a backfill in the background, then kill -9 it mid-flight.
INGESTION_DATABASE_URL="sqlite+aiosqlite:////tmp/cutover-src.db" \
DATABASE_URL="sqlite+aiosqlite:////tmp/cutover-tgt.db" \
  python -m backend.migrate_data --source simple --target app &
MIGRATION_PID=$!
sleep 2  # let it get partway through the tables
kill -9 $MIGRATION_PID
wait $MIGRATION_PID 2>/dev/null

# Re-run to completion — idempotent retry must converge.
INGESTION_DATABASE_URL="sqlite+aiosqlite:////tmp/cutover-src.db" \
DATABASE_URL="sqlite+aiosqlite:////tmp/cutover-tgt.db" \
  python -m backend.migrate_data --source simple --target app

# Verify: parity must be clean after the interrupted + resumed runs.
INGESTION_DATABASE_URL="sqlite+aiosqlite:////tmp/cutover-src.db" \
DATABASE_URL="sqlite+aiosqlite:////tmp/cutover-tgt.db" \
  python -m backend.parity_check --source simple --target app
# expected: PARITY OK, exit code 0
```

If parity is clean, the recovery property holds: any interruption is
fixed by re-running the migration.

## 5. Parity validation **[AWS]**

After the real backfill:

```bash
python -m backend.parity_check --source simple --target dynamodb --sample 100
```

It checks, per table:

- row counts (source vs target),
- feed counts by `sync_status`,
- episode unprocessed counts,
- payload spot-checks on a deterministic sample of rows (`--sample N`;
  UUIDs and datetimes are normalized so representations compare equal
  across backends).

Exit code 0 and `PARITY OK` means the backfill is faithful. Any
`MISMATCH` lines name the table, the kind (`missing-row`, `extra-row`,
`payload-diff`, `missing-table`) and the differing columns — investigate
before cutting over. Re-running the migration is always safe (idempotent
upserts) and is the first remediation for a mismatch.

## 6. Cutover (per environment) **[AWS]**

Cutover is a configuration change, not a code deploy. For each
environment (dev → staging → prod), set:

```bash
export DATABASE_BACKEND=dynamodb
export DATABASE_DYNAMODB_TABLE_NAME=tunedin        # per-env table name
export DATABASE_DYNAMODB_REGION=us-east-1
# DATABASE_DYNAMODB_ENDPOINT_URL stays unset (real AWS)
# AWS credentials via the standard chain (env / ~/.aws / IAM role)
```

or the equivalent in `settings.yaml`:

```yaml
database:
  backend: dynamodb
  dynamodb:
    table_name: "tunedin"
    region: "us-east-1"
```

Then restart the ingestion tooling / API. Verify with:

```bash
DATABASE_BACKEND=dynamodb python -m backend.ingestion.cli status
```

The counts must match the parity report from step 5. Keep the SQLite
file around — it is your rollback source.

**Cost baseline.** During the backfill, capture a CloudWatch baseline
(`ConsumedWriteCapacityUnits`, `ThrottledRequests`) so you can tell
steady-state ingestion traffic apart from the one-time backfill spike.
The table uses on-demand billing; the backfill is the most write-heavy
operation you will ever run against it.

## 7. Rollback **[AWS]**

Rollback is a reverse migration to the SQLite file. The SQLite file was
never modified by the cutover (DynamoDB was a copy), but re-migrating
picks up any writes that landed in DynamoDB while it was primary:

```bash
# DynamoDB -> SQLite (uses the same CLI, reversed)
python -m backend.migrate_data --source dynamodb --target simple
python -m backend.parity_check --source dynamodb --target simple

# Point the environment back at SQLite
export DATABASE_BACKEND=simple   # or database.backend: simple in settings.yaml
```

Then restart services and re-run `status` to confirm.

## Quick reference

| Step | Command |
|------|---------|
| Provision table | `DATABASE_BACKEND=dynamodb python -m backend.ingestion.cli init-db` |
| Dry-run backfill | `python -m backend.migrate_data --source simple --target dynamodb --dry-run` |
| Backfill | `python -m backend.migrate_data --source simple --target dynamodb` |
| Parity | `python -m backend.parity_check --source simple --target dynamodb --sample 100` |
| Cutover | `DATABASE_BACKEND=dynamodb` (+ `DATABASE_DYNAMODB_*`) |
| Rollback | `python -m backend.migrate_data --source dynamodb --target simple` |
