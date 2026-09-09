# Data Migration Tool — Implementation Notes

## What it is

`backend/migrate_data.py` copies every row from one configured database
backend to another:

```bash
python -m backend.migrate_data --source simple --target app [--dry-run]
```

- `--source` / `--target`: `simple` (SimpleDB, `backend/ingestion/simple.db`)
  or `app` (app database, `./tunedin.db` by default).
- `--dry-run`: prints per-table row counts that *would* be copied; writes nothing
  (the target is not even initialized).
- Refuses to run when source and target resolve to the same database
  (exit code 2 with a clear message).

Backend URLs are resolved from the same environment variables the backend
modules read at import time — `INGESTION_DATABASE_URL` for `simple`,
`DATABASE_URL` for `app` — with the same defaults those modules use. The
modules are imported lazily *after* the env var is pinned, so overrides are
honored exactly as if the modules had been imported directly. The settings
singleton is deliberately bypassed (source and target differ in one run).

## How the copy works

- Tables iterate in `Base.metadata.sorted_tables` order (parents before
  children), so foreign keys are never violated.
- Rows are read as plain `{column: value}` dicts and written with
  `session.merge()` — an upsert by primary key — so re-runs are idempotent
  and never duplicate rows.
- The copy is fully generic: no per-model code. The ORM class for each table
  is looked up from `Base.registry.mappers`.
- A per-table summary (`table | source rows | copied`) plus totals is printed.

## DynamoDB seam

`migrate()` orchestrates purely against the `Backend` ABC:

- `table_names` — tables in dependency order
- `read_table(name) -> list[dict]`
- `write_rows(name, rows) -> int` (idempotent upsert)
- `init()` / `close()` / `identity`

`SqlAlchemyBackend` is the current implementation (with a `from_url()`
constructor for programmatic/test use). A future DynamoDB backend subclasses
`Backend` and maps each table name to a DynamoDB table — `read_table` scans,
`write_rows` batch-writes keyed by the table's key schema (PutRequest is
naturally idempotent) — with zero changes to `migrate()`, the CLI, or the
reporting. The one design decision to revisit then: DynamoDB has no
server-side FK ordering, so `table_names` order only matters for
read-your-writes consistency, not constraint enforcement.

## Tests

`backend/persistence/tests/test_migrate_data.py` (11 tests): full copy with
value assertions, idempotent re-run, dry-run writes nothing, same-backend
refusal (in-process and via CLI exit code), FK ordering, env-var URL
resolution, and two subprocess end-to-end CLI runs (`--source simple
--target app`, plus dry-run).
