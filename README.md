# TuneIn

A podcast discovery and insights platform: crawl and ingest podcast catalogs,
then generate AI-powered insights (tags, summaries, chat) over episodes.

## Layout

- `backend/ingestion/` — RSS parsing, iTunes discovery client, catalog crawler,
  batch runner, and CLI
- `backend/persistence/` — SQLAlchemy models and the app database layer
- `backend/insights/` — LLM insight generation (plans and prompt specs)
- `backend/api/` — FastAPI backend *(planned)*
- `frontend/` — React frontend *(planned)*

See `PLAN.md`, `backend/PLAN.md`, and `frontend/PLAN.md` for the full roadmap.
Work is tracked in Linear (project **TuneIn**).

## Configuration

Edit `settings.yaml` at the repo root to suit your environment. It documents
every setting inline.

### Database (`database.backend`)

The ingestion tooling can use one of three database backends:

| Setting | Backend | When to use it |
|---|---|---|
| `"simple"` | **SimpleDB** — local SQLite file at `backend/ingestion/simple.db` (`backend/ingestion/simple_db.py`) | Local dev and batch crawling. Zero setup. |
| `"app"` | **App database** — shared DB via `database.app.url` (`backend/persistence/database.py`, `./tunedin.db` by default, Postgres in production) | When the API, worker, and ingestion tooling must share one database. |
| `"dynamodb"` | **DynamoDB** — single-table serverless backend (`backend/persistence/dynamodb/`) | Production scale without managing a database server; also works against DynamoDB Local for offline testing. |

The default is `"simple"` (SimpleDB), and `database.simple.path` points at the
existing `backend/ingestion/simple.db` file. `INGESTION_DATABASE_URL` can still
override the SimpleDB file location per environment; `database.app.url`
configures the app database.

The DynamoDB backend is configured under `database.dynamodb` in
`settings.yaml` (`table_name`, `region`, and an optional `endpoint_url` for
DynamoDB Local, e.g. `http://localhost:8000`), with per-environment overrides
via `DATABASE_DYNAMODB_TABLE_NAME`, `DATABASE_DYNAMODB_REGION`, and
`DATABASE_DYNAMODB_ENDPOINT_URL`. **AWS credentials are never stored in
settings** — they always come from the standard AWS chain
(`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, `~/.aws/credentials`, or an
IAM role). `init_db()` provisions the table (plus its GSIs and point-in-time
recovery) via `ensure_table()` — the DynamoDB equivalent of migrations:

```bash
DATABASE_BACKEND=dynamodb DATABASE_DYNAMODB_ENDPOINT_URL=http://localhost:8000 \
  python -m backend.ingestion.cli init-db     # provision the table
DATABASE_BACKEND=dynamodb DATABASE_DYNAMODB_ENDPOINT_URL=http://localhost:8000 \
  python -m backend.ingestion.cli status      # check the catalog
```

> **Note:** the backends are separate databases holding the same data.
> Switching `database.backend` starts from an empty catalog — to carry data
> over, copy the SQLite file first (e.g.
> `cp backend/ingestion/simple.db ./tunedin.db` when moving from SimpleDB to
> the app database), or use the data-migration tooling for DynamoDB.

### Persistence discipline: no ad-hoc SQL

All application code goes through the **Store / repository protocol**
(`backend/persistence/repositories.py`) — obtain a store via
`settings.open_store()` (or `async with settings.session_scope() as store:`)
and use its repositories. Never write ad-hoc SQL (`select()`,
`session.execute()`, …) in application code, and never import the backend
modules (`simple_db`, `database`, `dynamodb.*`) directly. This is what makes
the three backends interchangeable: the same application code runs unchanged
on SQLite, Postgres, and DynamoDB.

### Other settings

See `settings.yaml` — it documents every setting, including the
crawler's iTunes storefront countries and how many new episodes get queued for
AI processing on first sync.

## Development

```bash
python -m venv .venv && .venv/bin/pip install -r backend/requirements.txt
.venv/bin/pytest backend/ -q
```

### Database migrations

The app database (`database.backend: "app"`) is migrated with Alembic — never
`create_all`. `init_db()` runs `alembic upgrade head` automatically; databases
created by the old `create_all` path are stamped at head instead of migrated.

```bash
cd backend
../.venv/bin/alembic upgrade head                    # migrate to latest
../.venv/bin/alembic revision --autogenerate -m "..." # new migration after model changes
```

The migration scripts live in `backend/alembic/versions/` and target
`$DATABASE_URL` (default `./tunedin.db`). SimpleDB (`"simple"` backend) keeps
using `create_all` — it's the zero-setup local option with no migration
history.

Ingestion CLI:

```bash
python -m backend.ingestion.cli status
```
