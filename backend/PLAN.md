# TunedIn Backend - Master Implementation Plan

> XIN-66 (2026-09-11): this plan now describes the code as it exists.
> Aspirational items that are not implemented are marked **(planned)**.

## 1. Component Overview
The **TunedIn Backend** is a Python async service. It manages podcast discovery via Apple Podcasts (iTunes Search API), RSS metadata ingestion, asynchronous queue processing (local in-memory driver; Google Cloud Tasks driver available), tag/insight extraction over episode metadata, and HTTP endpoints (FastAPI) for feeds, developer APIs, and curated-playlist RSS.

---

## 2. Sub-System Technical Plans

The backend technical plan is split into three modular sub-system implementation plans:

1. 🗄️ **Persistence Layer Plan**: [backend/persistence/PLAN.md](persistence/PLAN.md)
   - Async SQLAlchemy 2.0 ORM persistence.
   - SQLite default (`sqlite+aiosqlite`) for dev/testing, swappable to Postgres; DynamoDB backend via `persistence/dynamodb/`.
   - Models: [`User`](persistence/models/user.py), [`Feed`](persistence/models/feed.py), [`Episode`](persistence/models/episode.py), [`Tag`](persistence/models/tag.py), [`Insight`](persistence/models/insight.py), [`CuratedPlaylist`](persistence/models/playlist.py), [`PlaylistEpisode`](persistence/models/playlist.py), [`TaskLog`](persistence/models/task_log.py).
   - Store protocol: [`SQLAlchemyStore`](persistence/sqlalchemy_store.py), [`DynamoDBStore`](persistence/dynamodb/store.py).

2. 📡 **Feed Ingestion, Apple Podcasts Discovery & Task Queue Plan**: [backend/ingestion/PLAN.md](ingestion/PLAN.md)
   - Podcast discovery & search via **Apple Podcasts (iTunes Search API)** (zero API-key requirement).
   - Direct extraction of canonical publisher RSS URLs (`feedUrl`).
   - Async RSS parser using `feedparser` & `httpx` with conditional caching (`ETag`, `Last-Modified`) & incremental deduplication.
   - Metadata normalization and direct database synchronization for feeds and episodes (without storing local media files).
   - `TaskQueueDriver` abstraction in [`task_queue/`](ingestion/task_queue/): `GCPCloudTasksDriver` (Google Cloud Tasks) & `LocalInMemoryDriver` (offline fallback).
   - Task worker webhook (`/api/worker/process-episode`, see [api/worker.py](api/worker.py)) — validates the `PROCESS_EPISODE` payload and 501s until the worker is implemented (XIN-31).

3. 🧠 **Insights & Curation Plan**: [backend/insights/PLAN.md](insights/PLAN.md)
   - [`EXTRACT.md`](insights/EXTRACT.md): tag + insight extraction prompt (title/summary/shownotes only — no audio).
   - [`BACKFILL.md`](insights/BACKFILL.md): backfill playbook through the Store persistence layer.
   - Agentic providers and conversational RAG chat are **(planned)** — not implemented.

---

## 3. High-Level Directory Layout

```
backend/
├── PLAN.md                    # Master backend implementation plan (this file)
├── requirements.txt           # Python dependencies
├── api/                       # HTTP API (FastAPI); app factory: api/__init__.py::create_app
│   ├── __init__.py            # create_app() — wires routers, rate limiting
│   ├── _etag.py               # ETag / conditional-request helpers
│   ├── feeds.py               # Public curated-playlist RSS endpoints (/f/<slug>)
│   ├── rss.py                 # RSS rendering (no web framework)
│   ├── developer.py           # Developer API (/api/v1)
│   ├── worker.py              # Worker webhook (PROCESS_EPISODE) — XIN-31
│   └── tests/                 # API test suite
├── ingestion/                 # Discovery, RSS parsing & task queue
│   ├── PLAN.md
│   ├── cli.py                 # CLI entrypoint (run_sync_only, batch ingest)
│   ├── batch_runner.py        # Batch ingestion runner
│   ├── crawler.py             # Chart/topic crawler
│   ├── itunes.py              # Apple Podcasts / iTunes Search API client
│   ├── parser.py              # Async RSS feed retrieval & incremental parser
│   ├── service.py             # Feed ingestion & persistence coordination service
│   ├── models.py              # Ingestion & discovery dataclasses
│   ├── simple_db.py           # Legacy ingestion SQLite stack (see XIN-30)
│   ├── task_queue/            # Queue drivers (base, GCP Cloud Tasks, local fallback)
│   │   ├── base.py
│   │   ├── gcp.py
│   │   └── local.py
│   └── tests/                 # Ingestion test suite
├── insights/                  # Extraction prompts & playbooks (no code yet)
│   ├── PLAN.md
│   ├── EXTRACT.md             # Tag + insight extraction prompt
│   └── BACKFILL.md            # Backfill playbook
├── persistence/               # Database & ORM models
│   ├── PLAN.md
│   ├── database.py            # Async engine, sessionmaker & init_db
│   ├── _sqlite.py             # Shared SQLite pragmas
│   ├── sqlalchemy_store.py    # SQLAlchemy Store implementation
│   ├── repositories.py        # Repository protocol
│   ├── migrate_data.py        # Backend-to-backend migration CLI
│   ├── parity_check.py        # Cross-backend parity checker
│   ├── models/                # SQLAlchemy 2.0 ORM models
│   ├── dynamodb/              # DynamoDB backend (table, store, repositories)
│   └── tests/                 # Persistence test suite
└── alembic/                   # Migrations
    └── versions/
```

---

## 4. Verification Strategy
- **Unit & Integration Tests**: Run `pytest` across all sub-system test suites (`backend/api/tests/`, `backend/persistence/tests/`, `backend/ingestion/tests/`). Chester runs the suite himself; contributors run `python -m py_compile` at minimum.
- **Persistence Verification**: Verify schema initialization, foreign key constraints, and CRUD operations on SQLite & DynamoDB (moto).
- **Task Queue & Ingestion Verification**: Test RSS parsing with mock/real feeds, iTunes client search/lookup endpoints, and async task execution using `LocalInMemoryDriver`.
