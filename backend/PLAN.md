# TunedIn Backend - Master Implementation Plan

## 1. Component Overview
The **TunedIn Backend** is a high-performance Python (FastAPI) async web service. It manages podcast RSS ingestion, interfaces with Google Cloud Tasks for asynchronous queue processing, executes Gemini AI analysis on audio and transcript metadata, tracks user playback positions, and serves REST endpoints for cross-platform clients.

---

## 2. Sub-System Technical Plans

The backend technical plan is split into three modular sub-system implementation plans:

1. 🗄️ **Persistence Layer Plan**: [backend/persistence/PLAN.md](file:///home/xing808/dev/tunedin/backend/persistence/PLAN.md)
   - Async SQLAlchemy 2.0 ORM persistence.
   - SQLite default (`sqlite+aiosqlite`) for dev/testing, swappable to Postgres.
   - Models: [`User`](file:///home/xing808/dev/tunedin/backend/persistence/models/user.py), [`Feed`](file:///home/xing808/dev/tunedin/backend/persistence/models/feed.py), [`Episode`](file:///home/xing808/dev/tunedin/backend/persistence/models/episode.py), [`UserEpisodeProgress`](file:///home/xing808/dev/tunedin/backend/persistence/models/episode.py) composite PK `(user_id, episode_id)`, [`Tag`](file:///home/xing808/dev/tunedin/backend/persistence/models/tag.py), [`EpisodeTag`](file:///home/xing808/dev/tunedin/backend/persistence/models/tag.py), [`Insight`](file:///home/xing808/dev/tunedin/backend/persistence/models/insight.py), [`CuratedPlaylist`](file:///home/xing808/dev/tunedin/backend/persistence/models/playlist.py), [`PlaylistEpisode`](file:///home/xing808/dev/tunedin/backend/persistence/models/playlist.py), [`TaskLog`](file:///home/xing808/dev/tunedin/backend/persistence/models/task_log.py).

2. 📥 **Feed Ingestion & Task Queue Plan**: [backend/ingestion/PLAN.md](file:///home/xing808/dev/tunedin/backend/ingestion/PLAN.md)
   - Async RSS parser using `feedparser` & `httpx`.
   - `TaskQueueDriver` abstraction: `GCPCloudTasksDriver` (Google Cloud Tasks) & `LocalInMemoryDriver` (offline fallback).
   - Task worker webhook (`/api/worker/process-episode`).

3. 🤖 **Agentic Insights & AI Curation Plan**: [backend/insights/PLAN.md](file:///home/xing808/dev/tunedin/backend/insights/PLAN.md)
   - `AIAgentProvider` abstraction: `GeminiAgentProvider` (Google Gemini API with Pydantic JSON schemas) & `MockAgentProvider` fallback.
   - Timestamped key takeaway extraction, topic/entity tagging.
   - Conversational RAG Chat assistant (`/api/chat`) and dynamic playlist creation engine.

---

## 3. High-Level Directory Layout

```
backend/
├── PLAN.md                    # Master backend implementation plan
├── requirements.txt           # Python dependencies
├── persistence/               # Sub-system 1: Database & ORM models
│   ├── PLAN.md                # Persistence layer technical plan
│   ├── database.py            # Async engine, sessionmaker & init_db
│   ├── models/                # SQLAlchemy 2.0 ORM models
│   │   ├── __init__.py        # Re-exports for all models & Base
│   │   ├── base.py            # DeclarativeBase with common conventions
│   │   ├── user.py            # User entity
│   │   ├── feed.py            # Feed entity
│   │   ├── episode.py         # Episode & UserEpisodeProgress entities
│   │   ├── tag.py             # Tag & EpisodeTag entities
│   │   ├── insight.py         # Insight entity
│   │   ├── playlist.py        # CuratedPlaylist & PlaylistEpisode entities
│   │   └── task_log.py        # TaskLog entity
│   └── tests/                 # Persistence layer test suite
│       └── test_persistence.py
├── ingestion/                 # Sub-system 2: RSS parsing & task queue
│   ├── PLAN.md                # Ingestion & queue technical plan
│   ├── rss_service.py         # Async RSS feed retrieval and parser
│   ├── queue/                 # Queue drivers (base, GCP Cloud Tasks, local fallback)
│   │   ├── base.py
│   │   ├── gcp.py
│   │   └── local.py
│   └── tests/                 # Ingestion & queue test suite
├── insights/                  # Sub-system 3: AI agentic analysis & chat
│   ├── PLAN.md                # Insights & AI curation technical plan
│   ├── ai/                    # AI provider drivers & schemas (base, Gemini, mock)
│   │   ├── base.py
│   │   ├── gemini.py
│   │   └── mock.py
│   └── tests/                 # AI insights test suite
└── api/                       # REST API routers & FastAPI app entrypoint
    ├── main.py                # FastAPI app initialization, middleware, lifecycle
    ├── config.py              # Environment configuration & settings
    ├── routers/               # Endpoint handlers
    │   ├── feeds.py           # Feed management & discovery
    │   ├── episodes.py        # Episode playback & detail endpoints
    │   ├── progress.py        # User playback sync
    │   ├── chat.py            # AI conversational RAG endpoints
    │   ├── playlists.py       # Curated playlist endpoints
    │   └── worker.py          # Background worker webhooks
    └── schemas/               # API request & response Pydantic schemas
```

---

## 4. Verification Strategy
- **Unit & Integration Tests**: Run `pytest` across all sub-system test suites (`backend/persistence/tests/`, `backend/ingestion/tests/`, `backend/insights/tests/`).
- **Persistence Verification**: Verify schema initialization, foreign key constraints, and CRUD operations on SQLite & PostgreSQL.
- **Task Queue & Ingestion Verification**: Test RSS parsing with mock/real feeds and async task execution using `LocalInMemoryDriver`.
- **AI & RAG Verification**: Test structured JSON insight extraction and chat completion using `MockAgentProvider` and live `GeminiAgentProvider`.
