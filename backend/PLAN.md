# TunedIn Backend - Master Implementation Plan

## 1. Component Overview
The **TunedIn Backend** is a high-performance Python (FastAPI) async web service. It manages podcast RSS ingestion, interfaces with Google Cloud Tasks for asynchronous queue processing, executes Gemini AI analysis on audio and transcript metadata, tracks user playback positions, and serves REST endpoints for cross-platform clients.

---

## 2. Sub-System Technical Plans

The backend technical plan is split into three modular sub-system implementation plans:

1. 🗄️ **Persistence Layer Plan**: [backend/persistence/PLAN.md](file:///home/chesd/dev/tunedin/backend/persistence/PLAN.md)
   - Async SQLAlchemy 2.0 ORM persistence.
   - SQLite default (`sqlite+aiosqlite`) for dev/testing, swappable to Postgres.
   - Models: `User`, `Feed`, `Episode`, `UserEpisodeProgress` composite PK `(user_id, episode_id)`, `Tag`, `EpisodeTag`, `Insight`, `CuratedPlaylist`, `PlaylistEpisode`, `TaskLog`.

2. 📥 **Feed Ingestion & Task Queue Plan**: [backend/ingestion/PLAN.md](file:///home/chesd/dev/tunedin/backend/ingestion/PLAN.md)
   - Async RSS parser using `feedparser`.
   - `TaskQueueDriver` abstraction: `GCPCloudTasksDriver` (Google Cloud Tasks) & `LocalInMemoryDriver` (offline fallback).
   - Task worker webhook (`/api/worker/process-episode`).

3. 🤖 **Agentic Insights & AI Curation Plan**: [backend/insights/PLAN.md](file:///home/chesd/dev/tunedin/backend/insights/PLAN.md)
   - `AIAgentProvider` abstraction: `GeminiAgentProvider` (Google Gemini API with Pydantic JSON schemas) & `MockAgentProvider` fallback.
   - Timestamped key takeaway extraction, topic/entity tagging.
   - Conversational RAG Chat assistant (`/api/chat`) and dynamic playlist creation engine.

---

## 3. High-Level Directory Layout

```
backend/
├── app/
│   ├── main.py                # FastAPI app initialization & CORS
│   ├── config.py              # Environment settings (Pydantic BaseSettings)
│   ├── database.py            # Async SQLAlchemy engine & session factory
│   ├── models/                # SQLAlchemy models (user, feed, episode, progress, tag, insight, playlist)
│   ├── schemas/               # Pydantic request & response schemas
│   ├── services/
│   │   ├── rss_service.py     # Feedparser RSS extraction engine
│   │   ├── ai/                # AIAgentProvider (Base, Gemini, Mock)
│   │   └── queue/             # TaskQueueDriver (Base, GCPCloudTasks, LocalInMemory)
│   └── api/                   # REST endpoints (feeds, episodes, progress, chat, playlists, worker)
├── tests/                     # Pytest suite
└── requirements.txt
```

---

## 4. Verification Strategy
- **Unit & Integration Tests**: Run `pytest` using SQLite in-memory database and `LocalInMemoryDriver`.
- **Playback & Ingestion Verification**: Verify model ops, task enqueuing, and API endpoints.
