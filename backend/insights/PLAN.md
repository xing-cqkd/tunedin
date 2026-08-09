# TunedIn Backend - Agentic Insights & AI Curation Implementation Plan

## 1. Overview
The **Agentic Insights & AI Curation Sub-System** leverages Google Gemini API models to extract structured JSON knowledge from podcast episodes, generate timestamped key takeaways, extract entity and topic tags, and power an interactive conversational RAG chat assistant for content discovery and dynamic playlist generation.

---

## 2. Key Technology & Architecture

### AI Provider Abstraction (`AIAgentProvider`)
- **Abstract Base Class**: `AIAgentProvider` defining:
  - `analyze_episode(episode_data: dict) -> EpisodeAnalysisResult`
  - `generate_chat_response(query: str, chat_history: list, context_episodes: list) -> ChatResponse`
  - `curate_playlist(prompt: str, available_episodes: list) -> CuratedPlaylistResult`
- **Primary AI Driver (`GeminiAgentProvider`)**:
  - Leverages Google Gemini models (`google-genai` SDK).
  - Uses Pydantic structured schemas (`response_schema`) to guarantee strict JSON formatting for insights, timestamp arrays, and tags.
- **Fallback AI Driver (`MockAgentProvider`)**:
  - Built-in fallback when `GEMINI_API_KEY` is omitted, returning pre-seeded, intelligent insights for offline development and testing.

---

## 3. Core Capabilities

### A. Episode Analysis & Extraction Pipeline
For each ingested podcast episode, Gemini extracts:
1. **Executive Summary**: 2-3 sentence overview.
2. **Key Takeaways & Insights**: Timestamped key highlights (e.g. `[04:15] Neural Network Scaling Laws`).
3. **Structured Entity & Topic Tags**: Categorized tags (`Topic`, `Person`, `Concept`, `Industry`).

### B. Conversational RAG & Chat Assistant (`/api/chat`)
- Uses full-text & metadata vector/tag retrieval across ingested episodes as RAG context.
- Answers user questions, cites relevant timestamped episode moments, and provides direct "Create Curated Playlist" actions.

---

## 4. File Structure
```
backend/app/
├── services/
│   └── ai/
│       ├── base.py          # AIAgentProvider ABC & Pydantic JSON schemas
│       ├── gemini.py        # GeminiAgentProvider implementation
│       └── mock.py          # MockAgentProvider keyless fallback
└── api/
    └── chat.py              # Conversational RAG & AI Curation API routes
```
