# TunedIn Backend - Insights & Curation Implementation Plan

> XIN-66 (2026-09-11): this plan now describes what exists. The provider
> abstraction, Gemini driver, and RAG chat below are **(planned)** — they are
> not implemented and no code for them exists in this directory.

## 1. Overview
The **Insights & Curation Sub-System** extracts structured knowledge from
podcast episodes (tags + timestamped takeaways) from title, summary, and
shownotes — no audio, no transcripts — and persists them through the Store
persistence layer.

---

## 2. What Exists Today

- [`EXTRACT.md`](EXTRACT.md): the tag + insight extraction prompt. Defines the
  tag dimensions, safety-as-tags rules, news-dating cutoff, privacy/redaction
  rules, rebroadcast detection, and the played-vs-discussed carve-out.
- [`BACKFILL.md`](BACKFILL.md): the playbook for running the extraction
  backfill through the Store persistence layer.

Extraction is currently performed by hand with the prompt (a standing
project decision — no LLM agent pipeline exists in this repo).

---

## 3. Core Capabilities

### A. Episode Analysis & Extraction Pipeline (implemented via prompt)
For each ingested podcast episode, extraction produces:
1. **Executive Summary**: 2-3 sentence overview.
2. **Key Takeaways & Insights**: Timestamped key highlights (e.g. `[04:15] Neural Network Scaling Laws`).
3. **Structured Entity & Topic Tags**: Categorized tags (`Topic`, `Person`, `Concept`, `Industry`).

### B. Conversational RAG & Chat Assistant (`/api/chat`) — (planned)
- Uses full-text & metadata vector/tag retrieval across ingested episodes as RAG context.
- Answers user questions, cites relevant timestamped episode moments, and provides direct "Create Curated Playlist" actions.

---

## 4. File Structure
```
backend/insights/
├── PLAN.md              # This plan
├── EXTRACT.md           # Tag + insight extraction prompt
└── BACKFILL.md          # Backfill playbook
```

---

## 5. Planned (not implemented)
- `AIAgentProvider` abstraction (`analyze_episode`, `generate_chat_response`, `curate_playlist`).
- `GeminiAgentProvider` (Google Gemini API) and `MockAgentProvider` fallback.
- Conversational RAG chat endpoint (`/api/chat`).
