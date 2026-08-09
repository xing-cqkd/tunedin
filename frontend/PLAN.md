# TunedIn Frontend - Technical Implementation Plan

## 1. Component Overview
The **TunedIn Frontend** is a modern, responsive React web application built with Vite and Vanilla CSS. It features a mobile-first, touch-friendly UI design system that seamlessly adapts across desktop web, mobile web, and provides clean abstractions for future expansion to native iOS/Android apps via **React Native / Expo**.

---

## 2. Key Architecture & Design Decisions

### Cross-Platform & Native Ready Architecture
To guarantee effortless migration to **React Native / Expo**:
- **Decoupled Business Logic**: Custom React hooks (`useFeeds`, `useEpisodes`, `useAudioPlayer`, `useAIChat`) handle state, API fetching, audio playback control, position syncing, and chat orchestration independently from HTML DOM rendering.
- **Playback Progress Sync**: `useAudioPlayer` periodically syncs playback position (`position_seconds`, `completed`) to the backend `/api/progress` API and automatically resumes playback from saved positions.
- **Platform Abstraction Layer**: API client calls are isolated in `src/services/api.js`.

### Visual Aesthetics & UI Design System
- **Theme**: Dark Glassmorphism (`#090d16` base, translucent `#131927` glass cards, backdrop blur, glowing borders).
- **Accents**: Neon Indigo (`#6366f1`), Purple (`#8b5cf6`), Emerald (`#10b981`), Pink (`#ec4899`).
- **Responsive Layout**: Sidebar + Main Content + Audio Player on desktop; Bottom Navigation Bar + Drawer Overlays on mobile screens.

---

## 3. Core UI Components

1. **Header & Navigation (`Header.jsx`)**
   - App branding logo, search input, AI topic tags bar, "+ Add Feed" modal trigger.

2. **Feed List Sidebar (`FeedList.jsx`)**
   - Displays subscribed podcast feeds with unread episode counters and quick filter capability.

3. **Episode Card & Grid (`EpisodeCard.jsx`)**
   - Glassmorphic card displaying podcast artwork, episode title, duration, AI summary excerpt, listening progress bar (`position_seconds` / `duration`), and interactive AI topic tags.

4. **Episode Detail & AI Insight Modal (`EpisodeDetailModal.jsx`)**
   - Modal drawer revealing AI Key Takeaways, Timestamped Insights (clicking a timestamp jumps audio player to that exact second), saved playback position, and full transcript preview.

5. **Docked Audio Player (`AudioPlayer.jsx`)**
   - Persistent bottom player with timeline seeking scrubber, play/pause, skip +/- 15s, volume control, playback progress sync, and speed multiplier scaling (1x, 1.25x, 1.5x, 2x).

6. **AI Chat & Playlist Curation Sidebar (`ChatSidebar.jsx`)**
   - Interactive slide-over assistant enabling conversational content search across ingested podcasts and one-click dynamic playlist creation.

7. **Add Feed Modal (`AddFeedModal.jsx`)**
   - Modal dialog accepting RSS feed URLs for instant ingestion.

---

## 4. Directory Layout

```
frontend/
├── index.html
├── package.json
├── vite.config.js
└── src/
    ├── index.css              # Vanilla CSS design system: glassmorphism, keyframes, mobile media queries
    ├── App.jsx                # Core app layout & provider wiring
    ├── components/
    │   ├── Header.jsx
    │   ├── FeedList.jsx
    │   ├── EpisodeCard.jsx
    │   ├── EpisodeDetailModal.jsx
    │   ├── ChatSidebar.jsx
    │   ├── PlaylistView.jsx
    │   ├── AudioPlayer.jsx
    │   └── AddFeedModal.jsx
    ├── hooks/                 # Platform-agnostic state hooks (React Native ready)
    │   ├── useFeeds.js
    │   ├── useEpisodes.js
    │   ├── useAudioPlayer.js  # Audio controls & periodic position sync to /api/progress
    │   └── useAIChat.js
    └── services/
        └── api.js             # Fetch wrapper for backend REST, Progress & Chat endpoints
```

---

## 5. Verification Strategy
- **Build Verification**: Run `npm run build` to confirm zero lint/type errors.
- **Cross-Device UI Verification**: Test responsive layout at desktop (>1024px), tablet (768px-1024px), and mobile phone (<768px) viewports.
- **Playback Position Verification**: Play audio, seek position, reload page, and verify audio player resumes from the saved `position_seconds`.
