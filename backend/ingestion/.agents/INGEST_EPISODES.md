# Ingesting Podcast Episodes: Earliest to Latest (Chronological Pipeline)

This guide provides operational and architectural instructions for AI agents and engineers on ingesting podcast episodes from a list of podcasts.

Episodes **must always be ingested and committed chronologically from earliest (oldest) to latest (newest)**. This ensures narrative continuity for serial podcasts, correct chronological indexing for downstream LLM insight generation, and orderly database insertion.

The instructions below cover both:
1. **First-Time Ingestion (Cold Start)**: Syncing a newly discovered podcast feed.
2. **Refreshing a Feed (Warm Incremental Sync)**: Checking an existing active feed for newly published episodes using HTTP conditional caching and deduplication.

---

## 1. Architectural Principles & Invariants

1. **Chronological Ordering Invariant (`published_at ASC`)**:
   - Standard podcast RSS feeds publish items in **reverse-chronological order** (newest episode at the top).
   - Ingestion pipelines must parse, deduplicate, and sort candidate episodes in **ascending chronological order** (`earliest -> latest`) before inserting them into the database or dispatching them to downstream task queues.
   - If publication dates are identical or missing, fallback to the reversed index of the feed entries (since the bottom of the feed represents the earliest content).

2. **Deduplication Hierarchy**:
   - **Primary Key / Unique Key**: `Episode.guid` scoped per `feed_id`.
   - **Secondary Key**: Enclosure audio URL (`Episode.audio_url`) when GUID is absent or malformed.
   - **Incremental Filter**: Skip any episode already present in `known_guids` or with `published_at <= latest_known_published_at` (unless updating existing records).

3. **HTTP Politeness & Conditional Caching**:
   - Always persist and send HTTP `ETag` (`If-None-Match`) and `Last-Modified` (`If-Modified-Since`) headers.
   - Handle `HTTP 304 Not Modified` gracefully: touch `last_fetched_at` and avoid redundant parsing.
   - Maintain a 0.25s – 0.5s inter-request delay to prevent origin CDN / server IP bans.

4. **Transactional Atomic Checkpointing**:
   - Commit transactions **per show**. If a feed fails, it must not roll back previously synced shows.
   - Update `Feed.sync_status`, `Feed.etag`, `Feed.last_modified`, and `Feed.last_fetched_at` atomically with the newly inserted episodes.

---

## 2. Ingestion Workflow Overview

```
                  ┌────────────────────────────────────────────────────────┐
                  │ 1. List Podcasts (Database Query / Input List / CLI)   │
                  └───────────────────────────┬────────────────────────────┘
                                              │
                                              ▼
                  ┌────────────────────────────────────────────────────────┐
                  │ 2. Determine Context (First-Time vs. Feed Refresh)     │
                  └───────────────────────────┬────────────────────────────┘
                                              │
                                              ▼
                  ┌────────────────────────────────────────────────────────┐
                  │ 3. Fetch Feed with Conditional Headers (ETag/Modified) │
                  └───────────────────────────┬────────────────────────────┘
                                              │
                         ┌────────────────────┴───────────────────┐
                         │                                        │
                 [HTTP 304 Not Mod]                               │ [HTTP 200 OK]
                         │                                        ▼
                         │                        ┌─────────────────────────────────┐
                         │                        │ 4. Parse XML/JSON Feed Entries   │
                         │                        └───────────────┬─────────────────┘
                         │                                        │
                         │                                        ▼
                         │                        ┌─────────────────────────────────┐
                         │                        │ 5. Filter Known GUIDs / Unseen  │
                         │                        └───────────────┬─────────────────┘
                         │                                        │
                         │                                        ▼
                         │                        ┌─────────────────────────────────┐
                         │                        │ 6. SORT ASCENDING: Oldest First │
                         │                        │    (Earliest -> Latest)         │
                         │                        └───────────────┬─────────────────┘
                         │                                        │
                         │                                        ▼
                         │                        ┌─────────────────────────────────┐
                         │                        │ 7. Insert Episodes (processed=0)│
                         │                        └───────────────┬─────────────────┘
                         │                                        │
                         ▼                                        ▼
                  ┌────────────────────────────────────────────────────────┐
                  │ 8. Commit Show & Episode State to simple.db / DB       │
                  │    Update last_fetched_at, etag, last_modified         │
                  └────────────────────────────────────────────────────────┘
```

---

## 3. Step-by-Step Execution Guide

### Step 1: Obtain the List of Podcasts

Podcasts can be listed from the `feeds` table in `simple.db` (or production database) depending on the job goal:

#### A. Listing Feeds for First-Time Ingestion
Filter for feeds with `sync_status IN ('discovered', 'pending')`:
```python
from sqlalchemy import select
from backend.persistence.models.feed import Feed

stmt = (
    select(Feed)
    .where(Feed.sync_status.in_(["discovered", "pending"]))
    .order_by(Feed.created_at.asc())
    .limit(batch_size)
)
pending_podcasts = (await session.execute(stmt)).scalars().all()
```

#### B. Listing Feeds for Refresh
Filter for active feeds whose `last_fetched_at` is older than the refresh threshold (e.g., 6 hours) or `NULL`:
```python
from datetime import datetime, timedelta, timezone
from sqlalchemy import or_, select
from backend.persistence.models.feed import Feed

threshold = datetime.now(timezone.utc) - timedelta(hours=6)
stmt = (
    select(Feed)
    .where(Feed.sync_status == "active")
    .where(or_(Feed.last_fetched_at.is_(None), Feed.last_fetched_at < threshold))
    .order_by(Feed.last_fetched_at.asc().nullsfirst())
    .limit(batch_size)
)
refresh_podcasts = (await session.execute(stmt)).scalars().all()
```

#### C. Listing from an Explicit List / URLs
If ingesting from an external list (e.g. JSON list, OPML, or iTunes search results):
```python
# Ingest or upsert show entries first, ensuring rss_url is populated
for p in podcast_list:
    feed = await service.save_podcast(db, p)
```

---

### Step 2: Fetch and Parse Feed (First-Time vs. Refresh)

For each podcast feed in the list:

1. **Load Existing State from Database**:
   - Query all existing GUIDs for this feed to avoid duplicate downloads:
     ```python
     ep_stmt = select(Episode.guid).where(Episode.feed_id == feed.feed_id)
     res = await db.execute(ep_stmt)
     known_guids: set[str] = {g for g in res.scalars().all() if g}
     ```
   - Query the maximum publication date already stored (for incremental sanity checks):
     ```python
     max_date_stmt = select(func.max(Episode.published_at)).where(Episode.feed_id == feed.feed_id)
     latest_known_date = (await db.execute(max_date_stmt)).scalar_one_or_none()
     ```

2. **Send HTTP Request with Caching Headers**:
   - If `feed.etag` exists: send `If-None-Match: <feed.etag>`.
   - If `feed.last_modified` exists: send `If-Modified-Since: <feed.last_modified>`.
   - Use browser-like User-Agent headers to avoid origin blocking:
     ```python
     headers = {
         "User-Agent": "Mozilla/5.0 (compatible; TunedInBot/1.0; +https://tunedin.ai)",
         "Accept": "application/rss+xml, application/xml, text/xml, */*",
     }
     ```

3. **Handle HTTP 304 Not Modified (Feed Refresh only)**:
   - If server responds with `304 Not Modified`, no new episodes have been released.
   - Update `feed.last_fetched_at = datetime.now(timezone.utc)` and `feed.error_count = 0`.
   - Commit and move to the next podcast in the list.

---

### Step 3: Extract and Filter Episodes

When HTTP 200 is received:

1. **Parse Entries**: Use `PodcastFeedParser` to extract all valid entries with an audio enclosure (`audio_url`).
2. **Filter Out Existing Episodes**:
   ```python
   candidate_episodes = []
   seen_guids_in_batch = set()

   for ep in parse_result.episodes:
       if not ep.guid or ep.guid in known_guids or ep.guid in seen_guids_in_batch:
           continue
       seen_guids_in_batch.add(ep.guid)
       candidate_episodes.append(ep)
   ```

---

### Step 4: Sort Episodes from Earliest to Latest (`published_at ASC`)

This is the **critical requirement**: feed entries are typically received newest-first. They must be reordered so that the earliest published episode comes first.

```python
from datetime import datetime, timezone

def episode_chronological_key(ep: ParsedEpisode) -> tuple:
    """
    Sort key to order episodes strictly from earliest to latest:
    1. Null publication dates placed first or fallback to min datetime.
    2. Ascending published_at datetime (UTC).
    3. Ascending episode_number (for tie-breaking within same release batch).
    """
    dt = ep.published_at
    if dt is not None:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
    else:
        # Fallback to minimum UTC datetime if date is unknown
        dt = datetime.min.replace(tzinfo=timezone.utc)

    ep_num = ep.episode_number if ep.episode_number is not None else 0
    return (dt, ep_num)

# Sort strictly earliest (oldest) to latest (newest)
sorted_episodes = sorted(candidate_episodes, key=episode_chronological_key)
```

> **Fallback for feeds with missing/identical dates**:
> If dates are missing across the feed, reverse the raw RSS entry order:
> ```python
> if all(ep.published_at is None for ep in candidate_episodes):
>     sorted_episodes = list(reversed(candidate_episodes))
> ```

---

### Step 5: Save Episodes and Update Show Metadata

Insert each episode in the sorted ascending order into the `episodes` table:

```python
new_episode_entities: list[Episode] = []

for ep_data in sorted_episodes:
    ep = Episode(
        feed_id=feed.feed_id,
        guid=ep_data.guid,
        title=ep_data.title,
        audio_url=ep_data.audio_url,
        duration=ep_data.duration,
        published_at=ep_data.published_at,
        summary=ep_data.summary,
        content_html=ep_data.content_html,
        transcript_url=ep_data.transcript_url,
        chapters_url=ep_data.chapters_url,
        image_url=ep_data.image_url,
        episode_type=ep_data.episode_type,
        episode_number=ep_data.episode_number,
        season_number=ep_data.season_number,
        explicit=ep_data.explicit,
        processed=False,  # Unprocessed, queued for LLM analysis in chronological order
    )
    db.add(ep)
    new_episode_entities.append(ep)

# Update Feed metadata from latest parse result
if parse_result.metadata.title:
    feed.title = parse_result.metadata.title
if parse_result.metadata.image_url:
    feed.image_url = parse_result.metadata.image_url
if parse_result.metadata.description:
    feed.description = parse_result.metadata.description
if parse_result.metadata.author:
    feed.author = parse_result.metadata.author

feed.etag = parse_result.metadata.etag or feed.etag
feed.last_modified = parse_result.metadata.last_modified or feed.last_modified
feed.last_fetched_at = datetime.now(timezone.utc)
feed.sync_status = "active"
feed.error_count = 0

await db.commit()
```

---

## 4. End-to-End Reference Implementation

Below is a complete, standalone async function showing how to ingest a list of podcasts chronologically:

```python
"""
backend/ingestion/chronological_ingest.py
Reference implementation for ingesting podcast episodes earliest-to-latest.
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Optional, Set
import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.ingestion.models import ParsedEpisode
from backend.ingestion.parser import PodcastFeedParser
from backend.persistence.models.episode import Episode
from backend.persistence.models.feed import Feed

logger = logging.getLogger("chronological_ingest")


def get_episode_sort_key(ep: ParsedEpisode) -> tuple:
    """Sort key guaranteeing earliest (oldest) episodes come first."""
    dt = ep.published_at
    if dt is not None:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
    else:
        dt = datetime.min.replace(tzinfo=timezone.utc)

    ep_num = ep.episode_number if ep.episode_number is not None else 0
    return (dt, ep_num)


async def ingest_podcast_episodes_chronologically(
    db: AsyncSession,
    feed: Feed,
    parser: PodcastFeedParser,
    client: httpx.AsyncClient,
) -> int:
    """
    Synchronizes a single podcast feed:
    - Works for first-time sync or refreshing an existing feed.
    - Preserves HTTP conditional caching (ETag / Last-Modified).
    - Sorts new episodes from earliest to latest before committing.
    
    Returns the number of new episodes saved.
    """
    now_utc = datetime.now(timezone.utc)

    # 1. Fetch existing known GUIDs for deduplication
    ep_stmt = select(Episode.guid).where(Episode.feed_id == feed.feed_id)
    known_guids: Set[str] = set((await db.execute(ep_stmt)).scalars().all())

    # 2. Fetch and parse feed
    try:
        parse_result = await parser.fetch_and_parse(
            rss_url=feed.rss_url,
            known_guids=known_guids,
            etag=feed.etag,
            last_modified=feed.last_modified,
            client=client,
        )
    except Exception as err:
        logger.error("Failed to fetch feed %s: %s", feed.rss_url, err)
        feed.error_count += 1
        feed.sync_status = "error"
        feed.last_fetched_at = now_utc
        await db.commit()
        return 0

    # 3. Handle 304 Not Modified
    if parse_result.is_not_modified:
        logger.info("Feed not modified (304): %s", feed.title)
        feed.last_fetched_at = now_utc
        feed.error_count = 0
        await db.commit()
        return 0

    # 4. Filter candidate episodes against known GUIDs and within-batch duplicates
    candidates: List[ParsedEpisode] = []
    seen_in_batch: Set[str] = set()

    for ep in parse_result.episodes:
        if not ep.guid or ep.guid in known_guids or ep.guid in seen_in_batch:
            continue
        seen_in_batch.add(ep.guid)
        candidates.append(ep)

    # 5. SORT ASCENDING: Earliest to Latest
    if all(ep.published_at is None for ep in candidates):
        # Reverse feed order if dates are completely absent
        sorted_episodes = list(reversed(candidates))
    else:
        sorted_episodes = sorted(candidates, key=get_episode_sort_key)

    # 6. Insert episodes chronologically
    for ep_data in sorted_episodes:
        episode = Episode(
            feed_id=feed.feed_id,
            guid=ep_data.guid,
            title=ep_data.title,
            audio_url=ep_data.audio_url,
            duration=ep_data.duration,
            published_at=ep_data.published_at,
            summary=ep_data.summary,
            content_html=ep_data.content_html,
            transcript_url=ep_data.transcript_url,
            chapters_url=ep_data.chapters_url,
            image_url=ep_data.image_url,
            episode_type=ep_data.episode_type,
            episode_number=ep_data.episode_number,
            season_number=ep_data.season_number,
            explicit=ep_data.explicit,
            processed=False,
        )
        db.add(episode)

    # 7. Update feed metadata
    if parse_result.metadata.title:
        feed.title = parse_result.metadata.title
    if parse_result.metadata.image_url:
        feed.image_url = parse_result.metadata.image_url
    if parse_result.metadata.description:
        feed.description = parse_result.metadata.description
    if parse_result.metadata.author:
        feed.author = parse_result.metadata.author

    feed.etag = parse_result.metadata.etag or feed.etag
    feed.last_modified = parse_result.metadata.last_modified or feed.last_modified
    feed.last_fetched_at = now_utc
    feed.sync_status = "active"
    feed.error_count = 0

    await db.commit()
    logger.info("Successfully ingested %d episodes (earliest -> latest) for: %s", len(sorted_episodes), feed.title)
    return len(sorted_episodes)


async def ingest_podcast_list(
    db: AsyncSession,
    podcasts: List[Feed],
    delay_between_feeds: float = 0.35,
) -> dict:
    """Iterates through a list of podcasts and ingests episodes chronologically."""
    parser = PodcastFeedParser()
    total_new_episodes = 0
    synced_count = 0

    transport = httpx.AsyncHTTPTransport(retries=2)
    async with httpx.AsyncClient(
        transport=transport,
        timeout=25.0,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (compatible; TunedInBot/1.0)"},
    ) as client:
        for idx, feed in enumerate(podcasts, start=1):
            logger.info("[%d/%d] Ingesting: %s", idx, len(podcasts), feed.title)
            new_eps = await ingest_podcast_episodes_chronologically(db, feed, parser, client)
            total_new_episodes += new_eps
            synced_count += 1
            await asyncio.sleep(delay_between_feeds)

    return {
        "podcasts_processed": synced_count,
        "new_episodes_saved": total_new_episodes,
    }
```

---

## 5. Comparison: First-Time Ingestion vs. Feed Refresh

| Dimension | First-Time Ingestion (Cold Start) | Feed Refresh (Incremental Sync) |
|---|---|---|
| **Target Feeds** | `sync_status IN ('discovered', 'pending')` | `sync_status = 'active'` |
| **Existing State** | No episodes in DB, `known_guids` is empty, `etag`/`last_modified` are `None` | Hundreds of existing episodes in DB, `etag` & `last_modified` cached |
| **HTTP Caching** | Direct HTTP 200 GET | Conditional GET (`If-None-Match`, `If-Modified-Since`); handles HTTP 304 |
| **Candidate Count** | Complete historical catalog (50 – 2,000+ episodes) | 1 – 10 newly published episodes |
| **Sorting Behavior** | Re-sort entire historical episode list: Episode #1 $\rightarrow$ Latest | Re-sort only newly discovered delta: Earliest new $\rightarrow$ Newest new |
| **Result Status** | Feed status transitions to `active` | Feed status remains `active`; `last_fetched_at` updated |
| **Downstream Effect** | LLM can backfill insights chronologically from origin | LLM instantly ingests latest episodes in order of release |

---

## 6. Verification and Troubleshooting Checklist

1. **Verify Chronological Insertion**:
   Run a quick SQL query on any newly ingested show to verify that `created_at` or `episode_id` order matches `published_at ASC`:
   ```sql
   SELECT episode_id, title, published_at 
   FROM episodes 
   WHERE feed_id = '<feed_uuid>' 
   ORDER BY published_at ASC 
   LIMIT 10;
   ```
2. **Check Unprocessed Queue**:
   Verify that all newly inserted episodes have `processed = 0`:
   ```sql
   SELECT count(*) FROM episodes WHERE processed = 0;
   ```
3. **Handle Rate Limiting (HTTP 429)**:
   If a host responds with 429, implement exponential backoff (e.g. 2s, 4s, 8s) and persist `delay_between_feeds >= 0.35s`.
4. **Invalid / Future Timestamps**:
   Some podcast feeds contain errant future publication dates. If `published_at > now() + 1 day`, clamp or log a warning to prevent sorting anomalies.
