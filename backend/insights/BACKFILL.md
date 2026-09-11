# Tag-extraction backfill playbook

How to run LLM tag extraction across the episode corpus and persist the
results through the persistence layer. This playbook covers procedure only;
the backfill runner itself is a separate implementation task.

Prompt used for extraction: `backend/insights/EXTRACT.md`.

## 1. Prerequisites

- The extraction prompt (`EXTRACT.md`) merged and reviewed — this is the
  only prompt the backfill may use.
- A Gemini API key (`GEMINI_API_KEY`). Without it, the provider falls back
  to `MockAgentProvider`, which returns canned insights — never run a
  backfill against the mock provider.
- Database reachable via `settings.open_store()`. The backfill must go
  through the `Store` abstraction (`backend/persistence/repositories.py`),
  never raw SQL or raw DynamoDB calls, so the same runner works on both
  backends (SQLAlchemy today, DynamoDB after cutover).
- Schema up to date: run `init_db()` (Alembic) before the first run.

## 2. The extraction loop

Work in batches. One batch = N episodes end-to-end (extract → validate →
persist → commit → mark processed). Suggested N = 50; tune against Gemini
rate limits and your write throughput.

```python
from settings import open_store
from backend.insights import pipeline  # for episode reads

async with open_store() as store:
    total = await store.episodes.count_unprocessed()
    while True:
        episodes = await store.episodes.list_unprocessed(limit=BATCH)
        if not episodes:
            break
        for ep in episodes:
            feed = await store.feeds.get_by_id(ep.feed_id)
            prompt = render("backend/insights/EXTRACT.md", feed=feed, episode=ep)
            raw = await gemini.generate(prompt)          # structured JSON output
            tags, insights = validate_and_normalize(raw) # see section 3
            for name, category in tags:
                tag = await store.tags.get_or_create(name, category)
                await store.tags.add_episode_tag(ep.episode_id, tag.tag_id)
            await store.insights.save_many(
                [Insight(episode_id=ep.episode_id, title=t, detail=d,
                         timestamp_seconds=s, insight_type=k)
                 for (t, d, s, k) in insights]
            )
            await store.episodes.mark_processed(ep.episode_id, True)
        await store.commit()
```

Why these APIs:

- `episodes.list_unprocessed(limit=...)` / `count_unprocessed()` — the
  canonical resumable work queue. Crashed runs resume by re-querying;
  already-processed episodes are excluded by the `processed` flag.
- `tags.get_or_create(name, category)` — concurrency-safe on both backends
  (SQL unique constraint on `(name, category)`; DynamoDB conditional claim
  on the hashed natural key). Safe to call from parallel workers.
- `tags.add_episode_tag(episode_id, tag_id)` — idempotent; re-adding a link
  is a no-op, so reprocessing an episode never duplicates links.
- `insights.save_many(...)` — one write unit per batch.
- `store.commit()` — finalizes the unit of work. On SQL this commits the
  transaction; on DynamoDB it is a no-op safety net (each `save` already
  wrote through).

## 3. Output validation and normalization (before any write)

Every model response must pass validation before it touches the store:

1. Parse as JSON; reject and retry (max 3) on malformed output. Log the
   episode id and raw output to `task_logs` (`task_type="extract_backfill"`,
   `status="failed"`) for later inspection.
2. Tag names: lowercase everything (including acronyms); preserve internal
   apostrophes/hyphens (`kevin o'connor`, `gamma-ray burst`).
3. Dedupe tags within the episode; drop empty strings.
4. Enforce the EXTRACT.md word caps: 4 words max for ordinary tags,
   8 words max for verbatim listener questions, no cap for dated-news tags
   (`<topic> <month> <year>`).
5. Category must be one of the nine dimensions in EXTRACT.md
   (entity, subject, trope, audience, modality, temporal, listening,
   safety, geography).

## 4. Idempotency and crash recovery

- Rerunning the backfill is always safe: `list_unprocessed` skips finished
  episodes, `add_episode_tag` is idempotent, `get_or_create` never duplicates
  tags, and `mark_processed` only flips a flag.
- Do NOT delete and re-extract wholesale unless the prompt changed
  materially. `Tag` rows are shared across episodes; deleting tags would
  break other episodes' links. To re-extract with a new prompt version,
  delete only the `episode_tags` links and `insights` rows for the target
  episodes, then reset their `processed` flags — never the `tags` table.
- Record each run in `task_logs`: one row per run (`task_type`,
  `payload_json` with prompt git SHA, batch size, episode count, model
  name) plus one row per failed episode. The prompt git SHA in the log is
  how you later answer "which prompt version tagged this episode".

## 5. Backend-specific notes

- **SQL (SQLAlchemy):** batch in a single session per batch; `commit()`
  once per batch. Watch connection-pool exhaustion if you parallelize —
  prefer a small worker pool (2–4) with batches of 50 over many tiny
  sessions.
- **DynamoDB:** writes are write-through, so `commit()` is a no-op; the
  unit of work is really the batch of `get_or_create`/`add_episode_tag`
  calls. Respect the shared 400 KiB per-item guard
  (`MAX_ITEM_BYTES` in `backend/persistence/validation.py`) — episode
  summaries are inputs, not stored items, so this only bites on
  pathological tag sets. Prefer `save_many` batching for insights over
  per-insight `save`.

## 6. Rate limits, cost, and pacing

- ~556k episodes in the corpus (Sept 2026). At ~10 tags/episode and a
  medium-length prompt, budget tokens accordingly and run a 1k-episode
  pilot first to measure real per-episode cost and latency.
- Pace Gemini calls under your quota; back off and retry on 429/5xx with
  jitter. A failed batch must not mark its episodes processed — only call
  `mark_processed` after the batch's writes are confirmed.
- Dry-run mode: render prompts and call the model but skip all writes and
  `mark_processed`; use it to validate a new prompt version on a few
  hundred episodes before a full run.

## 7. QA sampling

After (or during) a run, sample episodes per the evaluation methodology:
stratify by shownote length (empty / thin / medium / rich), spot-check tag
quality against the four user jobs — *find this episode*, *find episodes
about X*, *what to play next*, *is this for me* — and re-run the
normalization audit (word caps, lowercase, no duplicate tags) over the
sample. Fix prompt issues first, then re-extract only the affected slice
(see section 4).

## 8. What NOT to do

- Do not bypass `open_store()` / the repositories.
- Do not run against `MockAgentProvider`.
- Do not mark episodes processed before their tags and insights are
  persisted.
- Do not store raw model output; only validated, normalized tags and
  insights go into `tags` / `episode_tags` / `insights`.
