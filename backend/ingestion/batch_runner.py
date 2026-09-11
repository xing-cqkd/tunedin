import asyncio
from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
import httpx

from backend.config import configure_logging, get_settings
from backend.ingestion.orchestration import (
    FeedSyncOrchestrator,
    SyncPolicy,
    fetch_status_code,
)
from backend.ingestion.service import FeedSyncService
from backend.ingestion.task_queue import get_queue_driver
from settings import describe_database, get_auto_queue_episodes, init_db, session_scope

logger = logging.getLogger("batch_ingest")


def _progress_file() -> Path:
    """Progress tracker path, from config (XIN-63).

    Defaults to ``backend/.data/podcast_ingest.md`` (override with
    ``PODCAST_PROGRESS_FILE``); no longer hard-coded inside ``.local_agents``.
    """
    return get_settings().progress_file


def load_existing_logs() -> List[str]:
    """Preserves recent log entries from the existing progress markdown file."""
    progress_file = _progress_file()
    if not progress_file.exists():
        return []
    try:
        content = progress_file.read_text(encoding="utf-8")
        if "```text" in content:
            block = content.split("```text", 1)[1].split("```", 1)[0].strip()
            lines = [l for l in block.splitlines() if l.strip() and not l.startswith("Ingestion")]
            return lines[-20:]
    except Exception:
        pass
    return []


async def write_progress_file(
    batch_num: int,
    total_batches: Optional[int],
    batch_synced_shows: int,
    batch_new_episodes: int,
    recent_logs: List[str],
    last_error: Optional[str] = None,
) -> None:
    """Updates the podcast_ingest.md progress tracker (path from config)."""
    progress_file = _progress_file()
    progress_file.parent.mkdir(parents=True, exist_ok=True)

    async with session_scope() as store:
        total_feeds = await store.feeds.count_all()
        active_feeds = await store.feeds.count_by_status("active")
        pending_feeds = await store.feeds.count_by_statuses(["discovered", "pending"])
        error_feeds = await store.feeds.count_by_status("error")
        total_episodes = await store.episodes.count_all()

    pct = (active_feeds / total_feeds * 100) if total_feeds > 0 else 0.0
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    batch_str = f"Batch {batch_num} of {total_batches}" if total_batches else f"Batch {batch_num}"

    content = f"""# Podcast Ingestion Progress Tracker

**Last Updated:** {now_str}  
**Database:** `{describe_database()}`

---

## 📊 Summary Statistics
| Metric | Value |
|---|---|
| **Total Shows Registered** | {total_feeds:,} |
| **Active / Synced Shows** | {active_feeds:,} ({pct:.1f}%) |
| **Pending / Discovered Shows** | {pending_feeds:,} |
| **Errored / Dead Shows** | {error_feeds:,} |
| **Total Episodes Saved** | {total_episodes:,} |
| **Current Batch** | {batch_str} |
| **Last Batch Synced Shows** | {batch_synced_shows} |
| **Last Batch New Episodes** | {batch_new_episodes:,} |

---

## 🕒 Recent Ingestion Log
```text
{chr(10).join(recent_logs[-30:]) if recent_logs else 'Ingestion in progress...'}
```

---

## ⚠️ Throttle & Error Monitoring
- **Status**: {"⚠️ Recent Error / Throttle detected: " + last_error if last_error else "🟢 Healthy - Running smoothly"}
- **Checkpointing**: Every podcast commits immediately to the configured database. Resumption resumes automatically from pending shows.
"""
    with open(progress_file, "w", encoding="utf-8") as f:
        f.write(content)


async def run_batch_ingest(
    batch_size: int = 25,
    max_batches: Optional[int] = None,
    delay_between_feeds: float = 0.35,
) -> Dict[str, Any]:
    """
    Ingests episodes one podcast at a time in batches, committing after each
    show and recording progress in the configured progress markdown file.

    XIN-38: thin entry point over :class:`FeedSyncOrchestrator` — sequential
    (``concurrency=1``), one batch per run with ``max_feeds=batch_size``,
    plus the batch-runner progress callbacks. The only orchestration left
    here is the batch loop and the markdown progress file.
    """
    await init_db()
    sync_service = FeedSyncService(queue_driver=get_queue_driver())
    logs: List[str] = load_existing_logs()
    last_error_msg: Optional[str] = None

    # Initial progress file write
    await write_progress_file(0, max_batches, 0, 0, logs if logs else ["Ingestion job resumed."])

    def on_feed_synced(title: str, new_episode_count: int) -> None:
        nonlocal last_error_msg
        msg = f"[{datetime.now().strftime('%H:%M:%S')}] OK: Synced {new_episode_count:3d} eps -> {title[:45]}"
        logger.info(msg)
        logs.append(msg)
        last_error_msg = None

    def on_feed_failed(label: str, err: Exception) -> None:
        nonlocal last_error_msg
        status_code = fetch_status_code(err)
        if status_code == 429:
            last_error_msg = f"HTTP 429 Throttled on {label}. Backing off 5s..."
            logger.warning(last_error_msg)
            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] THROTTLE (429): {label} - Backing off 5s")
        else:
            status_label = f"HTTP {status_code}" if status_code else type(err).__name__
            last_error_msg = f"{status_label} on {label}"
            logger.warning(last_error_msg)
            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] FAIL ({status_label}): {label}")

    # Persistent HTTP client with browser User-Agent
    transport = httpx.AsyncHTTPTransport(retries=2)
    async with httpx.AsyncClient(
        transport=transport,
        timeout=25.0,
        follow_redirects=True,
        headers={"User-Agent": "TunedIn/1.0 (+https://github.com/tunedin; podcast crawler)"},
    ) as client:

        batch_idx = 0
        while True:
            batch_idx += 1
            if max_batches and batch_idx > max_batches:
                break

            policy = SyncPolicy(
                concurrency=1,
                delay_between_feeds=delay_between_feeds,
                max_feeds=batch_size,
                auto_queue_episodes=get_auto_queue_episodes(),
                on_feed_synced=on_feed_synced,
                on_feed_failed=on_feed_failed,
            )
            summary = await FeedSyncOrchestrator(
                sync_service=sync_service,
                policy=policy,
                # Same session source the orchestrator defaults to; passed
                # explicitly so tests can substitute it via this module.
                session_factory=session_scope,
                client=client,
            ).run()

            if summary["total_feeds_processed"] == 0:
                logger.info("No more pending feeds to ingest. All shows synced!")
                logs.append("All pending podcast shows have been processed.")
                await write_progress_file(batch_idx, max_batches or batch_idx, 0, 0, logs)
                break

            # Checkpoint progress after batch (XIN-129: the per-feed write
            # ran 5 count queries + rewrote the markdown file after EVERY
            # feed; batch-boundary writes are sufficient).
            await write_progress_file(
                batch_num=batch_idx,
                total_batches=max_batches,
                batch_synced_shows=summary["total_synced"],
                batch_new_episodes=summary["total_episodes_saved"],
                recent_logs=logs,
                last_error=last_error_msg,
            )
            logger.info(
                "Batch %d complete: %d shows synced, %d episodes saved.",
                batch_idx,
                summary["total_synced"],
                summary["total_episodes_saved"],
            )

    return {
        "batches_completed": batch_idx,
        "logs_count": len(logs),
    }


def main() -> None:
    """Entry point for the batch runner CLI."""
    # XIN-63: configure logging at the entry point, not at import time.
    configure_logging()

    import argparse

    parser = argparse.ArgumentParser(description="One-at-a-time Batch Podcast Episode Ingest Runner")
    parser.add_argument("--batch-size", type=int, default=25, help="Number of podcasts per batch (default: 25)")
    parser.add_argument("--batches", type=int, default=None, help="Maximum number of batches to run (default: all)")
    parser.add_argument("--delay", type=float, default=0.35, help="Delay in seconds between feeds (default: 0.35)")

    args = parser.parse_args()
    asyncio.run(
        run_batch_ingest(
            batch_size=args.batch_size,
            max_batches=args.batches,
            delay_between_feeds=args.delay,
        )
    )


if __name__ == "__main__":
    main()
