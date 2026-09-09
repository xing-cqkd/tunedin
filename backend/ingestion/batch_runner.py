import asyncio
from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
import httpx

from backend.ingestion.service import FeedIngestionService
from backend.ingestion.task_queue import get_queue_driver
from settings import describe_database, get_auto_queue_episodes, init_db, session_scope

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("batch_ingest")

PROGRESS_FILE = Path(__file__).parent / ".local_agents" / "podcast_ingest.md"


def load_existing_logs() -> List[str]:
    """Preserves recent log entries from the existing progress markdown file."""
    if not PROGRESS_FILE.exists():
        return []
    try:
        content = PROGRESS_FILE.read_text(encoding="utf-8")
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
    """Updates the podcast_ingest.md progress tracker in the .local_agents folder."""
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)

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
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        f.write(content)


async def run_batch_ingest(
    batch_size: int = 25,
    max_batches: Optional[int] = None,
    delay_between_feeds: float = 0.35,
) -> Dict[str, Any]:
    """
    Ingests episodes sequentially one podcast at a time in batches,
    committing to the configured database after each show and recording progress in .local_agents/podcast_ingest.md.
    """
    await init_db()
    auto_queue = get_auto_queue_episodes()
    service = FeedIngestionService(queue_driver=get_queue_driver())
    logs: List[str] = load_existing_logs()
    last_error_msg: Optional[str] = None

    # Initial progress file write
    await write_progress_file(0, max_batches, 0, 0, logs if logs else ["Ingestion job resumed."])

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

            # 1. Fetch next batch of pending feed IDs
            async with session_scope() as store:
                pending_batch = await store.feeds.list_by_statuses(
                    ["discovered", "pending"], limit=batch_size
                )

            if not pending_batch:
                logger.info("No more pending feeds to ingest. All shows synced!")
                logs.append("All pending podcast shows have been processed.")
                await write_progress_file(batch_idx, max_batches or batch_idx, 0, 0, logs)
                break

            logger.info("=== Starting Batch %d (%d shows) ===", batch_idx, len(pending_batch))
            batch_synced = 0
            batch_episodes = 0

            # 2. Process ONE podcast at a time
            for feed in pending_batch:
                feed_id, title, rss_url = feed.feed_id, feed.title, feed.rss_url
                show_label = f"{title[:40]} ({rss_url[:35]}...)"
                try:
                    async with session_scope() as store:
                        feed, new_eps = await service.sync_podcast_episodes(
                            store=store,
                            feed_or_id_or_url=feed_id,
                            client=client,
                            auto_queue_episodes=auto_queue,
                        )
                        batch_synced += 1
                        batch_episodes += len(new_eps)
                        msg = f"[{datetime.now().strftime('%H:%M:%S')}] OK: Synced {len(new_eps):3d} eps -> {feed.title[:45]}"
                        logger.info(msg)
                        logs.append(msg)
                        last_error_msg = None

                    # Politeness throttle
                    await asyncio.sleep(delay_between_feeds)

                except httpx.HTTPStatusError as e:
                    status_code = e.response.status_code
                    if status_code == 429:
                        last_error_msg = f"HTTP 429 Throttled on {show_label}. Backing off 5s..."
                        logger.warning(last_error_msg)
                        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] THROTTLE (429): {show_label} - Backing off 5s")
                        await asyncio.sleep(5.0)
                    else:
                        last_error_msg = f"HTTP {status_code} on {show_label}"
                        logger.warning(last_error_msg)
                        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] FAIL ({status_code}): {show_label}")
                except Exception as err:
                    last_error_msg = f"Error on {show_label}: {str(err)[:60]}"
                    logger.warning(last_error_msg)
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] FAIL: {show_label} ({str(err)[:40]})")

                # Live checkpoint progress after each podcast
                await write_progress_file(
                    batch_num=batch_idx,
                    total_batches=max_batches,
                    batch_synced_shows=batch_synced,
                    batch_new_episodes=batch_episodes,
                    recent_logs=logs,
                    last_error=last_error_msg,
                )

            # 3. Checkpoint progress after batch
            await write_progress_file(
                batch_num=batch_idx,
                total_batches=max_batches,
                batch_synced_shows=batch_synced,
                batch_new_episodes=batch_episodes,
                recent_logs=logs,
                last_error=last_error_msg,
            )
            logger.info(
                "Batch %d complete: %d shows synced, %d episodes saved.",
                batch_idx,
                batch_synced,
                batch_episodes,
            )

    return {
        "batches_completed": batch_idx,
        "logs_count": len(logs),
    }


if __name__ == "__main__":
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
