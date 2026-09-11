import argparse
import asyncio
import logging
import sys
from typing import List, Optional

from backend.config import configure_logging
from backend.ingestion.crawler import DEFAULT_TOPICS, PodcastCrawler
from settings import describe_database, get_crawler_countries, init_db, session_scope

logger = logging.getLogger("ingestion_cli")


async def run_init_db() -> None:
    """Provisions the configured database (creates tables / the DynamoDB table)."""
    await init_db()
    print(f"✅ Database ready: {describe_database()}")


async def show_status() -> None:
    """Displays current catalog counts in the configured database."""
    async with session_scope() as store:
        feed_total = await store.feeds.count_all()
        feed_discovered = await store.feeds.count_by_status("discovered")
        feed_active = await store.feeds.count_by_status("active")
        ep_total = await store.episodes.count_all()
        ep_unprocessed = await store.episodes.count_unprocessed()

    print("\n" + "=" * 55)
    print(" 📊 TunedIn Podcast Ingestion Database Status")
    print("=" * 55)
    print(f" Database                : {describe_database()}")
    print(f" Total Shows / Feeds     : {feed_total}")
    print(f"   - Discovered (Pending): {feed_discovered}")
    print(f"   - Active (Synced)     : {feed_active}")
    print(f" Total Episodes Saved    : {ep_total}")
    print(f"   - Ready for LLM Read  : {ep_unprocessed}")
    print("=" * 55 + "\n")


async def run_crawl(
    mode: str = "topics",
    topics: Optional[List[str]] = None,
    countries: Optional[List[str]] = None,
    limit: int = 25,
    min_episodes: Optional[int] = None,
    sync_episodes: bool = True,
    concurrency: int = 5,
) -> None:
    """Executes podcast harvesting and optional episode downloading."""
    await init_db()
    crawler = PodcastCrawler(request_delay=0.4)

    def _progress(item_name: str, count: int) -> None:
        print(f"  [+] Discovered & saved {count:3d} shows for: {item_name}")

    async with session_scope() as store:
        if mode in ("charts", "all"):
            c_list = countries or get_crawler_countries()
            print(f"\n🚀 Harvesting Top Charts across {c_list} (limit {limit} per country)...")
            chart_stats = await crawler.crawl_top_charts(
                store=store,
                countries=c_list,
                limit_per_chart=limit,
                on_progress=_progress,
            )
            print(f"✅ Top Charts Harvest complete: {chart_stats['unique_saved']} unique shows saved.")

        if mode in ("topics", "all"):
            t_list = topics or DEFAULT_TOPICS
            print(f"\n🚀 Harvesting Topic Taxonomy ({len(t_list)} topics, limit {limit} per topic, min_episodes={min_episodes})...")
            topic_stats = await crawler.crawl_topics(
                store=store,
                topics=t_list,
                limit_per_topic=limit,
                min_episodes=min_episodes,
                on_progress=_progress,
            )
            print(f"✅ Topic Harvest complete: {topic_stats['unique_saved']} unique shows saved.")

    if sync_episodes:
        print(f"\n⚡ Concurrently downloading episodes for discovered shows (concurrency={concurrency})...")
        def _feed_synced(title: str, ep_count: int) -> None:
            print(f"  [>] Synced {ep_count:3d} episodes: {title[:45]}")

        sync_stats = await crawler.sync_episodes_concurrently(
            concurrency=concurrency,
            on_feed_synced=_feed_synced,
        )
        print(f"✅ Episode Sync complete: {sync_stats['episodes_saved']} episodes saved across {sync_stats['synced']} shows.")

    await show_status()


async def run_sync_only(concurrency: int = 5, max_feeds: Optional[int] = None) -> None:
    """Downloads episodes for existing discovered/pending feeds in the database."""
    await init_db()
    crawler = PodcastCrawler()
    print(f"\n⚡ Syncing episodes for pending feeds (concurrency={concurrency}, max_feeds={max_feeds})...")
    def _feed_synced(title: str, ep_count: int) -> None:
        print(f"  [>] Synced {ep_count:3d} episodes: {title[:45]}")

    stats = await crawler.sync_episodes_concurrently(
        concurrency=concurrency,
        max_feeds=max_feeds,
        on_feed_synced=_feed_synced,
    )
    print(f"\n✅ Synced {stats['episodes_saved']} episodes across {stats['synced']} feeds.")
    await show_status()


def _positive_int(value: str) -> int:
    """argparse type for worker counts: must be an integer >= 1 (XIN-76).

    A bare ``Semaphore(0)`` downstream deadlocks the episode sync, so 0 and
    negative values are rejected at the CLI instead of hanging.
    """
    try:
        parsed = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"must be an integer >= 1, got {value!r}"
        ) from None
    if parsed < 1:
        raise argparse.ArgumentTypeError(
            f"must be >= 1, got {parsed} (--concurrency=0 deadlocks the episode sync)"
        )
    return parsed


def main() -> None:
    # XIN-63: configure logging at the entry point, not at import time.
    configure_logging()

    parser = argparse.ArgumentParser(description="TunedIn Podcast Ingestion & Crawling CLI")
    subparsers = parser.add_subparsers(dest="command", help="CLI command")

    # Command: init-db
    subparsers.add_parser("init-db", help="Provision the configured database (create tables)")

    # Command: status
    subparsers.add_parser("status", help="Show current catalog statistics")

    # Command: crawl
    crawl_parser = subparsers.add_parser("crawl", help="Discover podcasts and save to database")
    crawl_parser.add_argument(
        "--mode",
        choices=["topics", "charts", "all"],
        default="topics",
        help="Harvesting mode: topics, charts, or all (default: topics)",
    )
    crawl_parser.add_argument(
        "--topics",
        type=str,
        help="Comma-separated list of search topics (default: AI, Tech, Science topics)",
    )
    crawl_parser.add_argument(
        "--countries",
        type=str,
        help="Comma-separated storefront country codes (e.g. us,gb,ca; default: ingestion.crawler_countries from settings.yaml)",
    )
    crawl_parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum results per query (default: 25)",
    )
    crawl_parser.add_argument(
        "--min-episodes",
        type=int,
        default=None,
        help="Filter for popular/established shows with at least N episodes (e.g. 20)",
    )
    crawl_parser.add_argument(
        "--no-sync-episodes",
        action="store_true",
        help="Discover shows only without downloading episodes immediately",
    )
    crawl_parser.add_argument(
        "--concurrency",
        type=_positive_int,
        default=5,
        help="Concurrent episode download workers, >= 1 (default: 5)",
    )

    # Command: sync
    sync_parser = subparsers.add_parser("sync", help="Download episodes for pending feeds")
    sync_parser.add_argument(
        "--concurrency",
        type=_positive_int,
        default=5,
        help="Concurrent episode download workers, >= 1 (default: 5)",
    )
    sync_parser.add_argument(
        "--max-feeds",
        type=int,
        default=None,
        help="Maximum feeds to sync",
    )

    args = parser.parse_args()

    if args.command == "init-db":
        asyncio.run(run_init_db())
    elif args.command == "status":
        asyncio.run(show_status())
    elif args.command == "crawl":
        topics_list = [t.strip() for t in args.topics.split(",")] if args.topics else None
        countries_list = [c.strip() for c in args.countries.split(",")] if args.countries else None
        asyncio.run(
            run_crawl(
                mode=args.mode,
                topics=topics_list,
                countries=countries_list,
                limit=args.limit,
                min_episodes=args.min_episodes,
                sync_episodes=not args.no_sync_episodes,
                concurrency=args.concurrency,
            )
        )
    elif args.command == "sync":
        asyncio.run(run_sync_only(concurrency=args.concurrency, max_feeds=args.max_feeds))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
