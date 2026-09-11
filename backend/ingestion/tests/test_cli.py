"""XIN-130: coverage for the ingestion CLI (run_crawl, run_sync_only,
show_status, run_init_db, and argparse wiring)."""

import importlib
import logging
import sys
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

import backend.ingestion.cli as cli
from backend.ingestion.crawler import DEFAULT_TOPICS


class _FakeFeedsRepo:
    async def count_all(self):
        return 10

    async def count_by_status(self, status):
        return {"discovered": 3, "active": 7}.get(status, 0)


class _FakeEpisodesRepo:
    async def count_all(self):
        return 100

    async def count_unprocessed(self):
        return 40


class _FakeStore:
    def __init__(self):
        self.feeds = _FakeFeedsRepo()
        self.episodes = _FakeEpisodesRepo()


@pytest.fixture
def fake_session_scope(monkeypatch):
    @asynccontextmanager
    async def _scope():
        yield _FakeStore()

    monkeypatch.setattr(cli, "session_scope", _scope)
    monkeypatch.setattr(cli, "describe_database", lambda: "testdb")


@pytest.fixture
def fake_crawler_cls(monkeypatch):
    created = {}

    class _FakeCrawler:
        def __init__(self, request_delay=0.4):
            created["instance"] = self
            self.calls = []

        async def crawl_top_charts(
            self, store, countries, limit_per_chart=100, on_progress=None
        ):
            self.calls.append(
                (
                    "crawl_top_charts",
                    {"countries": list(countries), "limit_per_chart": limit_per_chart},
                )
            )
            return {"unique_saved": 2}

        async def crawl_topics(
            self,
            store,
            topics,
            limit_per_topic=200,
            country="us",
            min_episodes=None,
            on_progress=None,
        ):
            self.calls.append(
                (
                    "crawl_topics",
                    {
                        "topics": list(topics),
                        "limit_per_topic": limit_per_topic,
                        "min_episodes": min_episodes,
                    },
                )
            )
            return {"unique_saved": 3}

        async def sync_episodes_concurrently(
            self, concurrency=5, max_feeds=None, on_feed_synced=None
        ):
            self.calls.append(
                (
                    "sync_episodes_concurrently",
                    {"concurrency": concurrency, "max_feeds": max_feeds},
                )
            )
            return {"synced": 1, "episodes_saved": 5}

    monkeypatch.setattr(cli, "PodcastCrawler", _FakeCrawler)
    return created


@pytest.mark.asyncio
async def test_run_init_db(monkeypatch, capsys):
    monkeypatch.setattr(cli, "init_db", AsyncMock())
    monkeypatch.setattr(cli, "describe_database", lambda: "testdb")
    await cli.run_init_db()
    assert "Database ready: testdb" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_show_status(fake_session_scope, capsys):
    await cli.show_status()
    out = capsys.readouterr().out
    assert "Total Shows / Feeds     : 10" in out
    assert "Active (Synced)     : 7" in out
    assert "Total Episodes Saved    : 100" in out
    assert "Ready for LLM Read  : 40" in out


@pytest.mark.asyncio
async def test_run_crawl_topics_uses_default_topics(
    fake_session_scope, fake_crawler_cls, monkeypatch
):
    monkeypatch.setattr(cli, "init_db", AsyncMock())
    show_status_mock = AsyncMock()
    monkeypatch.setattr(cli, "show_status", show_status_mock)

    await cli.run_crawl(mode="topics")

    crawler = fake_crawler_cls["instance"]
    crawl_calls = [c for c in crawler.calls if c[0] == "crawl_topics"]
    assert len(crawl_calls) == 1
    assert crawl_calls[0][1]["topics"] == list(DEFAULT_TOPICS)
    assert crawl_calls[0][1]["limit_per_topic"] == 25
    assert crawl_calls[0][1]["min_episodes"] is None
    # Episode sync runs by default
    sync_calls = [c for c in crawler.calls if c[0] == "sync_episodes_concurrently"]
    assert sync_calls[0][1] == {"concurrency": 5, "max_feeds": None}
    show_status_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_crawl_charts_no_sync(
    fake_session_scope, fake_crawler_cls, monkeypatch
):
    monkeypatch.setattr(cli, "init_db", AsyncMock())
    monkeypatch.setattr(cli, "show_status", AsyncMock())

    await cli.run_crawl(
        mode="charts", countries=["us", "gb"], limit=10, sync_episodes=False
    )

    crawler = fake_crawler_cls["instance"]
    chart_calls = [c for c in crawler.calls if c[0] == "crawl_top_charts"]
    assert len(chart_calls) == 1
    assert chart_calls[0][1] == {"countries": ["us", "gb"], "limit_per_chart": 10}
    assert not any(c[0] == "crawl_topics" for c in crawler.calls)
    assert not any(c[0] == "sync_episodes_concurrently" for c in crawler.calls)


@pytest.mark.asyncio
async def test_run_crawl_explicit_topics(
    fake_session_scope, fake_crawler_cls, monkeypatch
):
    monkeypatch.setattr(cli, "init_db", AsyncMock())
    monkeypatch.setattr(cli, "show_status", AsyncMock())

    await cli.run_crawl(
        mode="topics", topics=["AI", "Tech"], min_episodes=20, sync_episodes=False
    )

    crawler = fake_crawler_cls["instance"]
    crawl_calls = [c for c in crawler.calls if c[0] == "crawl_topics"]
    assert crawl_calls[0][1]["topics"] == ["AI", "Tech"]
    assert crawl_calls[0][1]["min_episodes"] == 20


@pytest.mark.asyncio
async def test_run_sync_only(monkeypatch):
    """run_sync_only is a thin entry point over FeedSyncOrchestrator (XIN-38)."""
    monkeypatch.setattr(cli, "init_db", AsyncMock())
    show_status_mock = AsyncMock()
    monkeypatch.setattr(cli, "show_status", show_status_mock)

    created = {}

    class _FakeOrchestrator:
        def __init__(self, sync_service=None, policy=None, **kwargs):
            created["sync_service"] = sync_service
            created["policy"] = policy

        async def run(self):
            return {
                "total_feeds_processed": 2,
                "total_synced": 2,
                "total_episodes_saved": 9,
                "failed_count": 0,
                "failed_feed_ids": [],
                "skipped_backoff": 0,
            }

    monkeypatch.setattr(cli, "FeedSyncOrchestrator", _FakeOrchestrator)

    await cli.run_sync_only(concurrency=3, max_feeds=7)

    policy = created["policy"]
    assert policy.concurrency == 3
    assert policy.max_feeds == 7
    assert isinstance(created["sync_service"], cli.FeedSyncService)
    show_status_mock.assert_awaited_once()


def test_main_crawl_wiring(monkeypatch):
    run_crawl_mock = AsyncMock()
    monkeypatch.setattr(cli, "run_crawl", run_crawl_mock)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "crawl", "--mode", "charts", "--no-sync-episodes", "--limit", "10"],
    )
    cli.main()
    run_crawl_mock.assert_awaited_once_with(
        mode="charts",
        topics=None,
        countries=None,
        limit=10,
        min_episodes=None,
        sync_episodes=False,
        concurrency=5,
    )


def test_main_crawl_topics_list_parsing(monkeypatch):
    run_crawl_mock = AsyncMock()
    monkeypatch.setattr(cli, "run_crawl", run_crawl_mock)
    monkeypatch.setattr(
        sys, "argv", ["prog", "crawl", "--topics", "AI, Tech ,Science"]
    )
    cli.main()
    assert run_crawl_mock.await_args.kwargs["topics"] == ["AI", "Tech", "Science"]


def test_main_sync_wiring(monkeypatch):
    run_sync_mock = AsyncMock()
    monkeypatch.setattr(cli, "run_sync_only", run_sync_mock)
    monkeypatch.setattr(
        sys, "argv", ["prog", "sync", "--concurrency", "2", "--max-feeds", "9"]
    )
    cli.main()
    run_sync_mock.assert_awaited_once_with(concurrency=2, max_feeds=9)


def test_main_init_db_wiring(monkeypatch):
    run_init_db_mock = AsyncMock()
    monkeypatch.setattr(cli, "run_init_db", run_init_db_mock)
    monkeypatch.setattr(sys, "argv", ["prog", "init-db"])
    cli.main()
    run_init_db_mock.assert_awaited_once_with()


def test_main_status_wiring(monkeypatch):
    show_status_mock = AsyncMock()
    monkeypatch.setattr(cli, "show_status", show_status_mock)
    monkeypatch.setattr(sys, "argv", ["prog", "status"])
    cli.main()
    show_status_mock.assert_awaited_once_with()


def test_import_does_not_configure_logging():
    """XIN-63: importing cli must not touch the root logger."""
    for h in list(logging.root.handlers):
        logging.root.removeHandler(h)
    importlib.reload(cli)
    assert logging.root.handlers == []


def test_main_configures_logging(monkeypatch):
    """XIN-63: cli main() configures logging at the entry point."""
    for h in list(logging.root.handlers):
        logging.root.removeHandler(h)
    monkeypatch.setattr(sys, "argv", ["prog", "status"])
    monkeypatch.setattr(cli, "show_status", AsyncMock())
    cli.main()
    assert logging.root.handlers, "main() should configure root logging"
    for h in list(logging.root.handlers):
        logging.root.removeHandler(h)


def test_positive_int_accepts_valid():
    assert cli._positive_int("1") == 1
    assert cli._positive_int("16") == 16


def test_main_crawl_concurrency_zero_rejected(monkeypatch):
    """XIN-76: --concurrency=0 must fail at the CLI, not deadlock."""
    monkeypatch.setattr(cli, "run_crawl", AsyncMock())
    monkeypatch.setattr(sys, "argv", ["prog", "crawl", "--concurrency", "0"])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 2


def test_main_crawl_negative_concurrency_rejected(monkeypatch):
    monkeypatch.setattr(cli, "run_crawl", AsyncMock())
    monkeypatch.setattr(sys, "argv", ["prog", "crawl", "--concurrency", "-3"])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 2


def test_main_sync_concurrency_zero_rejected(monkeypatch):
    """XIN-76: --concurrency=0 must fail at the CLI, not deadlock."""
    monkeypatch.setattr(cli, "run_sync_only", AsyncMock())
    monkeypatch.setattr(sys, "argv", ["prog", "sync", "--concurrency", "0"])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 2
