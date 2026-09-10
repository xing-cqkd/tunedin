"""XIN-130: coverage for batch_runner (run_batch_ingest, write_progress_file,
load_existing_logs, and the 429 backoff branch)."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

import backend.ingestion.batch_runner as br


class _FakeFeedsRepo:
    def __init__(self, pending=()):
        self._pending = list(pending)

    async def list_by_statuses(self, statuses, limit=None):
        feeds = list(self._pending)
        return feeds[:limit] if limit else feeds

    async def count_all(self):
        return 10

    async def count_by_status(self, status):
        return {"active": 7, "error": 1}.get(status, 0)

    async def count_by_statuses(self, statuses):
        return 2


class _FakeEpisodesRepo:
    async def count_all(self):
        return 100


class _FakeStore:
    def __init__(self, pending=()):
        self.feeds = _FakeFeedsRepo(pending)
        self.episodes = _FakeEpisodesRepo()


def _patch_common(monkeypatch, tmp_path, pending=()):
    """Patch batch_runner's DB/settings surface with fakes."""
    store = _FakeStore(pending)

    @asynccontextmanager
    async def fake_session_scope():
        yield store

    monkeypatch.setattr(br, "session_scope", fake_session_scope)
    monkeypatch.setattr(br, "init_db", AsyncMock())
    monkeypatch.setattr(br, "get_auto_queue_episodes", lambda: 0)
    monkeypatch.setattr(br, "get_queue_driver", lambda: SimpleNamespace())
    monkeypatch.setattr(br, "describe_database", lambda: "testdb")
    monkeypatch.setattr(br, "PROGRESS_FILE", tmp_path / "progress.md")
    return store


def test_load_existing_logs_no_file(tmp_path, monkeypatch):
    monkeypatch.setattr(br, "PROGRESS_FILE", tmp_path / "missing.md")
    assert br.load_existing_logs() == []


def test_load_existing_logs_parses_block(tmp_path, monkeypatch):
    progress = tmp_path / "progress.md"
    progress.write_text(
        "# Tracker\n```text\nline1\nIngestion in progress...\nline2\n```\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(br, "PROGRESS_FILE", progress)
    assert br.load_existing_logs() == ["line1", "line2"]


@pytest.mark.asyncio
async def test_write_progress_file_writes_stats(tmp_path, monkeypatch):
    _patch_common(monkeypatch, tmp_path)
    await br.write_progress_file(
        batch_num=1,
        total_batches=2,
        batch_synced_shows=3,
        batch_new_episodes=40,
        recent_logs=["log line"],
        last_error=None,
    )
    content = (tmp_path / "progress.md").read_text(encoding="utf-8")
    assert "Total Shows Registered" in content
    assert "Batch 1 of 2" in content
    assert "Healthy - Running smoothly" in content
    assert "log line" in content


@pytest.mark.asyncio
async def test_run_batch_ingest_no_pending(tmp_path, monkeypatch):
    _patch_common(monkeypatch, tmp_path)
    result = await br.run_batch_ingest(batch_size=5, max_batches=2)
    assert result["batches_completed"] == 1
    content = (tmp_path / "progress.md").read_text(encoding="utf-8")
    assert "All pending podcast shows have been processed." in content


@pytest.mark.asyncio
async def test_run_batch_ingest_processes_feed(tmp_path, monkeypatch):
    feed = SimpleNamespace(
        feed_id="f1", title="Show One", rss_url="https://one.example.com/feed.xml"
    )
    _patch_common(monkeypatch, tmp_path, pending=[feed])

    class FakeService:
        def __init__(self, queue_driver=None):
            pass

        async def sync_podcast_episodes(
            self, store, feed_or_id_or_url, client=None, auto_queue_episodes=0
        ):
            return feed, [SimpleNamespace(), SimpleNamespace()]

    monkeypatch.setattr(br, "FeedIngestionService", FakeService)
    monkeypatch.setattr(br.asyncio, "sleep", AsyncMock())

    result = await br.run_batch_ingest(
        batch_size=5, max_batches=1, delay_between_feeds=0
    )
    # Note: batches_completed counts the final break-check iteration too
    # (existing behavior: max_batches=1 -> 2).
    assert result["batches_completed"] == 2
    content = (tmp_path / "progress.md").read_text(encoding="utf-8")
    # Batch-boundary checkpoint records the synced show + 2 episodes
    assert "Last Batch New Episodes" in content


@pytest.mark.asyncio
async def test_run_batch_ingest_429_backoff(tmp_path, monkeypatch):
    feed = SimpleNamespace(
        feed_id="f1", title="Show One", rss_url="https://one.example.com/feed.xml"
    )
    _patch_common(monkeypatch, tmp_path, pending=[feed])

    req = httpx.Request("GET", "https://one.example.com/feed.xml")
    err429 = httpx.HTTPStatusError(
        "429", request=req, response=httpx.Response(429, request=req)
    )

    class FakeService:
        def __init__(self, queue_driver=None):
            pass

        async def sync_podcast_episodes(
            self, store, feed_or_id_or_url, client=None, auto_queue_episodes=0
        ):
            raise err429

    monkeypatch.setattr(br, "FeedIngestionService", FakeService)
    sleep_mock = AsyncMock()
    monkeypatch.setattr(br.asyncio, "sleep", sleep_mock)

    result = await br.run_batch_ingest(
        batch_size=5, max_batches=1, delay_between_feeds=0
    )
    # Note: batches_completed counts the final break-check iteration too
    # (existing behavior: max_batches=1 -> 2).
    assert result["batches_completed"] == 2
    # 429 triggers the 5s backoff (not the per-feed delay)
    sleep_mock.assert_any_call(5.0)
    content = (tmp_path / "progress.md").read_text(encoding="utf-8")
    assert "Recent Error / Throttle detected" in content
    assert "429" in content
