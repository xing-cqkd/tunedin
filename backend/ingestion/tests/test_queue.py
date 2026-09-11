from datetime import datetime, timedelta, timezone
import sys
import types
import pytest
from unittest.mock import AsyncMock, patch
from backend.ingestion.task_queue import (
    GCPCloudTasksDriver,
    LocalInMemoryDriver,
    get_queue_driver,
)
import backend.ingestion.task_queue as task_queue_mod
from backend.ingestion.service import FeedSyncService
from backend.ingestion.models import ParsedEpisode, FeedParseResult, ParsedFeedMetadata
from backend.persistence.models.feed import Feed
from backend.persistence.sqlalchemy_store import SQLAlchemyStore


@pytest.fixture(autouse=True)
def _fresh_driver_cache():
    """Each test gets a hermetic driver-singleton cache (XIN-40)."""
    task_queue_mod._DRIVERS.clear()
    yield
    task_queue_mod._DRIVERS.clear()


@pytest.mark.asyncio
async def test_local_queue_enqueue():
    driver = LocalInMemoryDriver()
    task_id = await driver.enqueue(
        task_type="PROCESS_EPISODE",
        payload={"episode_id": "test-123", "audio_url": "https://test.com/audio.mp3"},
    )
    assert task_id is not None
    assert len(driver.tasks) == 1
    assert driver.tasks[0].task_type == "PROCESS_EPISODE"
    assert driver.tasks[0].payload["episode_id"] == "test-123"


@pytest.mark.asyncio
async def test_local_queue_process_with_handler():
    driver = LocalInMemoryDriver()
    handled_items = []

    async def mock_handler(payload):
        handled_items.append(payload["episode_id"])
        return f"processed {payload['episode_id']}"

    driver.register_handler("PROCESS_EPISODE", mock_handler)

    await driver.enqueue("PROCESS_EPISODE", {"episode_id": "ep-1"})
    await driver.enqueue("PROCESS_EPISODE", {"episode_id": "ep-2"})

    record = await driver.process_next()
    assert record["task_type"] == "PROCESS_EPISODE"
    assert record["result"] == "processed ep-1"
    assert handled_items == ["ep-1"]

    all_records = await driver.process_all()
    assert len(all_records) == 1
    assert all_records[0]["result"] == "processed ep-2"
    assert handled_items == ["ep-1", "ep-2"]


@pytest.mark.asyncio
async def test_local_queue_clear():
    driver = LocalInMemoryDriver()
    await driver.enqueue("TEST_TASK", {"foo": "bar"})
    assert len(driver.tasks) == 1
    driver.clear()
    assert len(driver.tasks) == 0
    res = await driver.process_next()
    assert res is None


def test_queue_driver_factory(monkeypatch):
    monkeypatch.delenv("TASK_QUEUE_DRIVER", raising=False)
    default_driver = get_queue_driver()
    assert isinstance(default_driver, LocalInMemoryDriver)

    local_driver = get_queue_driver("local")
    assert isinstance(local_driver, LocalInMemoryDriver)

    with pytest.raises(ValueError, match="Unknown TASK_QUEUE_DRIVER"):
        get_queue_driver("unsupported_driver")


def test_gcp_driver_configuration():
    gcp_driver = GCPCloudTasksDriver(
        project_id="test-proj",
        location="us-east1",
        queue_name="my-queue",
        target_url="https://api.example.com/worker",
        service_account_email="sa@test-proj.iam.gserviceaccount.com",
    )
    assert gcp_driver.project_id == "test-proj"
    assert gcp_driver.location == "us-east1"
    assert gcp_driver.queue_name == "my-queue"
    assert gcp_driver.target_url == "https://api.example.com/worker"
    assert gcp_driver.service_account_email == "sa@test-proj.iam.gserviceaccount.com"


from backend.persistence.models.base import Base
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


@pytest.fixture
async def in_memory_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session

    await engine.dispose()


@pytest.mark.asyncio
async def test_service_auto_queues_episodes(in_memory_session):
    queue_driver = LocalInMemoryDriver()
    service = FeedSyncService(queue_driver=queue_driver)

    mock_parse_result = FeedParseResult(
        metadata=ParsedFeedMetadata(
            title="Queued Test Show",
            rss_url="https://example.com/queue_test.rss",
        ),
        episodes=[
            ParsedEpisode(
                guid=f"queue-guid-{i}",
                title=f"Episode {i}",
                audio_url=f"https://example.com/ep{i}.mp3",
                transcript_url=f"https://example.com/ep{i}.json",
                published_at=datetime(2024, 1, i, 12, 0, tzinfo=timezone.utc),
            )
            for i in range(1, 5)
        ],
        total_feed_episodes=4,
        is_not_modified=False,
    )

    with patch.object(service.parser, "fetch_and_parse", new=AsyncMock(return_value=mock_parse_result)):
        feed, episodes = await service.sync_podcast_episodes_by_url(
            store=SQLAlchemyStore(lambda: in_memory_session),
            rss_url="https://example.com/queue_test.rss",
            auto_queue_episodes=2,
        )

    assert len(episodes) == 4
    # Should only have enqueued the latest 2 episodes
    assert len(queue_driver.tasks) == 2
    assert queue_driver.tasks[0].payload["title"] == "Episode 3"
    assert queue_driver.tasks[1].payload["title"] == "Episode 4"


# ---------------------------------------------------------------------------
# XIN-128 / XIN-130: delayed tasks, no-handler warning, GCP driver, factory
# ---------------------------------------------------------------------------
from types import SimpleNamespace


@pytest.mark.asyncio
async def test_process_next_no_handler_warns_and_records(caplog):
    driver = LocalInMemoryDriver()
    await driver.enqueue("UNKNOWN_TYPE", {"x": 1})
    with caplog.at_level("WARNING"):
        record = await driver.process_next()
    assert record is not None
    assert record["result"] is None
    assert record["error"] is None
    assert "No handler registered" in caplog.text


@pytest.mark.asyncio
async def test_enqueue_delay_not_processed_early():
    """XIN-128: a delayed task must not run before its schedule_time."""
    driver = LocalInMemoryDriver()
    handled = []

    async def handler(payload):
        handled.append(payload["episode_id"])
        return "ok"

    driver.register_handler("PROCESS_EPISODE", handler)
    await driver.enqueue("PROCESS_EPISODE", {"episode_id": "ep-1"}, in_seconds=3600)

    # Head task is not due: process_next returns None and leaves it queued
    assert await driver.process_next() is None
    assert handled == []
    assert driver._queue.qsize() == 1

    # Once due, it processes normally
    driver.tasks[0].schedule_time = datetime.now(timezone.utc) - timedelta(seconds=1)
    record = await driver.process_next()
    assert record is not None
    assert handled == ["ep-1"]


@pytest.mark.asyncio
async def test_process_all_skips_delayed_tasks():
    driver = LocalInMemoryDriver()
    handled = []

    async def handler(payload):
        handled.append(payload["episode_id"])
        return "ok"

    driver.register_handler("PROCESS_EPISODE", handler)
    await driver.enqueue("PROCESS_EPISODE", {"episode_id": "due-now"})
    await driver.enqueue("PROCESS_EPISODE", {"episode_id": "later"}, in_seconds=3600)

    results = await driver.process_all()
    assert [r["payload"]["episode_id"] for r in results] == ["due-now"]
    assert handled == ["due-now"]
    # The delayed task stays queued for a later pass
    assert driver._queue.qsize() == 1


@pytest.mark.asyncio
async def test_gcp_enqueue_with_mock_client(monkeypatch):
    from backend.ingestion.task_queue.gcp import GCPCloudTasksDriver

    # google-cloud-tasks/protobuf is not a project dependency; the driver
    # imports timestamp_pb2 lazily only when a delay is requested. Stub the
    # module so this test runs in clean test envs too.
    try:
        import google.protobuf.timestamp_pb2  # noqa: F401
    except ImportError:
        stub_pb2 = types.ModuleType("google.protobuf.timestamp_pb2")

        class _StubTimestamp:
            def FromDatetime(self, dt):
                self._dt = dt

        stub_pb2.Timestamp = _StubTimestamp
        monkeypatch.setitem(sys.modules, "google.protobuf", types.ModuleType("google.protobuf"))
        monkeypatch.setitem(sys.modules, "google.protobuf.timestamp_pb2", stub_pb2)

    class FakeTasksClient:
        def __init__(self):
            self.requests = []

        def queue_path(self, project, location, queue):
            return f"projects/{project}/locations/{location}/queues/{queue}"

        async def create_task(self, request):
            self.requests.append(request)
            return SimpleNamespace(name=request["parent"] + "/tasks/abc123")

    fake = FakeTasksClient()
    driver = GCPCloudTasksDriver(
        project_id="p",
        location="l",
        queue_name="q",
        target_url="https://worker.example.com/hook",
        # XIN-77: OIDC is required for non-localhost webhook URLs.
        service_account_email="sa@example.iam.gserviceaccount.com",
    )
    driver._client = fake

    task_id = await driver.enqueue("PROCESS_EPISODE", {"a": 1}, in_seconds=60)
    assert task_id == "abc123"
    req = fake.requests[0]
    assert req["parent"] == "projects/p/locations/l/queues/q"
    assert "schedule_time" in req["task"]
    assert req["task"]["http_request"]["url"] == "https://worker.example.com/hook"

    # Without a delay there is no schedule_time
    task_id2 = await driver.enqueue("PROCESS_EPISODE", {"a": 2})
    assert task_id2 == "abc123"
    assert "schedule_time" not in fake.requests[1]["task"]


def test_get_queue_driver_factory(monkeypatch):
    # XIN-77: constructing the GCP driver is fail-closed — the factory can
    # only build it with a securely configured webhook URL.
    monkeypatch.setenv("WORKER_WEBHOOK_URL", "https://worker.example.com/hook")
    monkeypatch.setenv(
        "GCP_SERVICE_ACCOUNT_EMAIL", "sa@example.iam.gserviceaccount.com"
    )
    assert isinstance(get_queue_driver("gcp"), GCPCloudTasksDriver)
    assert isinstance(get_queue_driver("local"), LocalInMemoryDriver)
    with pytest.raises(ValueError, match="Unknown TASK_QUEUE_DRIVER"):
        get_queue_driver("bogus")


# ---------------------------------------------------------------------------
# XIN-40: handler contract is real on the local driver, fail-fast on GCP;
# driver singletons share one lifecycle.
# XIN-77: GCP driver URL validation is fail-closed.
# ---------------------------------------------------------------------------


def test_driver_singletons_cached_consistently(monkeypatch):
    """XIN-40: both drivers share the same singleton lifecycle."""
    monkeypatch.setenv("WORKER_WEBHOOK_URL", "https://worker.example.com/hook")
    monkeypatch.setenv(
        "GCP_SERVICE_ACCOUNT_EMAIL", "sa@example.iam.gserviceaccount.com"
    )
    assert get_queue_driver("local") is get_queue_driver("local")
    assert get_queue_driver("gcp") is get_queue_driver("gcp")
    # Explicit selection and env selection resolve to the same instance.
    monkeypatch.setenv("TASK_QUEUE_DRIVER", "gcp")
    assert get_queue_driver() is get_queue_driver("gcp")


@pytest.mark.asyncio
async def test_local_handler_register_get_round_trip():
    """XIN-40: register_handler/get_handler are a real registry locally."""
    driver = LocalInMemoryDriver()

    async def handler(payload):
        return payload

    assert driver.get_handler("PROCESS_EPISODE") is None
    driver.register_handler("PROCESS_EPISODE", handler)
    assert driver.get_handler("PROCESS_EPISODE") is handler

    # And the registered handler actually fires through process_next.
    await driver.enqueue("PROCESS_EPISODE", {"episode_id": "ep-rt"})
    record = await driver.process_next()
    assert record["result"] == {"episode_id": "ep-rt"}


def _gcp_driver(**kwargs):
    kwargs.setdefault("project_id", "p")
    kwargs.setdefault("location", "l")
    kwargs.setdefault("queue_name", "q")
    return GCPCloudTasksDriver(**kwargs)


def test_gcp_driver_rejects_handler_registration():
    """XIN-40: in-process dispatch can never work on the GCP driver — it must
    fail fast instead of silently dropping the handler."""
    driver = _gcp_driver(
        target_url="https://worker.example.com/hook",
        service_account_email="sa@example.iam.gserviceaccount.com",
    )

    async def handler(payload):
        return payload

    with pytest.raises(NotImplementedError, match="in-process"):
        driver.register_handler("PROCESS_EPISODE", handler)
    with pytest.raises(NotImplementedError, match="in-process"):
        driver.get_handler("PROCESS_EPISODE")


def test_gcp_driver_refuses_default_localhost_without_opt_in(monkeypatch):
    """XIN-77: the default plaintext-http localhost target is refused unless
    explicitly opted in."""
    monkeypatch.delenv("WORKER_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("TASK_QUEUE_ALLOW_INSECURE_LOCAL", raising=False)
    with pytest.raises(ValueError, match="TASK_QUEUE_ALLOW_INSECURE_LOCAL"):
        _gcp_driver()


def test_gcp_driver_allows_localhost_with_explicit_opt_in(monkeypatch):
    """XIN-77: the opt-in env makes the dev-localhost default constructible."""
    monkeypatch.delenv("WORKER_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("TASK_QUEUE_ALLOW_INSECURE_LOCAL", "1")
    driver = _gcp_driver()
    assert driver.target_url == "http://localhost:8000/api/worker/process-episode"


def test_gcp_driver_rejects_plaintext_remote_url(monkeypatch):
    """XIN-77: non-localhost webhook URLs must use https."""
    monkeypatch.setenv("TASK_QUEUE_ALLOW_INSECURE_LOCAL", "1")
    with pytest.raises(ValueError, match="https"):
        _gcp_driver(target_url="http://worker.example.com/hook")


def test_gcp_driver_requires_oidc_for_remote_url():
    """XIN-77: non-localhost webhook URLs require OIDC auth."""
    with pytest.raises(ValueError, match="GCP_SERVICE_ACCOUNT_EMAIL"):
        _gcp_driver(target_url="https://worker.example.com/hook")


def test_gcp_driver_accepts_https_with_oidc():
    """XIN-77: https + service account is the happy path."""
    driver = _gcp_driver(
        target_url="https://worker.example.com/hook",
        service_account_email="sa@example.iam.gserviceaccount.com",
    )
    assert driver.target_url == "https://worker.example.com/hook"
