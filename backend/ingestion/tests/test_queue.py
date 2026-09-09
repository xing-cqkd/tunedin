from datetime import datetime, timezone
import pytest
from unittest.mock import AsyncMock, patch
from backend.ingestion.task_queue import (
    GCPCloudTasksDriver,
    LocalInMemoryDriver,
    get_queue_driver,
)
from backend.ingestion.service import FeedIngestionService
from backend.ingestion.models import ParsedEpisode, FeedParseResult, ParsedFeedMetadata
from backend.persistence.models.feed import Feed


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
    service = FeedIngestionService(queue_driver=queue_driver)

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
        feed, episodes = await service.sync_podcast_episodes(
            db=in_memory_session,
            feed_or_id_or_url="https://example.com/queue_test.rss",
            auto_queue_episodes=2,
        )

    assert len(episodes) == 4
    # Should only have enqueued the latest 2 episodes
    assert len(queue_driver.tasks) == 2
    assert queue_driver.tasks[0].payload["title"] == "Episode 3"
    assert queue_driver.tasks[1].payload["title"] == "Episode 4"
