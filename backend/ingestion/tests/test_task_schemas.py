"""Producer/consumer contract tests for the PROCESS_EPISODE payload (XIN-41).

The ingestion service (producer) and the worker webhook (consumer) used to
define the task payload independently; this pins that they agree on the
single shared schema in ``backend.ingestion.task_queue.schemas``.
"""

import uuid

import pytest

from backend.ingestion.service import FeedSyncService
from backend.ingestion.task_queue.schemas import (
    PROCESS_EPISODE_TASK_TYPE,
    ProcessEpisodePayload,
    TaskEnvelope,
)
from backend.persistence.models.episode import Episode
from backend.persistence.models.feed import Feed


def _episode(**overrides) -> Episode:
    kwargs = {
        "episode_id": uuid.uuid4(),
        "title": "Ep",
        "audio_url": "https://example.com/audio.mp3",
        "transcript_url": None,
    }
    kwargs.update(overrides)
    return Episode(**kwargs)


def _feed() -> Feed:
    return Feed(
        feed_id=uuid.uuid4(),
        rss_url="https://example.com/feed.xml",
        title="Show",
    )


def test_producer_payload_validates_as_shared_schema() -> None:
    """The producer's payload round-trips through the shared schema."""
    payload = FeedSyncService._process_episode_payload(_feed(), _episode())
    assert isinstance(payload, ProcessEpisodePayload)
    assert payload.schema_version == "1"
    # What the driver enqueues (model_dump) is what the consumer receives.
    received = ProcessEpisodePayload.model_validate(payload.model_dump())
    assert received == payload


def test_optional_transcript_url() -> None:
    """transcript_url may be absent (None); the old worker model required it."""
    payload = FeedSyncService._process_episode_payload(
        _feed(), _episode(transcript_url=None)
    )
    assert payload.transcript_url is None
    ProcessEpisodePayload.model_validate(payload.model_dump())


def test_envelope_wraps_inner_payload() -> None:
    """Drivers wrap the inner payload as {task_type, payload} (XIN-41)."""
    payload = FeedSyncService._process_episode_payload(_feed(), _episode())
    envelope = TaskEnvelope(
        task_type=PROCESS_EPISODE_TASK_TYPE, payload=payload.model_dump()
    )
    assert envelope.task_type == "PROCESS_EPISODE"
    assert envelope.payload == payload


def test_worker_webhook_imports_shared_schema() -> None:
    """The consumer validates with the same class the producer builds."""
    fastapi = pytest.importorskip("fastapi")  # noqa: F841
    from backend.api import worker

    assert worker.ProcessEpisodePayload is ProcessEpisodePayload
