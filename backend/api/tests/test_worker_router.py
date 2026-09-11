"""Worker router tests (XIN-31).

The worker endpoint validates the payload contract and 501s until the
transcription/insight worker exists. The 501 is the *correct* behavior —
it proves the route is wired and the payload schema matches what the
ingestion service enqueues.
"""

from fastapi.testclient import TestClient

from backend.api import create_app


def _client() -> TestClient:
    # The worker endpoint never touches the store, so a dummy factory is fine.
    return TestClient(create_app(store_factory=lambda: None))


_VALID_PAYLOAD = {
    "episode_id": "123e4567-e89b-12d3-a456-426614174000",
    "feed_id": "123e4567-e89b-12d3-a456-426614174001",
    "title": "Test episode",
    "audio_url": "https://example.com/ep.mp3",
    "transcript_url": "",
}


def test_process_episode_validates_and_501s():
    r = _client().post("/api/worker/process-episode", json=_VALID_PAYLOAD)
    assert r.status_code == 501
    assert "not implemented" in r.json()["detail"].lower()


def test_process_episode_rejects_bad_payload():
    r = _client().post("/api/worker/process-episode", json={"episode_id": "x"})
    assert r.status_code == 422
