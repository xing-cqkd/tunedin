"""Worker webhook endpoints (XIN-31).

The ingestion queue drivers deliver ``PROCESS_EPISODE`` tasks by POSTing to
``/api/worker/process-episode`` — this is the default ``WORKER_WEBHOOK_URL``
in ``backend/ingestion/task_queue/gcp.py``. This router accepts that payload
and validates it against the contract the sync service enqueues
(``FeedSyncService.sync_podcast_episodes_by_feed`` in
``backend/ingestion/service.py``).

The payload schema is shared (XIN-41): both the producer
(``FeedSyncService._process_episode_payload``) and this consumer validate
against ``ProcessEpisodePayload`` in
``backend/ingestion/task_queue/schemas.py``. Drivers wrap it in the
``TaskEnvelope`` (``{"task_type": ..., "payload": {...}}``) on the wire;
this endpoint currently validates the inner payload. When the real worker
lands, decide explicitly whether it accepts the envelope or the driver
unwraps before POSTing.

The transcription/insight worker itself does not exist yet, so the endpoint
returns ``501 Not Implemented`` with a clear message. This is a documented
placeholder — not dead code — so the queue driver's target URL resolves to
a real route once the worker is built. When the worker lands, its handler
goes here.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from backend.ingestion.task_queue.schemas import ProcessEpisodePayload

router = APIRouter(prefix="/api/worker")


@router.post("/process-episode")
async def process_episode(payload: ProcessEpisodePayload):
    """Accept a PROCESS_EPISODE task from the queue driver.

    Validates the payload, then 501s until the transcription/insight worker
    is implemented (XIN-31 follow-up).
    """
    raise HTTPException(
        status_code=501,
        detail=(
            "Episode worker not implemented yet (XIN-31). Payload validated "
            f"for episode {payload.episode_id}; no processing was performed."
        ),
    )
