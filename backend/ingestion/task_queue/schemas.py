"""Shared task payload schemas (XIN-41).

The PROCESS_EPISODE contract used to be defined in three disagreeing places:
the dict built by ``FeedSyncService._enqueue_episode_tasks``, the
``{'task_type': ..., 'payload': {...}}`` envelope ``GCPCloudTasksDriver``
POSTs, and the ad-hoc Pydantic model on the worker webhook. This module is
the single definition both sides validate against.

Wire format
-----------
Drivers POST the *envelope* (:class:`TaskEnvelope`) to the worker webhook::

    {"task_type": "PROCESS_EPISODE", "payload": {<ProcessEpisodePayload>}}

``LocalInMemoryDriver`` delivers the inner payload dict to registered
handlers. The webhook (``POST /api/worker/process-episode``) currently
validates the inner payload; when the real worker lands it should accept
the envelope (or the driver should unwrap) — see the webhook's docstring.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class ProcessEpisodePayload(BaseModel):
    """Inner payload for a PROCESS_EPISODE task.

    Built by ``FeedSyncService._enqueue_episode_tasks`` (the producer) and
    validated by the worker webhook (the consumer); the producer/consumer
    contract test pins that they agree.
    """

    schema_version: str = Field(
        default="1",
        description="Payload schema version; bump on incompatible change.",
    )
    episode_id: str
    feed_id: str
    title: str
    audio_url: str
    transcript_url: str | None = Field(
        default=None,
        description="Episode transcript URL when the feed provides one.",
    )


class TaskEnvelope(BaseModel):
    """Wire envelope drivers POST to the worker webhook (XIN-41).

    ``GCPCloudTasksDriver.enqueue`` wraps the inner payload exactly like
    this before POSTing to ``WORKER_WEBHOOK_URL``.
    """

    task_type: str
    payload: ProcessEpisodePayload


PROCESS_EPISODE_TASK_TYPE = "PROCESS_EPISODE"


__all__ = [
    "PROCESS_EPISODE_TASK_TYPE",
    "ProcessEpisodePayload",
    "TaskEnvelope",
]
