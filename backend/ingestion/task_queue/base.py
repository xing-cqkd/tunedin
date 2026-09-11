import abc
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine, Dict, Optional
from pydantic import BaseModel, Field


class EnqueuedTask(BaseModel):
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str
    payload: Dict[str, Any]
    enqueued_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    schedule_time: Optional[datetime] = None


TaskHandler = Callable[[Dict[str, Any]], Coroutine[Any, Any, Any]]


class TaskQueueDriver(abc.ABC):
    """Abstract interface for background task queue drivers.

    The driver has two responsibilities:

    * **enqueue** — durable, driver-agnostic task submission. Every driver
      must implement this.
    * **in-process handler dispatch** — :meth:`register_handler` /
      :meth:`get_handler` keep a registry of async callables keyed by
      task type. This registry is only meaningful for drivers that execute
      tasks in-process (e.g. :class:`LocalInMemoryDriver`, which dispatches
      via ``process_next``/``process_all``).

    Drivers whose delivery model is remote — tasks are POSTed to a worker
    webhook and executed elsewhere, e.g. :class:`GCPCloudTasksDriver` — can
    never honor in-process dispatch. Such drivers MUST override
    :meth:`register_handler`/:meth:`get_handler` to raise
    ``NotImplementedError`` with a clear message instead of silently
    ignoring registered handlers (XIN-40).
    """

    def __init__(self):
        self._handlers: Dict[str, TaskHandler] = {}

    def register_handler(self, task_type: str, handler: TaskHandler) -> None:
        """Registers an async handler function for a specific task type.

        The handler only fires on drivers that dispatch in-process. Remote
        drivers (e.g. GCP Cloud Tasks) raise ``NotImplementedError`` here —
        register the handler on the local driver, or consume the task via
        the worker webhook, instead.
        """
        self._handlers[task_type] = handler

    def get_handler(self, task_type: str) -> Optional[TaskHandler]:
        """Retrieves registered handler for task_type.

        Remote drivers raise ``NotImplementedError`` (see
        :meth:`register_handler`).
        """
        return self._handlers.get(task_type)

    @abc.abstractmethod
    async def enqueue(
        self,
        task_type: str,
        payload: Dict[str, Any],
        in_seconds: Optional[int] = None,
    ) -> str:
        """
        Enqueues a background task.
        Returns a unique task identifier string.
        """
        pass
