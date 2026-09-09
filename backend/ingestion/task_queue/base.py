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
    """Abstract interface for background task queue drivers."""

    def __init__(self):
        self._handlers: Dict[str, TaskHandler] = {}

    def register_handler(self, task_type: str, handler: TaskHandler) -> None:
        """Registers an async handler function for a specific task type."""
        self._handlers[task_type] = handler

    def get_handler(self, task_type: str) -> Optional[TaskHandler]:
        """Retrieves registered handler for task_type."""
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
