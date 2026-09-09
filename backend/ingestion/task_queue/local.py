import asyncio
from datetime import datetime, timedelta, timezone
import logging
from typing import Any, Dict, List, Optional
from backend.ingestion.task_queue.base import EnqueuedTask, TaskQueueDriver

logger = logging.getLogger(__name__)


class LocalInMemoryDriver(TaskQueueDriver):
    """
    In-memory async task queue driver for development, testing, and offline execution.
    Holds enqueued tasks in an internal queue and can process them with registered handlers.
    """

    def __init__(self):
        super().__init__()
        self.tasks: List[EnqueuedTask] = []
        self._queue: asyncio.Queue[EnqueuedTask] = asyncio.Queue()
        self.executed_tasks: List[Dict[str, Any]] = []

    async def enqueue(
        self,
        task_type: str,
        payload: Dict[str, Any],
        in_seconds: Optional[int] = None,
    ) -> str:
        sched_time = None
        if in_seconds and in_seconds > 0:
            sched_time = datetime.now(timezone.utc) + timedelta(seconds=in_seconds)

        task = EnqueuedTask(
            task_type=task_type,
            payload=payload,
            schedule_time=sched_time,
        )
        self.tasks.append(task)
        await self._queue.put(task)
        logger.debug("Local queue enqueued task %s (%s)", task.task_id, task_type)
        return task.task_id

    async def process_next(self) -> Optional[Dict[str, Any]]:
        """Processes the next task from the queue using its registered handler."""
        if self._queue.empty():
            return None
        task = await self._queue.get()
        handler = self.get_handler(task.task_type)
        result = None
        error = None
        if handler:
            try:
                result = await handler(task.payload)
            except Exception as e:
                logger.error("Error executing task %s: %s", task.task_id, str(e))
                error = str(e)
        else:
            logger.warning("No handler registered for task type '%s'", task.task_type)

        record = {
            "task_id": task.task_id,
            "task_type": task.task_type,
            "payload": task.payload,
            "result": result,
            "error": error,
        }
        self.executed_tasks.append(record)
        self._queue.task_done()
        return record

    async def process_all(self) -> List[Dict[str, Any]]:
        """Processes all currently enqueued tasks."""
        results = []
        while not self._queue.empty():
            res = await self.process_next()
            if res:
                results.append(res)
        return results

    def clear(self) -> None:
        """Clears all tasks."""
        self.tasks.clear()
        self.executed_tasks.clear()
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except (asyncio.QueueEmpty, ValueError):
                break
