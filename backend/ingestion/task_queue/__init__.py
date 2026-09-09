import os
from typing import Optional
from backend.ingestion.task_queue.base import EnqueuedTask, TaskHandler, TaskQueueDriver
from backend.ingestion.task_queue.local import LocalInMemoryDriver
from backend.ingestion.task_queue.gcp import GCPCloudTasksDriver

_DEFAULT_DRIVER: Optional[TaskQueueDriver] = None


def get_queue_driver(driver_type: Optional[str] = None) -> TaskQueueDriver:
    """
    Factory function returning the configured TaskQueueDriver singleton or instance.
    Reads TASK_QUEUE_DRIVER env var ('local' or 'gcp'). Defaults to 'local'.
    """
    global _DEFAULT_DRIVER
    selected = (driver_type or os.getenv("TASK_QUEUE_DRIVER", "local")).lower()

    if selected == "gcp":
        return GCPCloudTasksDriver()
    elif selected == "local":
        if _DEFAULT_DRIVER is None or not isinstance(_DEFAULT_DRIVER, LocalInMemoryDriver):
            _DEFAULT_DRIVER = LocalInMemoryDriver()
        return _DEFAULT_DRIVER
    else:
        raise ValueError(f"Unknown TASK_QUEUE_DRIVER: '{selected}'. Supported: 'local', 'gcp'.")


__all__ = [
    "TaskQueueDriver",
    "EnqueuedTask",
    "TaskHandler",
    "LocalInMemoryDriver",
    "GCPCloudTasksDriver",
    "get_queue_driver",
]
