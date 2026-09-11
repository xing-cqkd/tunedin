import os
from typing import Dict, Optional
from backend.ingestion.task_queue.base import EnqueuedTask, TaskHandler, TaskQueueDriver
from backend.ingestion.task_queue.local import LocalInMemoryDriver
from backend.ingestion.task_queue.gcp import GCPCloudTasksDriver

# Cached driver instances, keyed by driver type (XIN-40: lifecycle semantics
# are identical for every driver — one singleton per type, created lazily).
_DRIVERS: Dict[str, TaskQueueDriver] = {}


def get_queue_driver(driver_type: Optional[str] = None) -> TaskQueueDriver:
    """
    Factory function returning the configured TaskQueueDriver singleton.
    Reads TASK_QUEUE_DRIVER env var ('local' or 'gcp'). Defaults to 'local'.

    Both drivers are cached as singletons with the same lifecycle: the first
    call for a driver type constructs it, later calls return the same
    instance. Note that selecting 'gcp' constructs a GCPCloudTasksDriver,
    whose fail-closed URL validation (XIN-77) raises ValueError unless the
    worker webhook URL is securely configured.
    """
    selected = (driver_type or os.getenv("TASK_QUEUE_DRIVER", "local")).lower()

    if selected not in ("gcp", "local"):
        raise ValueError(f"Unknown TASK_QUEUE_DRIVER: '{selected}'. Supported: 'local', 'gcp'.")

    driver = _DRIVERS.get(selected)
    if driver is None:
        driver = GCPCloudTasksDriver() if selected == "gcp" else LocalInMemoryDriver()
        _DRIVERS[selected] = driver
    return driver


__all__ = [
    "TaskQueueDriver",
    "EnqueuedTask",
    "TaskHandler",
    "LocalInMemoryDriver",
    "GCPCloudTasksDriver",
    "get_queue_driver",
]
