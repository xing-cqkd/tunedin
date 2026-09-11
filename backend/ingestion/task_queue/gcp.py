from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
import json
import logging
import os
from typing import Any, Dict, Optional
from backend.ingestion.task_queue.base import TaskHandler, TaskQueueDriver

logger = logging.getLogger(__name__)

try:
    # google-cloud-tasks is an optional dependency (XIN-36 tracks declaring
    # it). Resolve the POST enum at import time so enqueue() never carries a
    # magic integer, while the module still imports cleanly when the package
    # is absent (enqueue() raises a clear ImportError via _get_client then).
    from google.cloud.tasks_v2 import HttpMethod as _TasksHttpMethod

    _HTTP_POST = _TasksHttpMethod.POST
except ImportError:  # pragma: no cover - depends on the installed env
    _HTTP_POST = 1  # tasks_v2.HttpMethod.POST

# Fail-closed target-URL policy (XIN-77):
# - Plaintext HTTP to localhost is only a dev convenience. It is refused
#   unless the operator explicitly opts in via TASK_QUEUE_ALLOW_INSECURE_LOCAL=1
#   (applies whether the localhost URL is the default or explicitly configured).
# - Non-localhost targets must use https.
# - Non-localhost targets must authenticate via OIDC (service_account_email).
_DEFAULT_LOCAL_TARGET_URL = "http://localhost:8000/api/worker/process-episode"
_INSECURE_LOCAL_OPT_IN_ENV = "TASK_QUEUE_ALLOW_INSECURE_LOCAL"
_LOCAL_HOSTNAMES = {"localhost", "127.0.0.1", "::1"}


def _validate_target_url(target_url: str, service_account_email: Optional[str]) -> None:
    """Fail-closed validation for the worker webhook URL (XIN-77).

    Raises ``ValueError`` with a clear, actionable message on any insecure
    or unauthenticated configuration.
    """
    parsed = urlparse(target_url)
    if not parsed.scheme or not parsed.hostname:
        raise ValueError(
            f"XIN-77: WORKER_WEBHOOK_URL is not a valid absolute URL: {target_url!r}. "
            "Set it to the https URL of the deployed worker "
            "(e.g. https://worker.example.com/api/worker/process-episode)."
        )
    is_local = parsed.hostname.lower() in _LOCAL_HOSTNAMES
    if is_local:
        if parsed.scheme != "https" and os.getenv(_INSECURE_LOCAL_OPT_IN_ENV) != "1":
            raise ValueError(
                f"XIN-77: refusing insecure localhost worker target {target_url!r}. "
                f"Plaintext HTTP to localhost is a dev-only convenience: set "
                f"{_INSECURE_LOCAL_OPT_IN_ENV}=1 to opt in explicitly, or point "
                "WORKER_WEBHOOK_URL at the deployed https worker."
            )
        return
    if parsed.scheme != "https":
        raise ValueError(
            f"XIN-77: refusing non-https worker target {target_url!r}. "
            "Non-localhost webhook URLs must use the https scheme."
        )
    if not service_account_email:
        raise ValueError(
            f"XIN-77: refusing unauthenticated remote worker target {target_url!r}. "
            "Set GCP_SERVICE_ACCOUNT_EMAIL so Cloud Tasks signs requests with an "
            "OIDC token (or pass service_account_email explicitly)."
        )


class GCPCloudTasksDriver(TaskQueueDriver):
    """
    Google Cloud Tasks driver for production deployment.
    Dispatches tasks via HTTP to a worker webhook URL (e.g., Cloud Run /api/worker/process-episode).

    Fail-closed configuration (XIN-77): constructing this driver validates
    the target URL — plaintext localhost targets require
    ``TASK_QUEUE_ALLOW_INSECURE_LOCAL=1``, non-localhost targets require the
    https scheme and an OIDC service-account email. There is no silent
    insecure default.

    In-process handler dispatch (XIN-40): this driver delivers tasks to a
    *remote* webhook, so registered handlers can never fire here.
    :meth:`register_handler`/:meth:`get_handler` raise ``NotImplementedError``
    instead of silently ignoring the registration. Register the handler on
    :class:`LocalInMemoryDriver` for in-process dispatch, or consume tasks
    via ``POST /api/worker/process-episode`` on the deployed worker.
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        location: Optional[str] = None,
        queue_name: Optional[str] = None,
        target_url: Optional[str] = None,
        service_account_email: Optional[str] = None,
    ):
        super().__init__()
        self.project_id = project_id or os.getenv("GCP_PROJECT_ID", "tunedin-prod")
        self.location = location or os.getenv("GCP_LOCATION", "us-central1")
        self.queue_name = queue_name or os.getenv("GCP_CLOUD_TASKS_QUEUE", "podcast-processing-queue")
        self.service_account_email = service_account_email or os.getenv("GCP_SERVICE_ACCOUNT_EMAIL")
        self.target_url = target_url or os.getenv(
            "WORKER_WEBHOOK_URL", _DEFAULT_LOCAL_TARGET_URL
        )
        _validate_target_url(self.target_url, self.service_account_email)
        self._client = None

    def register_handler(self, task_type: str, handler: TaskHandler) -> None:
        raise NotImplementedError(
            "XIN-40: GCPCloudTasksDriver cannot dispatch in-process handlers — "
            f"tasks are POSTed to the remote worker webhook ({self.target_url!r}), "
            "so a registered handler would never fire. Register the handler on "
            "LocalInMemoryDriver for in-process dispatch, or consume the task "
            "via POST /api/worker/process-episode on the deployed worker."
        )

    def get_handler(self, task_type: str) -> Optional[TaskHandler]:
        raise NotImplementedError(
            "XIN-40: GCPCloudTasksDriver has no in-process handlers — tasks are "
            f"POSTed to the remote worker webhook ({self.target_url!r}). See "
            "register_handler for the alternatives."
        )

    def _get_client(self):
        if self._client is None:
            try:
                from google.cloud import tasks_v2
                self._client = tasks_v2.CloudTasksAsyncClient()
            except ImportError:
                raise ImportError(
                    "google-cloud-tasks is required to use GCPCloudTasksDriver. "
                    "Install it via 'pip install google-cloud-tasks'."
                )
        return self._client

    async def enqueue(
        self,
        task_type: str,
        payload: Dict[str, Any],
        in_seconds: Optional[int] = None,
    ) -> str:
        client = self._get_client()
        parent = client.queue_path(self.project_id, self.location, self.queue_name)

        body = {
            "task_type": task_type,
            "payload": payload,
        }
        json_payload = json.dumps(body).encode()

        http_request = {
            "http_method": _HTTP_POST,
            "url": self.target_url,
            "headers": {"Content-Type": "application/json"},
            "body": json_payload,
        }

        if self.service_account_email:
            http_request["oidc_token"] = {
                "service_account_email": self.service_account_email,
            }

        task = {"http_request": http_request}

        if in_seconds and in_seconds > 0:
            from google.protobuf import timestamp_pb2
            timestamp = timestamp_pb2.Timestamp()
            target_dt = datetime.now(timezone.utc) + timedelta(seconds=in_seconds)
            timestamp.FromDatetime(target_dt)
            task["schedule_time"] = timestamp

        response = await client.create_task(
            request={"parent": parent, "task": task}
        )
        task_name = response.name.split("/")[-1]
        logger.info("Enqueued GCP Cloud Task %s for %s", task_name, task_type)
        return task_name
