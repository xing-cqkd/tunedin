from datetime import datetime, timedelta, timezone
import json
import logging
import os
from typing import Any, Dict, Optional
from backend.ingestion.task_queue.base import TaskQueueDriver

logger = logging.getLogger(__name__)


class GCPCloudTasksDriver(TaskQueueDriver):
    """
    Google Cloud Tasks driver for production deployment.
    Dispatches tasks via HTTP to a worker webhook URL (e.g., Cloud Run /api/worker/process-episode).
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
        self.target_url = target_url or os.getenv(
            "WORKER_WEBHOOK_URL", "http://localhost:8000/api/worker/process-episode"
        )
        self.service_account_email = service_account_email or os.getenv("GCP_SERVICE_ACCOUNT_EMAIL")
        self._client = None

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
            "http_method": 1,  # tasks_v2.HttpMethod.POST
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
