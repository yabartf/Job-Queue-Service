"""Webhook job — simulates calling an external endpoint.

Fails 20 % of the time by design, so retry and backoff have something real to
exercise.
"""

from typing import Annotated, Any, Literal

from pydantic import AfterValidator, Field

from app.core.enums import JobType
from app.jobs.base import BaseJob, JobExecutionError, JobPayload, JobResult
from app.jobs.registry import register
from app.jobs.validators import SafeHttpUrl, validate_headers

SLEEP_RANGE = (1.0, 2.0)
FAILURE_RATE = 0.2


class WebhookPayload(JobPayload):
    url: SafeHttpUrl
    method: Literal["POST", "PUT"] = "POST"
    headers: Annotated[dict[str, str], AfterValidator(validate_headers)] = Field(
        default_factory=dict, max_length=10
    )
    body: dict[str, Any] | None = None


class WebhookResult(JobResult):
    status_code: int
    response_ms: int


@register
class WebhookJob(BaseJob):
    job_type = JobType.WEBHOOK
    Payload = WebhookPayload
    Result = WebhookResult

    payload: WebhookPayload

    async def run(self) -> WebhookResult:
        await self._sleep_between(*SLEEP_RANGE)

        if self._rng.random() < FAILURE_RATE:
            # No URL or payload content in the message: it is persisted on the
            # job row and returned through the API.
            raise JobExecutionError("Webhook endpoint returned an error response")

        return WebhookResult(
            status_code=200,
            response_ms=self._rng.randint(20, 400),
        )
