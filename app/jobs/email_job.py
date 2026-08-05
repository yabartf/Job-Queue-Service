"""Email job — simulates sending a message."""

from uuid import uuid4

from pydantic import EmailStr, Field

from app.core.enums import JobType
from app.jobs.base import BaseJob, JobPayload, JobResult
from app.jobs.registry import register

SLEEP_RANGE = (1.0, 3.0)


class EmailPayload(JobPayload):
    to: EmailStr
    subject: str = Field(min_length=1, max_length=200)
    body: str = Field(min_length=1, max_length=10_000)
    cc: list[EmailStr] = Field(default_factory=list, max_length=10)


class EmailResult(JobResult):
    message_id: str


@register
class EmailJob(BaseJob):
    job_type = JobType.EMAIL
    Payload = EmailPayload
    Result = EmailResult

    payload: EmailPayload

    async def run(self) -> EmailResult:
        await self._sleep_between(*SLEEP_RANGE)
        return EmailResult(message_id=f"msg-{uuid4().hex[:16]}")
