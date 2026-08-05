"""Report job — simulates generating a report file."""

from datetime import date
from typing import Literal
from uuid import uuid4

from pydantic import model_validator

from app.core.enums import JobType
from app.jobs.base import BaseJob, JobPayload, JobResult
from app.jobs.registry import register

SLEEP_RANGE = (3.0, 5.0)


class ReportPayload(JobPayload):
    report_type: Literal["sales", "users", "activity"]
    date_from: date
    date_to: date
    format: Literal["csv", "pdf"] = "csv"

    @model_validator(mode="after")
    def _check_date_range(self) -> "ReportPayload":
        if self.date_from > self.date_to:
            raise ValueError("date_from must not be after date_to")
        return self


class ReportResult(JobResult):
    file_url: str
    row_count: int


@register
class ReportJob(BaseJob):
    job_type = JobType.REPORT
    Payload = ReportPayload
    Result = ReportResult

    payload: ReportPayload

    # Reports are the slowest job type here, so they get more headroom before
    # the timeout enforced in spec 14 applies.
    timeout_seconds = 120

    async def run(self) -> ReportResult:
        await self._sleep_between(*SLEEP_RANGE)
        return ReportResult(
            file_url=(
                f"https://reports.example.com/{self.payload.report_type}/"
                f"{uuid4().hex}.{self.payload.format}"
            ),
            row_count=self._rng.randint(1, 50_000),
        )
