"""Batch job — processes many items, reporting progress as it goes."""

from typing import Annotated, Literal

from pydantic import Field

from app.core.enums import JobType
from app.jobs.base import BaseJob, JobPayload, JobResult
from app.jobs.registry import register

ITEM_DELAY_SECONDS = 0.05
ITEM_FAILURE_RATE = 0.05
MAX_REPORTED_ERRORS = 100


class BatchPayload(JobPayload):
    items: list[Annotated[str, Field(min_length=1, max_length=500)]] = Field(
        min_length=1, max_length=1000
    )
    operation: Literal["index", "validate", "transform"]


class BatchResult(JobResult):
    total: int
    succeeded: int
    failed: int
    errors: list[str] = Field(default_factory=list, max_length=MAX_REPORTED_ERRORS)


@register
class BatchJob(BaseJob):
    job_type = JobType.BATCH
    Payload = BatchPayload
    Result = BatchResult

    payload: BatchPayload

    timeout_seconds = 300

    async def run(self) -> BatchResult:
        """Process every item, then report a summary.

        Individual item failures do not fail the job: a batch that processed 998
        of 1000 items successfully has done useful work, and re-running the whole
        batch to retry two items would redo the other 998. The failures are
        reported in the result instead, and the job completes.
        """
        items = self.payload.items
        total = len(items)
        succeeded = 0
        failed = 0
        errors: list[str] = []
        last_reported = -1

        for position, _item in enumerate(items, start=1):
            await self._sleeper.sleep(ITEM_DELAY_SECONDS)

            if self._rng.random() < ITEM_FAILURE_RATE:
                failed += 1
                if len(errors) < MAX_REPORTED_ERRORS:
                    # Index only — item contents are user data and end up in the
                    # persisted result and the API response.
                    errors.append(f"item at index {position - 1} failed")
            else:
                succeeded += 1

            # Report only when the whole-number percentage moves. A 1000-item
            # batch then produces 100 updates rather than 1000 identical ones.
            percent = position * 100 // total
            if percent != last_reported:
                await self.ctx.report_progress(percent)
                await self.ctx.heartbeat()
                last_reported = percent

        return BatchResult(total=total, succeeded=succeeded, failed=failed, errors=errors)
