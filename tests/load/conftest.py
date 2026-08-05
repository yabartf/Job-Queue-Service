"""Fixtures shared by the load tests.

``executions`` lives here rather than in one of the test modules so both can use
it without importing a fixture across files, which shadows the parameter of the
same name.
"""

from uuid import UUID

import pytest

from app.core.enums import JobType
from app.jobs.base import BaseJob, JobPayload, JobResult
from app.jobs.registry import JOB_REGISTRY


class ProbePayload(JobPayload):
    marker: str


class ProbeResult(JobResult):
    marker: str


@pytest.fixture
def executions() -> list[UUID]:
    """Swap a recording handler in for the duration of the test.

    Every run appends its job id, so "exactly once" is asserted against what
    actually executed rather than inferred from the attempt counter.
    """
    recorded: list[UUID] = []

    class ProbeJob(BaseJob):
        job_type = JobType.EMAIL
        Payload = ProbePayload
        Result = ProbeResult

        async def run(self) -> ProbeResult:
            recorded.append(self.ctx.job_id)
            return ProbeResult(marker=self.payload.marker)

    original = JOB_REGISTRY[JobType.EMAIL]
    JOB_REGISTRY[JobType.EMAIL] = ProbeJob
    try:
        yield recorded
    finally:
        JOB_REGISTRY[JobType.EMAIL] = original
