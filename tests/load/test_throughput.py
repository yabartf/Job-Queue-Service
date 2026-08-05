"""W4-01, W4-02 — a backlog drained by concurrent slots.

The claim tests prove one job goes to one worker. These prove it stays true
across a few hundred jobs and several slots running at once, and that a worker
dying part-way through costs nothing but time.

Every execution is recorded by the handler itself, so "exactly once" is asserted
against what actually ran rather than inferred from the attempt counter.
"""

import asyncio
from datetime import timedelta
from uuid import UUID

import pytest
from sqlalchemy import func, select, text

from app.core.config import Settings
from app.core.enums import JobStatus, JobType
from app.db.models import Job
from app.dispatch.base import NullDispatch
from app.jobs.base import BaseJob, JobPayload, JobResult
from app.jobs.registry import JOB_REGISTRY
from app.worker.runtime import Worker
from tests.doubles import RecordingSleeper, StubRandom
from tests.factories import add_job

pytestmark = pytest.mark.committing

BACKLOG = 200
SLOTS = 4


class ProbePayload(JobPayload):
    marker: str


class ProbeResult(JobResult):
    marker: str


@pytest.fixture
def executions() -> list[UUID]:
    """Swap a recording handler in for the duration of the test."""
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


def build_worker(sessions, clock, **overrides) -> Worker:
    values: dict = {
        "worker_concurrency": SLOTS,
        "worker_lease_seconds": 60,
        "worker_heartbeat_seconds": 3600,
        "worker_poll_interval_seconds": 0.01,
        "maintenance_interval_seconds": 0.25,
        "maintenance_batch_size": BACKLOG,
        "shutdown_grace_seconds": 5.0,
    }
    values.update(overrides)
    return Worker(
        Settings(**values),
        sessions,
        NullDispatch(),
        clock,
        rng=StubRandom(0.99),
        sleeper=RecordingSleeper(),
    )


async def seed_backlog(sessions, count: int = BACKLOG) -> None:
    async with sessions() as session:
        for index in range(count):
            await add_job(
                session,
                job_type=JobType.EMAIL,
                payload={"marker": f"m{index}"},
                priority=index % 10,
            )
        await session.commit()


async def all_job_ids(sessions) -> set[UUID]:
    async with sessions() as session:
        return set((await session.execute(select(Job.id))).scalars().all())


async def count_where(sessions, clause: str) -> int:
    async with sessions() as session:
        return int(
            (await session.execute(text(f"SELECT count(*) FROM jobs WHERE {clause}"))).scalar_one()
        )


async def drain(worker: Worker, sessions, timeout: float = 60.0) -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))

    async def wait_for_terminal() -> None:
        while await count_where(sessions, "status IN ('pending','processing','scheduled')"):
            await asyncio.sleep(0.02)

    try:
        await asyncio.wait_for(wait_for_terminal(), timeout=timeout)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=timeout)


async def test_w4_01_a_backlog_is_drained_exactly_once(pooled_sessions, clock, executions):
    await seed_backlog(pooled_sessions)

    await drain(build_worker(pooled_sessions, clock), pooled_sessions)

    assert len(executions) == BACKLOG, "some job ran more or fewer times than once"
    assert len(set(executions)) == BACKLOG, "a job was executed twice"

    async with pooled_sessions() as session:
        by_status = dict(
            (await session.execute(select(Job.status, func.count()).group_by(Job.status))).all()
        )
    assert by_status == {JobStatus.COMPLETED: BACKLOG}


async def test_w4_01b_no_job_is_left_holding_a_lease(pooled_sessions, clock, executions):
    await seed_backlog(pooled_sessions, count=50)

    await drain(build_worker(pooled_sessions, clock), pooled_sessions)

    assert await count_where(pooled_sessions, "worker_id IS NOT NULL") == 0
    assert await count_where(pooled_sessions, "lease_until IS NOT NULL") == 0


async def test_w4_02_a_worker_dying_mid_run_costs_only_time(pooled_sessions, clock, executions):
    """Killed without a chance to release anything — the crash case, not the
    shutdown case. A second worker's reaper has to finish the backlog."""
    await seed_backlog(pooled_sessions, count=60)

    victim = build_worker(pooled_sessions, clock)
    stop = asyncio.Event()
    task = asyncio.create_task(victim.run(stop))
    while await count_where(pooled_sessions, "status = 'completed'") < 5:
        await asyncio.sleep(0.01)

    task.cancel()  # no graceful path: leases are simply abandoned
    await asyncio.gather(task, return_exceptions=True)

    stranded = await count_where(pooled_sessions, "status = 'processing'")

    # The reaper waits out the lease; expiring it by hand keeps the test quick
    # without changing what is being tested.
    async with pooled_sessions() as session:
        await session.execute(
            text(
                "UPDATE jobs SET lease_until = now() - interval '1 second' "
                "WHERE status = 'processing'"
            )
        )
        await session.commit()

    await drain(build_worker(pooled_sessions, clock), pooled_sessions)

    assert await count_where(pooled_sessions, "status <> 'completed'") == 0
    # Every job ran, and nothing was lost — the guarantee that matters.
    assert set(executions) == await all_job_ids(pooled_sessions)
    # Delivery is at-least-once, so a job caught by the kill may have run twice.
    # The excess is bounded by what was stranded, and can be lower: a job
    # cancelled between its claim and the handler starting was stranded without
    # ever executing.
    assert 60 <= len(executions) <= 60 + stranded


async def test_w4_02b_recovery_still_respects_the_attempt_limit(pooled_sessions, clock, executions):
    """A job whose lease expires with nothing left cannot go back to the queue —
    the next claim would breach ck_jobs_attempts."""
    async with pooled_sessions() as session:
        job = await add_job(
            session,
            job_type=JobType.EMAIL,
            payload={"marker": "m"},
            status=JobStatus.PROCESSING,
            worker_id="w-dead",
            started_at=clock.now(),
            lease_until=clock.now() - timedelta(minutes=5),
            attempts=3,
            max_attempts=3,
        )
        await session.commit()

    worker = build_worker(pooled_sessions, clock)
    assert (await worker.maintenance.run_once()).failed == [job.id]

    async with pooled_sessions() as session:
        stored = await session.get(Job, job.id)
        assert stored is not None
        assert stored.status == JobStatus.FAILED
        assert stored.error["type"] == "LeaseExpired"
