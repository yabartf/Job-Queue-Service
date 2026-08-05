"""W2-15, W2-16 — the worker process against a real PostgreSQL.

These use `committing_sessions`: the worker opens a fresh unit of work per
operation, so a single rolled-back session would not see its own writes.
"""

import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.core.enums import JobStatus
from app.db.models import Job
from app.db.repository import JobRepository
from app.dispatch.base import NullDispatch
from app.dispatch.redis_dispatch import RedisDispatch
from app.worker.__main__ import install_signal_handlers, run_worker
from app.worker.maintenance import Maintenance
from app.worker.runtime import Worker
from tests.doubles import BlockingSleeper, RecordingSleeper, StubRandom
from tests.factories import add_job, add_processing_job

pytestmark = pytest.mark.committing


def worker_settings(**overrides) -> Settings:
    values: dict = {
        "worker_concurrency": 1,
        "worker_lease_seconds": 60,
        "worker_heartbeat_seconds": 3600,
        "worker_poll_interval_seconds": 0.01,
        "maintenance_interval_seconds": 0.01,
        "maintenance_batch_size": 50,
        "shutdown_grace_seconds": 2.0,
    }
    values.update(overrides)
    return Settings(**values)


def build_worker(sessions, clock, **overrides) -> Worker:
    sleeper = overrides.pop("sleeper", RecordingSleeper())
    return Worker(
        worker_settings(**overrides),
        sessions,
        NullDispatch(),
        clock,
        rng=StubRandom(0.99),
        sleeper=sleeper,
    )


async def seed(sessions: async_sessionmaker[AsyncSession], **overrides) -> Job:
    async with sessions() as session:
        job = await add_job(session, **overrides)
        await session.commit()
        return job


async def read(sessions: async_sessionmaker[AsyncSession], job_id) -> Job:
    async with sessions() as session:
        job = await session.get(Job, job_id)
        assert job is not None
        return job


async def run_until_idle(worker: Worker, sessions, job_id, timeout: float = 5.0) -> None:
    """Start the worker, stop it once the job reaches a terminal state."""
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))

    async def wait_for_terminal() -> None:
        while True:
            job = await read(sessions, job_id)
            if job.status in {JobStatus.COMPLETED, JobStatus.FAILED}:
                return
            await asyncio.sleep(0.01)

    try:
        await asyncio.wait_for(wait_for_terminal(), timeout=timeout)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=timeout)


async def test_a_worker_runs_a_job_to_completion(committing_sessions, clock):
    job = await seed(committing_sessions)
    worker = build_worker(committing_sessions, clock)

    await run_until_idle(worker, committing_sessions, job.id)

    stored = await read(committing_sessions, job.id)
    assert stored.status == JobStatus.COMPLETED
    assert stored.result is not None
    assert stored.attempts == 1


async def test_w2_15_shutdown_finishes_the_job_in_hand(committing_sessions, clock):
    """Graceful, not merely fast: no new work is claimed, but the job already
    running is allowed to finish."""
    await seed(committing_sessions)
    worker = build_worker(committing_sessions, clock)
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))

    while worker.slots[0].current is None:
        await asyncio.sleep(0.01)

    stop.set()
    await asyncio.wait_for(task, timeout=5)

    async with committing_sessions() as session:
        statuses = (await session.execute(text("SELECT status FROM jobs"))).scalars().all()
    assert list(statuses) == [JobStatus.COMPLETED]


async def test_w2_15b_a_stopped_worker_claims_nothing(committing_sessions, clock):
    await seed(committing_sessions)
    worker = build_worker(committing_sessions, clock)
    stop = asyncio.Event()
    stop.set()

    await asyncio.wait_for(worker.run(stop), timeout=5)

    stored = await read(committing_sessions, (await seed(committing_sessions)).id)
    assert stored.status == JobStatus.PENDING


async def test_w2_16_work_still_running_at_the_deadline_is_released(committing_sessions, clock):
    """The lease is handed back explicitly rather than left to lapse, so the job
    is claimable at once instead of invisible for a full lease duration.

    Two slots and one job, so the release also has to skip the idle slot rather
    than assume every slot is holding something.
    """
    job = await seed(committing_sessions)
    worker = build_worker(
        committing_sessions,
        clock,
        sleeper=BlockingSleeper(),
        shutdown_grace_seconds=0.2,
        worker_concurrency=2,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))

    while all(slot.current is None for slot in worker.slots):
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=5)

    stored = await read(committing_sessions, job.id)
    assert stored.status == JobStatus.PENDING
    assert stored.worker_id is None
    assert stored.lease_until is None
    assert stored.attempts == 1  # the attempt was still spent


async def test_w2_16b_a_released_job_is_immediately_claimable(committing_sessions, clock):
    job = await seed(committing_sessions)
    worker = build_worker(
        committing_sessions, clock, sleeper=BlockingSleeper(), shutdown_grace_seconds=0.2
    )
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))
    while worker.slots[0].current is None:
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=5)

    async with committing_sessions() as session:
        reclaimed = await JobRepository(session).claim_next("w-next", 60)
        await session.commit()

    assert reclaimed is not None and reclaimed.id == job.id
    assert reclaimed.attempts == 2


async def test_slots_get_distinct_identities(committing_sessions, clock):
    worker = build_worker(committing_sessions, clock, worker_concurrency=3)

    identities = {slot.worker_id for slot in worker.slots}

    assert len(identities) == 3


async def test_several_slots_drain_a_backlog(committing_sessions, clock):
    for _ in range(6):
        await seed(committing_sessions)
    worker = build_worker(committing_sessions, clock, worker_concurrency=3)
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))

    async def wait_for_all() -> None:
        while True:
            async with committing_sessions() as session:
                remaining = (
                    await session.execute(
                        text("SELECT count(*) FROM jobs WHERE status <> 'completed'")
                    )
                ).scalar_one()
            if remaining == 0:
                return
            await asyncio.sleep(0.01)

    try:
        await asyncio.wait_for(wait_for_all(), timeout=10)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)

    async with committing_sessions() as session:
        doubled = (
            await session.execute(text("SELECT count(*) FROM jobs WHERE attempts > 1"))
        ).scalar_one()
    assert doubled == 0  # nothing was executed twice


async def test_the_entry_point_wires_a_working_worker(test_db_url):
    """`python -m app.worker` has to build the whole graph correctly; a mistake
    here means the process starts and quietly does nothing."""
    stop = asyncio.Event()
    stop.set()

    await asyncio.wait_for(run_worker(stop, worker_settings(database_url=test_db_url)), timeout=10)


async def test_the_entry_point_gives_the_worker_a_real_dispatch(monkeypatch, test_db_url):
    """The wiring that was once missed: a worker holding a NullDispatch still
    runs jobs — through the fallback claim — so it looks healthy while never
    registering its liveness or consuming a single hint. Nothing but this
    assertion notices."""
    built: list[object] = []
    original = Worker.__init__

    def record(self, settings, sessions, dispatch, clock, **kwargs):  # type: ignore[no-untyped-def]
        built.append(dispatch)
        original(self, settings, sessions, dispatch, clock, **kwargs)

    monkeypatch.setattr(Worker, "__init__", record)
    stop = asyncio.Event()
    stop.set()

    await asyncio.wait_for(run_worker(stop, worker_settings(database_url=test_db_url)), timeout=10)

    assert isinstance(built[0], RedisDispatch)


async def test_signal_handlers_can_be_installed():
    stop = asyncio.Event()

    install_signal_handlers(stop)

    assert not stop.is_set()


# ---------------------------------------------------------------------------
# Maintenance
# ---------------------------------------------------------------------------


async def test_maintenance_recovers_an_abandoned_job(committing_sessions, clock):
    async with committing_sessions() as session:
        job = await add_processing_job(
            session,
            worker_id="w-dead",
            lease_until=clock.now() - timedelta(minutes=5),
            attempts=1,
        )
        await session.commit()

    worker = build_worker(committing_sessions, clock)
    result = await worker.maintenance.run_once()

    assert result.released == [job.id]
    assert (await read(committing_sessions, job.id)).status == JobStatus.PENDING


async def test_maintenance_keeps_running_after_a_failed_sweep(committing_sessions, clock):
    """A sweep that raises must not take the worker down: the slots are still
    processing, and the next pass will retry."""
    worker = build_worker(committing_sessions, clock)
    calls: list[int] = []

    async def explode_once() -> None:
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("transient database blip")

    maintenance = Maintenance(scope=worker.maintenance._scope, interval_seconds=0.01, batch_size=10)
    maintenance.run_once = explode_once  # type: ignore[method-assign]
    stop = asyncio.Event()

    task = asyncio.create_task(maintenance.run_forever(stop))
    while len(calls) < 3:
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=2)

    assert len(calls) >= 3
