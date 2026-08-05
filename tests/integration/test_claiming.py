"""W2-01 .. W2-07, W2-17 — claiming against a real PostgreSQL.

The concurrency tests here use `committing_sessions`, not the shared rolled-back
session: sharing one transaction would serialise exactly the concurrency they
exist to exercise.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.enums import JobStatus, JobType
from app.db.models import Job
from app.db.repository import JobRepository, Ownership
from tests.factories import add_job, add_processing_job

LEASE = 60


async def test_w2_01_highest_priority_is_claimed_first(repository, db_session):
    for priority in (2, 9, 5):
        await add_job(db_session, priority=priority)

    claimed = [
        (await repository.claim_next(f"w-{n}", LEASE)).priority  # type: ignore[union-attr]
        for n in range(3)
    ]

    assert claimed == [9, 5, 2]


async def test_w2_01b_oldest_first_within_a_priority(repository, db_session):
    base = datetime.now(UTC) - timedelta(hours=1)
    for offset in (2, 0, 1):
        await add_job(db_session, priority=5, created_at=base + timedelta(minutes=offset))

    order = [
        (await repository.claim_next(f"w-{n}", LEASE)).created_at  # type: ignore[union-attr]
        for n in range(3)
    ]

    assert order == sorted(order)


async def test_w2_02_future_scheduled_jobs_are_not_claimable(repository, db_session):
    await add_job(
        db_session,
        status=JobStatus.PENDING,
        scheduled_at=datetime.now(UTC) + timedelta(hours=1),
    )

    assert await repository.claim_next("w-0", LEASE) is None


async def test_w2_02b_elapsed_scheduled_jobs_are_claimable(repository, db_session):
    await add_job(
        db_session,
        status=JobStatus.PENDING,
        scheduled_at=datetime.now(UTC) - timedelta(minutes=1),
    )

    assert await repository.claim_next("w-0", LEASE) is not None


@pytest.mark.parametrize(
    "status", [JobStatus.SCHEDULED, JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]
)
async def test_w2_02c_only_pending_jobs_are_claimable(repository, db_session, status):
    extra: dict = {}
    if status is JobStatus.SCHEDULED:
        extra["scheduled_at"] = datetime.now(UTC) - timedelta(hours=1)
    if status is JobStatus.COMPLETED:
        extra |= {"started_at": datetime.now(UTC), "completed_at": datetime.now(UTC)}
    await add_job(db_session, status=status, **extra)

    assert await repository.claim_next("w-0", LEASE) is None


async def test_w2_05_claim_records_the_lease_and_consumes_an_attempt(repository, db_session):
    await add_job(db_session)

    job = await repository.claim_next("w-7", LEASE)

    assert job is not None
    assert job.status == JobStatus.PROCESSING
    assert job.worker_id == "w-7"
    assert job.attempts == 1
    assert job.lease_until is not None and job.lease_until > datetime.now(UTC)
    assert job.started_at is not None


async def test_w2_05b_claim_by_id_takes_that_job(repository, db_session):
    wanted = await add_job(db_session, priority=0)
    await add_job(db_session, priority=9)

    job = await repository.claim_by_id(wanted.id, "w-0", LEASE)

    assert job is not None
    assert job.id == wanted.id  # priority is irrelevant when a hint names a job


@pytest.mark.parametrize("reason", ["already claimed", "cancelled", "not yet due"])
async def test_w2_05c_a_stale_hint_claims_nothing(repository, db_session, reason):
    if reason == "already claimed":
        job = await add_processing_job(
            db_session, worker_id="w-other", lease_until=datetime.now(UTC) + timedelta(minutes=5)
        )
    elif reason == "cancelled":
        job = await add_job(db_session, status=JobStatus.CANCELLED)
    else:
        job = await add_job(db_session, scheduled_at=datetime.now(UTC) + timedelta(hours=1))

    assert await repository.claim_by_id(job.id, "w-0", LEASE) is None


async def test_w2_05d_claim_returns_none_on_an_empty_queue(repository):
    assert await repository.claim_next("w-0", LEASE) is None


async def test_w2_06_a_stale_attempts_value_owns_nothing(repository, db_session):
    await add_job(db_session)
    job = await repository.claim_next("w-0", LEASE)
    assert job is not None

    stale = Ownership(job_id=job.id, worker_id="w-0", attempts=job.attempts - 1)
    wrong_worker = Ownership(job_id=job.id, worker_id="w-1", attempts=job.attempts)
    current = Ownership.of(job)

    assert await repository.extend_lease(stale, LEASE) is False
    assert await repository.extend_lease(wrong_worker, LEASE) is False
    assert await repository.extend_lease(current, LEASE) is True


async def test_w2_06b_ownership_of_an_unclaimed_job_is_rejected(db_session):
    job = await add_job(db_session)

    with pytest.raises(ValueError, match="no worker_id"):
        Ownership.of(job)


# ---------------------------------------------------------------------------
# Concurrency — real connections, real commits
# ---------------------------------------------------------------------------


async def _claim_with(sessions: async_sessionmaker[AsyncSession], worker_id: str) -> UUID | None:
    async with sessions() as session:
        job = await JobRepository(session).claim_next(worker_id, LEASE)
        await session.commit()
        return job.id if job else None


async def _seed(sessions: async_sessionmaker[AsyncSession], count: int) -> None:
    async with sessions() as session:
        for _ in range(count):
            await add_job(session)
        await session.commit()


async def test_w2_03_ten_workers_one_job_produce_one_winner(committing_sessions):
    await _seed(committing_sessions, 1)

    results = await asyncio.gather(*(_claim_with(committing_sessions, f"w-{n}") for n in range(10)))

    winners = [job_id for job_id in results if job_id is not None]
    assert len(winners) == 1


async def test_w2_04_ten_workers_ten_jobs_claim_ten_distinct(committing_sessions):
    await _seed(committing_sessions, 10)

    results = await asyncio.gather(*(_claim_with(committing_sessions, f"w-{n}") for n in range(10)))

    claimed = [job_id for job_id in results if job_id is not None]
    assert len(claimed) == 10
    assert len(set(claimed)) == 10  # SKIP LOCKED stepped over, never doubled up


async def test_w2_07_a_displaced_worker_discards_its_own_result(committing_sessions):
    """The test the whole ownership design exists for.

    A worker stalls, the reaper hands its job to someone else, and then the
    original worker wakes up and tries to record a result. Its write must match
    nothing — otherwise it silently overwrites the work of the worker that
    legitimately took over.
    """
    await _seed(committing_sessions, 1)

    async with committing_sessions() as session:
        first = await JobRepository(session).claim_next("w-slow", LEASE)
        assert first is not None
        stale_ownership = Ownership.of(first)
        await session.commit()

    # The worker stalls: its lease lapses without being extended.
    async with committing_sessions() as session:
        await session.execute(
            text("UPDATE jobs SET lease_until = now() - interval '1 second' WHERE id = :i"),
            {"i": first.id},
        )
        await session.commit()

    async with committing_sessions() as session:
        reaped = await JobRepository(session).reap_expired_leases(10, {"type": "LeaseExpired"})
        await session.commit()
    assert reaped.released == [first.id]

    async with committing_sessions() as session:
        repo = JobRepository(session)
        second = await repo.claim_next("w-fresh", LEASE)
        assert second is not None and second.id == first.id
        assert second.attempts == 2  # the fencing token moved
        assert await repo.mark_completed(Ownership.of(second), {"by": "w-fresh"}) is True
        await session.commit()

    async with committing_sessions() as session:
        repo = JobRepository(session)
        assert await repo.mark_completed(stale_ownership, {"by": "w-slow"}) is False
        await session.commit()

    async with committing_sessions() as session:
        final = await session.get(Job, first.id)
        assert final is not None
        assert final.status == JobStatus.COMPLETED
        assert final.result == {"by": "w-fresh"}


async def test_w2_17_the_claim_query_uses_its_index(db_session):
    """Index usability, without seeding a table large enough for the planner to
    prefer it unprompted."""
    await db_session.execute(text("SET LOCAL enable_seqscan = off"))
    plan = (
        (
            await db_session.execute(
                text(
                    "EXPLAIN SELECT id FROM jobs "
                    "WHERE status = 'pending' "
                    "  AND (scheduled_at IS NULL OR scheduled_at <= now()) "
                    "ORDER BY priority DESC, created_at ASC LIMIT 1 "
                    "FOR UPDATE SKIP LOCKED"
                )
            )
        )
        .scalars()
        .all()
    )

    assert any("ix_jobs_claim" in line for line in plan), plan


async def test_claim_of_a_missing_id_is_harmless(repository):
    assert await repository.claim_by_id(uuid4(), "w-0", LEASE) is None


async def test_claim_spans_job_types_from_one_index(repository, db_session):
    """The flat table exists so one query can serve every type (spec 01 §2)."""
    await add_job(db_session, job_type=JobType.EMAIL, priority=1)
    await add_job(
        db_session,
        job_type=JobType.BATCH,
        payload={"items": ["a"], "operation": "index"},
        priority=9,
    )

    job = await repository.claim_next("w-0", LEASE)

    assert job is not None
    assert job.job_type == JobType.BATCH
