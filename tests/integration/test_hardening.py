"""H-01 .. H-12 — manual retry, job timeout and dead-letter routing.

The distinction these tests exist to protect: a job that failed is not the same
as a job that *cannot run*. Only the second is dead-lettered, and only the second
is refused a retry.
"""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.exc import IntegrityError
from structlog.testing import capture_logs

from app.core.enums import DeadLetterReason, JobStatus
from app.db.repository import JobFilters, Ownership
from app.jobs.base import JobExecutionError, JobTimeoutError
from tests.factories import add_job

LEASE = 60


async def claimed(execution, db_session, **overrides):
    await add_job(db_session, **overrides)
    job = await execution.claim("w-0", LEASE)
    assert job is not None
    return job, Ownership.of(job)


async def failed_job(execution, db_session, exc=None, **overrides):
    """Run a job's last attempt into the ground and return the row."""
    job, own = await claimed(execution, db_session, attempts=2, max_attempts=3, **overrides)
    await execution.fail(job, own, exc or JobExecutionError("downstream was down"))
    await db_session.refresh(job)
    return job


# ---------------------------------------------------------------------------
# H-01 .. H-04 — manual retry
# ---------------------------------------------------------------------------


async def test_h_01_retry_puts_a_failed_job_back_in_the_queue(repository, execution, db_session):
    job = await failed_job(execution, db_session)
    assert job.status == JobStatus.FAILED

    retried = await repository.retry_failed(job.id)

    assert retried is not None
    await db_session.refresh(job)
    assert job.status == JobStatus.PENDING
    assert job.attempts == 0  # or it would fail again on its first claim
    assert job.error is None
    assert job.completed_at is None and job.started_at is None
    assert job.scheduled_at is None
    assert job.progress == 0


async def test_h_02_a_retried_job_is_claimable_and_runs_again(repository, execution, db_session):
    job = await failed_job(execution, db_session)
    await repository.retry_failed(job.id)

    reclaimed = await execution.claim("w-fresh", LEASE)

    assert reclaimed is not None
    assert reclaimed.id == job.id
    assert reclaimed.attempts == 1  # counting starts over


@pytest.mark.parametrize(
    "status", [JobStatus.PENDING, JobStatus.PROCESSING, JobStatus.COMPLETED, JobStatus.CANCELLED]
)
async def test_h_03_only_failed_jobs_can_be_retried(repository, db_session, status):
    extra: dict = {}
    if status is JobStatus.PROCESSING:
        extra = {"worker_id": "w-1", "lease_until": datetime.now(UTC) + timedelta(minutes=1)}
    if status is JobStatus.COMPLETED:
        extra = {"started_at": datetime.now(UTC), "completed_at": datetime.now(UTC)}
    job = await add_job(db_session, status=status, **extra)

    assert await repository.retry_failed(job.id) is None

    await db_session.refresh(job)
    assert job.status == status


async def test_h_04_a_dead_lettered_job_is_refused(repository, execution, db_session):
    """The protection the classification exists for: an operator retrying
    everything that failed must not re-arm a job that kills workers."""
    job, own = await claimed(execution, db_session, payload={"unusable": "shape"})
    await execution.fail(job, own, JobExecutionError("cannot parse"), retryable=False)
    await db_session.refresh(job)
    assert job.dead_letter_reason == DeadLetterReason.UNPROCESSABLE_PAYLOAD

    assert await repository.retry_failed(job.id) is None

    await db_session.refresh(job)
    assert job.status == JobStatus.FAILED


# ---------------------------------------------------------------------------
# H-05 .. H-07 — timeout
# ---------------------------------------------------------------------------


async def test_h_06_a_timeout_with_attempts_left_is_an_ordinary_retry(execution, db_session):
    """One slow run proves nothing about the next one."""
    job, own = await claimed(execution, db_session, max_attempts=3)

    assert await execution.fail(job, own, JobTimeoutError("exceeded 60s")) is True

    await db_session.refresh(job)
    assert job.status == JobStatus.PENDING
    assert job.dead_letter_reason is None
    assert job.scheduled_at is not None


async def test_h_07_timing_out_on_every_attempt_is_poison(execution, db_session):
    job, own = await claimed(execution, db_session, attempts=2, max_attempts=3)

    with capture_logs() as entries:
        await execution.fail(job, own, JobTimeoutError("exceeded 60s"))

    await db_session.refresh(job)
    assert job.status == JobStatus.FAILED
    assert job.dead_letter_reason == DeadLetterReason.TIMEOUT_LOOP
    event = next(e for e in entries if e["event"] == "job.dead_lettered")
    assert event["log_level"] == "error"


# ---------------------------------------------------------------------------
# H-08 .. H-10 — classification
# ---------------------------------------------------------------------------


async def test_h_08_an_unusable_payload_is_dead_lettered_on_the_first_attempt(
    execution, db_session
):
    job, own = await claimed(execution, db_session, max_attempts=3)
    assert job.attempts == 1  # two attempts still remain

    await execution.fail(job, own, JobExecutionError("schema moved"), retryable=False)

    await db_session.refresh(job)
    assert job.status == JobStatus.FAILED
    assert job.dead_letter_reason == DeadLetterReason.UNPROCESSABLE_PAYLOAD


async def test_h_09_a_job_that_kills_its_workers_is_dead_lettered(execution, db_session, clock):
    job, _ = await claimed(execution, db_session, attempts=2, max_attempts=3)
    job.lease_until = clock.now() - timedelta(minutes=5)
    await db_session.flush()

    result = await execution.sweep(50)

    assert result.failed == [job.id]
    await db_session.refresh(job)
    assert job.dead_letter_reason == DeadLetterReason.WORKER_CRASH_LOOP


async def test_h_10_an_ordinary_exhausted_failure_is_not_poison(execution, db_session):
    """The row that carries the whole idea. A webhook that returned 500 three
    times did its work; the thing it called was down. Retrying it once that
    recovers is exactly right, so it must stay retryable."""
    job = await failed_job(execution, db_session)

    assert job.status == JobStatus.FAILED
    assert job.dead_letter_reason is None


async def test_h_10b_an_ordinary_failure_stays_retryable(repository, execution, db_session):
    job = await failed_job(execution, db_session)

    assert await repository.retry_failed(job.id) is not None


# ---------------------------------------------------------------------------
# H-11 .. H-12 — the constraint and the exposure
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", [JobStatus.PENDING, JobStatus.COMPLETED, JobStatus.CANCELLED])
async def test_h_11_a_dead_letter_reason_requires_a_failed_job(db_session, status):
    """A dead letter is always a failed job, enforced by the database rather
    than by remembering."""
    extra: dict = {}
    if status is JobStatus.COMPLETED:
        extra = {"started_at": datetime.now(UTC), "completed_at": datetime.now(UTC)}

    with pytest.raises(IntegrityError):
        await add_job(
            db_session,
            status=status,
            dead_letter_reason=DeadLetterReason.UNPROCESSABLE_PAYLOAD,
            **extra,
        )


async def test_h_11b_the_reason_must_be_one_the_system_knows(db_session):
    with pytest.raises(IntegrityError):
        await add_job(db_session, status=JobStatus.FAILED, dead_letter_reason="something_invented")


async def test_h_12_the_dead_letter_queue_can_be_listed(repository, execution, db_session):
    poison, own = await claimed(execution, db_session, payload={"unusable": "shape"})
    await execution.fail(poison, own, JobExecutionError("nope"), retryable=False)
    ordinary = await failed_job(execution, db_session)
    await db_session.flush()

    dead, _ = await repository.list_jobs(JobFilters(dead_lettered=True), 50, 0)
    alive, _ = await repository.list_jobs(JobFilters(dead_lettered=False), 50, 0)

    assert [job.id for job in dead] == [poison.id]
    assert ordinary.id in [job.id for job in alive]


async def test_h_12b_dead_letters_are_counted_for_health(repository, execution, db_session):
    assert await repository.count_dead_lettered() == 0

    job, own = await claimed(execution, db_session, payload={"unusable": "shape"})
    await execution.fail(job, own, JobExecutionError("nope"), retryable=False)

    assert await repository.count_dead_lettered() == 1


async def test_no_filter_returns_everything(repository, execution, db_session):
    poison, own = await claimed(execution, db_session, payload={"unusable": "shape"})
    await execution.fail(poison, own, JobExecutionError("nope"), retryable=False)
    await failed_job(execution, db_session)
    await db_session.flush()

    jobs, _ = await repository.list_jobs(JobFilters(), 50, 0)

    assert len(jobs) == 2
