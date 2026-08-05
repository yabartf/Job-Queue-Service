"""W2-08 .. W2-11 — lease expiry, the reaper, and scheduled promotion."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.exc import IntegrityError

from app.core.enums import JobStatus
from app.db.models import Job
from app.db.repository import Ownership
from tests.factories import add_job, add_processing_job

LEASE = 60
EXPIRY_ERROR = {"type": "LeaseExpired", "message": "Worker stopped extending the lease"}
SHUTDOWN_ERROR = {"type": "WorkerShutdown", "message": "Worker shut down mid-attempt"}


def expired_at() -> datetime:
    return datetime.now(UTC) - timedelta(seconds=1)


def healthy_until() -> datetime:
    return datetime.now(UTC) + timedelta(minutes=5)


async def test_w2_08_expired_leases_return_to_the_queue(repository, db_session):
    job = await add_processing_job(
        db_session, worker_id="w-dead", lease_until=expired_at(), attempts=1
    )

    result = await repository.reap_expired_leases(10, EXPIRY_ERROR)

    assert result.released == [job.id]
    assert result.failed == []
    await db_session.refresh(job)
    assert job.status == JobStatus.PENDING
    assert job.worker_id is None
    assert job.lease_until is None


async def test_w2_09_expired_and_exhausted_jobs_fail_instead(repository, db_session):
    job = await add_processing_job(
        db_session,
        worker_id="w-dead",
        lease_until=expired_at(),
        attempts=3,
        max_attempts=3,
    )

    result = await repository.reap_expired_leases(10, EXPIRY_ERROR)

    assert result.failed == [job.id]
    assert result.released == []
    await db_session.refresh(job)
    assert job.status == JobStatus.FAILED
    assert job.error == EXPIRY_ERROR
    assert job.completed_at is not None


async def test_w2_09b_returning_an_exhausted_job_to_the_queue_would_break_the_claim(
    repository, db_session
):
    """Why the reaper needs two statements rather than one.

    A job put back as `pending` with no attempts left cannot be claimed: the
    claim increments attempts past max_attempts and ck_jobs_attempts rejects it.
    This is the failure the split avoids, demonstrated directly.
    """
    await add_job(db_session, status=JobStatus.PENDING, attempts=3, max_attempts=3)

    with pytest.raises(IntegrityError):
        await repository.claim_next("w-0", LEASE)


async def test_w2_10_a_live_lease_is_left_alone(repository, db_session):
    job = await add_processing_job(
        db_session, worker_id="w-alive", lease_until=healthy_until(), attempts=1
    )

    result = await repository.reap_expired_leases(10, EXPIRY_ERROR)

    assert result.released == [] and result.failed == []
    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING
    assert job.worker_id == "w-alive"


async def test_w2_10b_a_lease_about_to_expire_is_still_live(repository, db_session):
    await add_processing_job(
        db_session,
        worker_id="w-alive",
        lease_until=datetime.now(UTC) + timedelta(seconds=2),
        attempts=1,
    )

    result = await repository.reap_expired_leases(10, EXPIRY_ERROR)

    assert result.released == []


async def test_w2_10c_the_reaper_batch_is_bounded(repository, db_session):
    for _ in range(5):
        await add_processing_job(
            db_session, worker_id="w-dead", lease_until=expired_at(), attempts=1
        )

    first = await repository.reap_expired_leases(2, EXPIRY_ERROR)
    second = await repository.reap_expired_leases(10, EXPIRY_ERROR)

    assert len(first.released) == 2
    assert len(second.released) == 3


async def test_reaped_jobs_are_claimable_again(repository, db_session):
    job = await add_processing_job(
        db_session, worker_id="w-dead", lease_until=expired_at(), attempts=1
    )
    await repository.reap_expired_leases(10, EXPIRY_ERROR)

    reclaimed = await repository.claim_next("w-fresh", LEASE)

    assert reclaimed is not None
    assert reclaimed.id == job.id
    assert reclaimed.attempts == 2  # the dead attempt was counted


async def test_release_lease_hands_a_job_back_without_an_outcome(repository, db_session):
    await add_job(db_session)
    job = await repository.claim_next("w-0", LEASE)
    assert job is not None

    assert await repository.release_lease(Ownership.of(job), SHUTDOWN_ERROR) == JobStatus.PENDING

    await db_session.refresh(job)
    assert job.status == JobStatus.PENDING
    assert job.worker_id is None
    assert job.lease_until is None
    assert job.result is None and job.error is None


async def test_release_lease_after_losing_ownership_does_nothing(repository, db_session):
    await add_job(db_session)
    job = await repository.claim_next("w-0", LEASE)
    assert job is not None
    stale = Ownership(job_id=job.id, worker_id="w-0", attempts=job.attempts - 1)

    assert await repository.release_lease(stale, SHUTDOWN_ERROR) is None

    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING


async def test_w2_16c_releasing_the_final_attempt_fails_it_instead_of_requeueing(
    repository, db_session
):
    """The mirror of the reaper's second statement, and the one that was missing.

    A job released back to `pending` with no attempts left is a landmine: the
    next claim computes attempts + 1 past the limit and ck_jobs_attempts raises.
    Because the claim picks by priority, that one row would break claiming for
    every worker, not just the one that released it.
    """
    await add_job(db_session, max_attempts=1)
    job = await repository.claim_next("w-0", LEASE)
    assert job is not None
    assert job.attempts == job.max_attempts  # nothing left to retry with

    assert await repository.release_lease(Ownership.of(job), SHUTDOWN_ERROR) == JobStatus.FAILED

    await db_session.refresh(job)
    assert job.status == JobStatus.FAILED
    assert job.error == SHUTDOWN_ERROR
    assert job.worker_id is None and job.lease_until is None


async def test_w2_16d_a_released_final_attempt_does_not_wedge_the_claim(repository, db_session):
    """The failure this exists to prevent, asserted end to end."""
    await add_job(db_session, max_attempts=1)
    job = await repository.claim_next("w-0", LEASE)
    assert job is not None
    await repository.release_lease(Ownership.of(job), SHUTDOWN_ERROR)
    await add_job(db_session, priority=0)  # a later job the wedge would have hidden

    claimed = await repository.claim_next("w-1", LEASE)

    assert claimed is not None
    assert claimed.id != job.id  # the exhausted job is out of the queue, not in it


async def test_w2_16e_a_shutdown_failure_is_not_poison(repository, db_session):
    """Nothing about the job made it fail, so a manual retry must still work."""
    await add_job(db_session, max_attempts=1)
    job = await repository.claim_next("w-0", LEASE)
    assert job is not None
    await repository.release_lease(Ownership.of(job), SHUTDOWN_ERROR)

    await db_session.refresh(job)
    assert job.dead_letter_reason is None
    assert await repository.retry_failed(job.id) is not None


# ---------------------------------------------------------------------------
# Promotion of scheduled jobs
# ---------------------------------------------------------------------------


async def test_w2_11_due_scheduled_jobs_are_promoted(repository, db_session):
    due = await add_job(
        db_session,
        status=JobStatus.SCHEDULED,
        scheduled_at=datetime.now(UTC) - timedelta(minutes=1),
    )

    promoted = await repository.promote_due_scheduled(10)

    assert promoted == [due.id]
    await db_session.refresh(due)
    assert due.status == JobStatus.PENDING
    assert due.scheduled_at is not None  # kept: it records what was asked for


async def test_w2_11b_jobs_not_yet_due_are_untouched(repository, db_session):
    later = await add_job(
        db_session,
        status=JobStatus.SCHEDULED,
        scheduled_at=datetime.now(UTC) + timedelta(hours=1),
    )

    assert await repository.promote_due_scheduled(10) == []

    await db_session.refresh(later)
    assert later.status == JobStatus.SCHEDULED


async def test_w2_11c_promotion_makes_a_job_claimable(repository, db_session):
    await add_job(
        db_session,
        status=JobStatus.SCHEDULED,
        scheduled_at=datetime.now(UTC) - timedelta(minutes=1),
    )
    assert await repository.claim_next("w-0", LEASE) is None

    await repository.promote_due_scheduled(10)

    assert await repository.claim_next("w-0", LEASE) is not None


async def test_w2_11d_promotion_is_ordered_by_due_time(repository, db_session):
    base = datetime.now(UTC) - timedelta(hours=1)
    later = await add_job(
        db_session, status=JobStatus.SCHEDULED, scheduled_at=base + timedelta(minutes=30)
    )
    earlier = await add_job(db_session, status=JobStatus.SCHEDULED, scheduled_at=base)

    assert await repository.promote_due_scheduled(1) == [earlier.id]
    assert await repository.promote_due_scheduled(1) == [later.id]


async def test_oldest_pending_age_is_none_when_nothing_waits(repository, db_session):
    await add_processing_job(db_session, worker_id="w-0", lease_until=healthy_until())

    assert await repository.oldest_pending_age_seconds() is None


async def test_oldest_pending_age_tracks_the_oldest_job(repository, db_session):
    await add_job(db_session, created_at=datetime.now(UTC) - timedelta(minutes=10))
    await add_job(db_session)

    age = await repository.oldest_pending_age_seconds()

    # `created_at` is written from the application clock and the age is computed
    # from the database's, so an exact bound would be comparing two clocks — the
    # very thing the production code avoids by never mixing them. A second of
    # tolerance is far tighter than the ten minutes being asserted.
    assert age is not None
    assert age > 599
    assert age < 620  # it picked the old job, not the fresh one


async def test_count_by_status_sees_worker_states(repository, db_session):
    await add_processing_job(db_session, worker_id="w-0", lease_until=healthy_until())
    await add_job(db_session, status=JobStatus.FAILED, error={"type": "X"})

    counts = await repository.count_by_status()

    assert counts["processing"] == 1
    assert counts["failed"] == 1


async def test_reap_leaves_unrelated_jobs_alone(repository, db_session):
    pending = await add_job(db_session)

    await repository.reap_expired_leases(10, EXPIRY_ERROR)

    stored = await db_session.get(Job, pending.id)
    assert stored is not None
    assert stored.status == JobStatus.PENDING
