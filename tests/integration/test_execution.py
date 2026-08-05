"""W2-12 .. W2-14 — the worker's use cases against a real PostgreSQL."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from structlog.testing import capture_logs

from app.core.enums import DeadLetterReason, JobStatus
from app.db.models import JobLog
from app.db.repository import Ownership
from app.jobs.base import JobExecutionError
from app.jobs.email_job import EmailResult
from app.services.backoff import BASE_DELAY_SECONDS, GROWTH_FACTOR
from app.services.execution_service import MAX_ERROR_MESSAGE_CHARS
from tests.factories import add_job

LEASE = 60
RESULT = EmailResult(message_id="msg-abc")


async def claimed(execution, db_session, **overrides):
    await add_job(db_session, **overrides)
    job = await execution.claim("w-0", LEASE)
    assert job is not None
    return job, Ownership.of(job)


async def logs_for(db_session, job_id) -> list[JobLog]:
    rows = await db_session.execute(select(JobLog).where(JobLog.job_id == job_id))
    return list(rows.scalars().all())


# ---------------------------------------------------------------------------
# Claiming
# ---------------------------------------------------------------------------


async def test_claim_records_the_transition(execution, db_session):
    with capture_logs() as entries:
        job, _ = await claimed(execution, db_session)

    events = [entry["event"] for entry in entries]
    assert "job.claimed" in events
    assert [row.message for row in await logs_for(db_session, job.id)] == ["Claimed by w-0"]


async def test_claim_reports_how_long_the_job_waited(execution, db_session):
    await add_job(db_session, created_at=datetime.now(UTC) - timedelta(seconds=30))

    with capture_logs() as entries:
        await execution.claim("w-0", LEASE)

    claim_event = next(e for e in entries if e["event"] == "job.claimed")
    assert claim_event["queued_seconds"] >= 30


async def test_claim_prefers_the_hinted_job(execution, db_session):
    await add_job(db_session, priority=9)
    hinted = await add_job(db_session, priority=0)

    job = await execution.claim("w-0", LEASE, hint=hinted.id)

    assert job is not None and job.id == hinted.id


async def test_a_stale_hint_falls_through_to_a_scan(execution, db_session):
    cancelled = await add_job(db_session, status=JobStatus.CANCELLED)
    other = await add_job(db_session)

    job = await execution.claim("w-0", LEASE, hint=cancelled.id)

    assert job is not None and job.id == other.id


async def test_claim_returns_none_when_there_is_nothing(execution):
    assert await execution.claim("w-0", LEASE) is None


# ---------------------------------------------------------------------------
# Completion
# ---------------------------------------------------------------------------


async def test_complete_stores_the_result(execution, db_session):
    job, own = await claimed(execution, db_session)

    assert await execution.complete(job, own, RESULT) is True

    await db_session.refresh(job)
    assert job.status == JobStatus.COMPLETED
    assert job.result == {"message_id": "msg-abc"}
    assert job.completed_at is not None
    assert job.worker_id is None and job.lease_until is None


async def test_complete_logs_the_status_it_moved_to(execution, db_session):
    job, own = await claimed(execution, db_session)

    with capture_logs() as entries:
        await execution.complete(job, own, RESULT)

    completed = next(e for e in entries if e["event"] == "job.completed")
    # The in-memory row still says "processing"; the log must not.
    assert completed["status"] == JobStatus.COMPLETED


async def test_complete_after_losing_the_job_writes_nothing(execution, db_session):
    job, own = await claimed(execution, db_session)
    stale = Ownership(job_id=own.job_id, worker_id=own.worker_id, attempts=0)

    with capture_logs() as entries:
        assert await execution.complete(job, stale, RESULT) is False

    assert any(e["event"] == "job.lease_lost" for e in entries)
    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING
    assert job.result is None


# ---------------------------------------------------------------------------
# W2-12 / W2-13 — failure, retry, exhaustion
# ---------------------------------------------------------------------------


async def test_w2_12_a_failed_attempt_is_rescheduled(execution, db_session):
    job, own = await claimed(execution, db_session, max_attempts=3)

    before = datetime.now(UTC)
    assert await execution.fail(job, own, JobExecutionError("upstream said no")) is True

    await db_session.refresh(job)
    assert job.status == JobStatus.PENDING
    # The deadline is computed by the database, so it is asserted as a window
    # around real time rather than against the frozen application clock.
    assert job.scheduled_at is not None
    assert before + timedelta(seconds=BASE_DELAY_SECONDS - 1) <= job.scheduled_at
    assert job.scheduled_at <= before + timedelta(seconds=BASE_DELAY_SECONDS + 5)
    assert job.error["type"] == "JobExecutionError"
    assert job.error["attempt"] == 1
    assert job.worker_id is None and job.lease_until is None


async def test_w2_12b_a_rescheduled_job_is_not_claimable_yet(execution, db_session):
    job, own = await claimed(execution, db_session, max_attempts=3)
    await execution.fail(job, own, JobExecutionError("boom"))

    assert await execution.claim("w-1", LEASE) is None


async def test_w2_12c_the_second_delay_is_longer(execution, db_session):
    job, own = await claimed(execution, db_session, attempts=1, max_attempts=3)
    # The claim above took it to attempt 2.
    assert job.attempts == 2

    before = datetime.now(UTC)
    await execution.fail(job, own, JobExecutionError("boom"))

    await db_session.refresh(job)
    expected = BASE_DELAY_SECONDS * GROWTH_FACTOR
    assert job.scheduled_at is not None
    assert job.scheduled_at >= before + timedelta(seconds=expected - 1)
    assert job.scheduled_at <= before + timedelta(seconds=expected + 5)


async def test_w2_13_the_last_attempt_fails_permanently(execution, db_session):
    job, own = await claimed(execution, db_session, attempts=2, max_attempts=3)
    assert job.attempts == 3

    assert await execution.fail(job, own, JobExecutionError("final")) is True

    await db_session.refresh(job)
    assert job.status == JobStatus.FAILED
    assert job.completed_at is not None
    assert job.result is None
    assert job.scheduled_at is None


async def test_w2_13b_permanent_failure_is_logged_at_error(execution, db_session):
    job, own = await claimed(execution, db_session, attempts=2, max_attempts=3)

    with capture_logs() as entries:
        await execution.fail(job, own, JobExecutionError("final"))

    failed = next(e for e in entries if e["event"] == "job.failed")
    assert failed["log_level"] == "error"
    assert failed["error_type"] == "JobExecutionError"


async def test_a_retryable_failure_is_logged_at_warning(execution, db_session):
    job, own = await claimed(execution, db_session, max_attempts=3)

    with capture_logs() as entries:
        await execution.fail(job, own, JobExecutionError("transient"))

    attempt = next(e for e in entries if e["event"] == "job.failed_attempt")
    assert attempt["log_level"] == "warning"
    assert attempt["retry_in_seconds"] == BASE_DELAY_SECONDS


async def test_failing_after_losing_the_job_writes_nothing(execution, db_session):
    job, own = await claimed(execution, db_session, max_attempts=3)
    stale = Ownership(job_id=own.job_id, worker_id=own.worker_id, attempts=0)

    assert await execution.fail(job, stale, JobExecutionError("boom")) is False

    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING
    assert job.error is None


async def test_final_failure_after_losing_the_job_writes_nothing(execution, db_session):
    """The exhausted branch needs the same ownership check as the retry branch —
    a displaced worker must not be able to mark a job permanently failed."""
    job, own = await claimed(execution, db_session, attempts=2, max_attempts=3)
    stale = Ownership(job_id=own.job_id, worker_id=own.worker_id, attempts=0)

    with capture_logs() as entries:
        assert await execution.fail(job, stale, JobExecutionError("final")) is False

    assert any(e["event"] == "job.lease_lost" for e in entries)
    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING
    assert job.error is None


async def test_error_messages_are_truncated(execution, db_session):
    job, own = await claimed(execution, db_session, max_attempts=3)

    await execution.fail(job, own, JobExecutionError("x" * 5000))

    await db_session.refresh(job)
    assert len(job.error["message"]) == MAX_ERROR_MESSAGE_CHARS


async def test_stored_errors_never_carry_a_traceback(execution, db_session):
    job, own = await claimed(execution, db_session, max_attempts=3)

    try:
        raise ValueError("something went wrong inside the handler")
    except ValueError as exc:
        await execution.fail(job, own, exc)

    await db_session.refresh(job)
    assert job.error["type"] == "ValueError"
    assert "Traceback" not in str(job.error)
    assert 'File "' not in str(job.error)


# ---------------------------------------------------------------------------
# W2-14 — progress and lease
# ---------------------------------------------------------------------------


async def test_w2_14_progress_is_visible_while_the_job_runs(execution, db_session):
    job, own = await claimed(execution, db_session)

    assert await execution.report_progress(own, 42) is True

    await db_session.refresh(job)
    assert job.progress == 42


async def test_w2_14b_progress_from_a_displaced_worker_is_ignored(execution, db_session):
    job, own = await claimed(execution, db_session)
    stale = Ownership(job_id=own.job_id, worker_id=own.worker_id, attempts=0)

    assert await execution.report_progress(stale, 99) is False

    await db_session.refresh(job)
    assert job.progress == 0


async def test_extend_lease_pushes_the_deadline_out(execution, db_session):
    job, own = await claimed(execution, db_session)
    original = job.lease_until

    assert await execution.extend_lease(own, LEASE * 4) is True

    await db_session.refresh(job)
    assert original is not None and job.lease_until is not None
    assert job.lease_until > original


async def test_release_hands_the_job_back_and_says_so(execution, db_session):
    job, own = await claimed(execution, db_session)

    with capture_logs() as entries:
        assert await execution.release(own) is True

    await db_session.refresh(job)
    assert job.status == JobStatus.PENDING
    assert any(e["event"] == "worker.forced_release" for e in entries)


async def test_release_without_ownership_is_silent(execution, db_session):
    _, own = await claimed(execution, db_session)
    stale = Ownership(job_id=own.job_id, worker_id=own.worker_id, attempts=0)

    with capture_logs() as entries:
        assert await execution.release(stale) is False

    assert not any(e["event"] == "worker.forced_release" for e in entries)


# ---------------------------------------------------------------------------
# Maintenance
# ---------------------------------------------------------------------------


async def test_sweep_reports_and_records_everything_it_touched(execution, db_session, clock):
    job, _ = await claimed(execution, db_session)
    await db_session.execute(JobLog.__table__.delete().where(JobLog.job_id == job.id))
    job.lease_until = clock.now() - timedelta(minutes=5)
    await db_session.flush()

    due = await add_job(
        db_session,
        status=JobStatus.SCHEDULED,
        scheduled_at=datetime.now(UTC) - timedelta(minutes=1),
    )

    with capture_logs() as entries:
        result = await execution.sweep(50)

    assert result.released == [job.id]
    assert result.promoted == [due.id]
    assert result.total == 2

    events = {e["event"] for e in entries}
    assert {"job.reaped", "job.promoted"} <= events
    assert [row.message for row in await logs_for(db_session, job.id)] == [
        "Lease expired; returned to the queue"
    ]


async def test_sweep_records_an_exhausted_job_as_a_permanent_failure(execution, db_session, clock):
    job, _ = await claimed(execution, db_session, attempts=2, max_attempts=3)
    job.lease_until = clock.now() - timedelta(minutes=5)
    await db_session.flush()

    with capture_logs() as entries:
        result = await execution.sweep(50)

    assert result.failed == [job.id]
    dead_lettered = next(e for e in entries if e["event"] == "job.dead_lettered")
    assert dead_lettered["log_level"] == "error"

    await db_session.refresh(job)
    assert job.error["type"] == "LeaseExpired"
    # It used every attempt without a worker surviving to report anything —
    # the clearest poison there is.
    assert job.dead_letter_reason == DeadLetterReason.WORKER_CRASH_LOOP


async def test_an_idle_sweep_changes_nothing(execution):
    result = await execution.sweep(50)

    assert result.total == 0


async def test_a_handler_note_is_recorded_apart_from_transitions(execution, db_session):
    """Lines a handler chooses to write share the audit trail but carry their
    own event name, so they can be told apart from state changes."""
    job, _ = await claimed(execution, db_session)

    with capture_logs() as entries:
        await execution.note(job.id, "warning", "upstream was slow", latency_ms=4200)

    note = next(e for e in entries if e["event"] == "job.note")
    assert note["log_level"] == "warning"
    assert note["latency_ms"] == 4200
    assert "upstream was slow" in [row.message for row in await logs_for(db_session, job.id)]


@pytest.mark.parametrize("percent", [0, 100])
async def test_progress_accepts_its_bounds(execution, db_session, percent):
    _, own = await claimed(execution, db_session)

    assert await execution.report_progress(own, percent) is True
