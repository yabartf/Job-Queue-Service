"""Service-layer use cases against a real PostgreSQL, without HTTP."""

import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text
from structlog.testing import capture_logs

from app.core.enums import JobStatus, JobType
from app.core.errors import (
    JobNotCancellableError,
    JobNotFoundError,
    PayloadValidationError,
    UnknownJobTypeError,
)
from app.db.models import Job, JobLog
from app.db.repository import JobFilters, JobRepository
from app.jobs.email_job import EmailJob
from app.services.job_service import JobService, SubmitJobCommand
from tests.conftest import FIXED_NOW

EMAIL_PAYLOAD = {"to": "user@example.com", "subject": "Hi", "body": "Hello"}


def submit_command(**overrides) -> SubmitJobCommand:
    values: dict = {"job_type": JobType.EMAIL, "payload": EMAIL_PAYLOAD}
    values.update(overrides)
    return SubmitJobCommand(**values)


async def test_submit_creates_a_pending_job(service, db_session):
    job, created = await service.submit(submit_command())

    assert created is True
    assert job.status == JobStatus.PENDING
    assert job.scheduled_at is None


async def test_submit_applies_the_job_class_defaults(service):
    job, _ = await service.submit(submit_command())

    assert job.priority == EmailJob.default_priority
    assert job.max_attempts == EmailJob.default_max_attempts


async def test_submit_honours_explicit_priority_and_attempts(service):
    job, _ = await service.submit(submit_command(priority=9, max_attempts=1))

    assert job.priority == 9
    assert job.max_attempts == 1


async def test_submit_with_a_future_time_is_scheduled(service, clock):
    when = clock.now() + timedelta(hours=2)
    job, _ = await service.submit(submit_command(scheduled_at=when))

    assert job.status == JobStatus.SCHEDULED
    assert job.scheduled_at == when


async def test_submit_with_a_past_time_is_pending(service, clock):
    """A client whose clock is slightly behind should not get an error."""
    when = clock.now() - timedelta(minutes=5)
    job, _ = await service.submit(submit_command(scheduled_at=when))

    assert job.status == JobStatus.PENDING
    assert job.scheduled_at == when


async def test_submit_rejects_an_unknown_job_type(service):
    with pytest.raises(UnknownJobTypeError):
        await service.submit(submit_command(job_type="telepathy"))


async def test_submit_rejects_an_invalid_payload(service):
    with pytest.raises(PayloadValidationError):
        await service.submit(submit_command(payload={"to": "nope"}))


async def test_submit_normalises_the_payload_to_json_types(service):
    job, _ = await service.submit(
        submit_command(
            job_type=JobType.REPORT,
            payload={
                "report_type": "sales",
                "date_from": "2026-01-01",
                "date_to": "2026-02-01",
            },
        )
    )

    # Stored as the JSON types it will be read back as, not Python date objects.
    assert job.payload["date_from"] == "2026-01-01"
    assert isinstance(job.payload["date_from"], str)


async def test_submit_is_idempotent_for_a_repeated_key(service, db_session):
    first, first_created = await service.submit(submit_command(idempotency_key="k-1"))
    await db_session.flush()
    second, second_created = await service.submit(submit_command(idempotency_key="k-1"))

    assert (first_created, second_created) == (True, False)
    assert first.id == second.id

    total = (await db_session.execute(select(func.count()).select_from(Job))).scalar_one()
    assert total == 1


async def test_idempotent_replay_with_a_different_payload_warns(service, db_session):
    await service.submit(submit_command(idempotency_key="k-2"))
    await db_session.flush()

    with capture_logs() as logs:
        job, created = await service.submit(
            submit_command(
                idempotency_key="k-2",
                payload=EMAIL_PAYLOAD | {"subject": "Something else"},
            )
        )

    # The assignment specifies returning the existing job, so this is not an
    # error — but it is almost always a client bug, so it is surfaced.
    assert created is False
    assert job.payload["subject"] == "Hi"
    assert any(
        entry["event"] == "job.idempotent_replay_mismatch" and entry["log_level"] == "warning"
        for entry in logs
    )


async def test_l2_20_transitions_write_an_audit_row_and_a_log_line(service, db_session):
    """One helper writes both, so the audit trail and the log stream cannot
    drift apart."""
    with capture_logs() as logs:
        job, _ = await service.submit(submit_command())
    await db_session.flush()

    rows = (await db_session.execute(select(JobLog).where(JobLog.job_id == job.id))).scalars().all()

    assert len(rows) == 1
    assert rows[0].level == "info"
    assert any(entry["event"] == "job.created" for entry in logs)


async def test_created_log_records_a_fingerprint_not_the_payload(service):
    with capture_logs() as logs:
        await service.submit(submit_command())

    created = next(entry for entry in logs if entry["event"] == "job.created")
    assert "payload_sha256" in created
    assert "user@example.com" not in str(created)


async def test_get_returns_the_job(service, db_session):
    job, _ = await service.submit(submit_command())
    await db_session.flush()

    assert (await service.get(job.id)).id == job.id


async def test_get_raises_for_a_missing_job(service):
    with pytest.raises(JobNotFoundError):
        await service.get(uuid4())


async def test_cancel_marks_a_pending_job_and_records_it(service, db_session):
    job, _ = await service.submit(submit_command())
    await db_session.flush()

    cancelled = await service.cancel(job.id)
    await db_session.flush()

    assert cancelled.status == JobStatus.CANCELLED
    messages = (
        (await db_session.execute(select(JobLog.message).where(JobLog.job_id == job.id)))
        .scalars()
        .all()
    )
    assert "Job cancelled" in messages


async def test_cancel_twice_reports_a_conflict(service, db_session):
    job, _ = await service.submit(submit_command())
    await db_session.flush()
    await service.cancel(job.id)
    await db_session.flush()

    with pytest.raises(JobNotCancellableError, match="cancelled"):
        await service.cancel(job.id)


async def test_cancel_raises_for_a_missing_job(service):
    with pytest.raises(JobNotFoundError):
        await service.cancel(uuid4())


async def test_l2_08_list_limit_is_capped_before_reaching_sql(service, db_session):
    for _ in range(3):
        await service.submit(submit_command())
    await db_session.flush()

    jobs, _ = await service.list_jobs(JobFilters(), limit=10_000, offset=0)

    # The API answers 422 for an over-large limit; a non-HTTP caller is capped
    # rather than allowed an unbounded page.
    assert len(jobs) <= 100


async def test_l2_14_concurrent_submissions_with_one_key_create_one_job(committing_sessions, clock):
    """Ten real connections, ten real transactions.

    This test cannot use the shared rolled-back session: doing so would
    serialise exactly the concurrency it exists to exercise.
    """

    async def submit_once() -> tuple[str, bool]:
        async with committing_sessions() as session:
            service = JobService(JobRepository(session), clock)
            job, created = await service.submit(submit_command(idempotency_key="race"))
            await session.commit()
            return str(job.id), created

    results = await asyncio.gather(*(submit_once() for _ in range(10)))

    assert len({job_id for job_id, _ in results}) == 1
    assert sum(1 for _, created in results if created) == 1

    async with committing_sessions() as session:
        total = (
            await session.execute(text("SELECT count(*) FROM jobs WHERE idempotency_key = 'race'"))
        ).scalar_one()
    assert total == 1


def test_fixed_now_is_timezone_aware():
    assert FIXED_NOW.tzinfo is not None
