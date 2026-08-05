"""Row builders shared across test modules.

Kept here rather than duplicated per file: several suites need a job in a status
the submission path cannot produce, and each copy would drift.
"""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import JobStatus, JobType
from app.db.models import Job
from app.db.repository import NewJob

EMAIL_PAYLOAD = {"to": "user@example.com", "subject": "Hi", "body": "Hello"}
BATCH_PAYLOAD = {"items": ["a", "b"], "operation": "index"}


def new_job(**overrides: Any) -> NewJob:
    """A submission candidate, as the service would build it."""
    values: dict[str, Any] = {
        "job_type": JobType.EMAIL,
        "payload": EMAIL_PAYLOAD,
        "status": JobStatus.PENDING,
        "priority": 5,
        "max_attempts": 3,
        "scheduled_at": None,
        "idempotency_key": None,
    }
    values.update(overrides)
    return NewJob(**values)


def build_job(**overrides: Any) -> Job:
    """A row in any status, including ones only the worker can create."""
    values: dict[str, Any] = {
        "job_type": JobType.EMAIL,
        "payload": EMAIL_PAYLOAD,
        "status": JobStatus.PENDING,
    }
    values.update(overrides)
    return Job(**values)


async def add_job(session: AsyncSession, **overrides: Any) -> Job:
    """Insert a row directly and flush so it is visible to the next statement."""
    job = build_job(**overrides)
    session.add(job)
    await session.flush()
    return job


def build_claimed_job(**overrides: Any) -> Job:
    """A job as it looks the instant a claim returns it, with no database behind
    it. For unit tests of code that only reads the row."""
    values: dict[str, Any] = {
        "id": uuid4(),
        "status": JobStatus.PROCESSING,
        "worker_id": "w-test-0",
        "attempts": 1,
        "max_attempts": 3,
        "created_at": datetime.now(UTC),
        "started_at": datetime.now(UTC),
        "lease_until": datetime.now(UTC) + timedelta(seconds=60),
    }
    values.update(overrides)
    return build_job(**values)


async def add_processing_job(
    session: AsyncSession,
    *,
    worker_id: str,
    lease_until: datetime,
    **overrides: Any,
) -> Job:
    """A claimed job. ck_jobs_processing_has_lease forbids one without a lease,
    so both fields are required rather than optional."""
    return await add_job(
        session,
        status=JobStatus.PROCESSING,
        worker_id=worker_id,
        lease_until=lease_until,
        started_at=overrides.pop("started_at", lease_until),
        **overrides,
    )
