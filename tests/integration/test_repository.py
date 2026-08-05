"""L2-01 .. L2-19 — repository statements against a real PostgreSQL."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

from app.core.enums import JobStatus, JobType
from app.db.models import Job, JobLog
from app.db.repository import JobFilters
from tests.factories import EMAIL_PAYLOAD, add_job, new_job


async def test_l2_01_insert_and_read_back(repository, db_session):
    created = await repository.insert_if_absent(
        new_job(idempotency_key="k1", priority=7, scheduled_at=None)
    )
    assert created is not None
    await db_session.flush()

    fetched = await repository.get(created.id)
    assert fetched is not None
    assert fetched.payload == EMAIL_PAYLOAD  # JSONB round-trips unchanged
    assert fetched.priority == 7
    assert fetched.status == JobStatus.PENDING
    assert fetched.attempts == 0
    assert fetched.progress == 0
    assert fetched.result is None
    assert fetched.created_at.tzinfo is not None


async def test_l2_02_missing_id_returns_none(repository):
    assert await repository.get(uuid4()) is None


async def test_l2_03_list_returns_newest_first(repository, db_session):
    for index in range(3):
        await repository.insert_if_absent(new_job(idempotency_key=f"k{index}"))
    await db_session.flush()

    jobs, has_more = await repository.list_jobs(JobFilters(), limit=10, offset=0)
    assert has_more is False
    assert [job.created_at for job in jobs] == sorted(
        (job.created_at for job in jobs), reverse=True
    )


async def test_l2_04_filter_by_status(repository, db_session):
    await add_job(db_session, status=JobStatus.PENDING)
    await add_job(db_session, status=JobStatus.CANCELLED)

    jobs, _ = await repository.list_jobs(JobFilters(status=JobStatus.CANCELLED), limit=10, offset=0)
    assert [job.status for job in jobs] == [JobStatus.CANCELLED]


async def test_l2_05_filter_by_job_type(repository, db_session):
    await add_job(db_session, job_type=JobType.EMAIL)
    await add_job(
        db_session, job_type=JobType.BATCH, payload={"items": ["a"], "operation": "index"}
    )

    jobs, _ = await repository.list_jobs(JobFilters(job_type=JobType.BATCH), limit=10, offset=0)
    assert [job.job_type for job in jobs] == [JobType.BATCH]


async def test_l2_06_filters_combine(repository, db_session):
    await add_job(db_session, job_type=JobType.EMAIL, status=JobStatus.PENDING)
    await add_job(db_session, job_type=JobType.EMAIL, status=JobStatus.CANCELLED)
    await add_job(
        db_session,
        job_type=JobType.BATCH,
        status=JobStatus.CANCELLED,
        payload={"items": ["a"], "operation": "index"},
    )

    jobs, _ = await repository.list_jobs(
        JobFilters(status=JobStatus.CANCELLED, job_type=JobType.EMAIL),
        limit=10,
        offset=0,
    )
    assert len(jobs) == 1
    assert jobs[0].job_type == JobType.EMAIL
    assert jobs[0].status == JobStatus.CANCELLED


async def test_l2_07_pagination_reports_more_and_never_repeats(repository, db_session):
    for _ in range(5):
        await add_job(db_session)

    first, first_more = await repository.list_jobs(JobFilters(), limit=2, offset=0)
    second, second_more = await repository.list_jobs(JobFilters(), limit=2, offset=2)
    third, third_more = await repository.list_jobs(JobFilters(), limit=2, offset=4)

    assert (first_more, second_more, third_more) == (True, True, False)
    assert len(first) == len(second) == 2
    assert len(third) == 1

    seen = [job.id for job in (*first, *second, *third)]
    assert len(seen) == len(set(seen)) == 5


async def test_l2_09_cancel_pending_job(repository, db_session):
    job = await add_job(db_session, status=JobStatus.PENDING)

    cancelled = await repository.cancel(job.id)
    assert cancelled is not None
    assert cancelled.status == JobStatus.CANCELLED


async def test_l2_10_cancel_scheduled_job(repository, db_session):
    job = await add_job(
        db_session,
        status=JobStatus.SCHEDULED,
        scheduled_at=datetime.now(UTC) + timedelta(hours=1),
    )

    cancelled = await repository.cancel(job.id)
    assert cancelled is not None
    assert cancelled.status == JobStatus.CANCELLED


@pytest.mark.parametrize(
    "status", [JobStatus.COMPLETED, JobStatus.CANCELLED, JobStatus.PROCESSING, JobStatus.FAILED]
)
async def test_l2_11_cancel_matches_nothing_in_other_statuses(repository, db_session, status):
    extra: dict = {}
    if status is JobStatus.PROCESSING:
        extra = {
            "worker_id": "w-1",
            "lease_until": datetime.now(UTC) + timedelta(minutes=1),
        }
    if status is JobStatus.COMPLETED:
        extra = {"started_at": datetime.now(UTC), "completed_at": datetime.now(UTC)}
    job = await add_job(db_session, status=status, **extra)

    assert await repository.cancel(job.id) is None

    unchanged = await db_session.get(Job, job.id)
    assert unchanged is not None
    assert unchanged.status == status


async def test_l2_11b_cancel_of_a_missing_job_matches_nothing(repository):
    assert await repository.cancel(uuid4()) is None


@pytest.mark.parametrize(
    ("description", "overrides"),
    [
        ("status not in enum", {"status": "sideways"}),
        ("job_type not in enum", {"job_type": "telepathy"}),
        ("priority above range", {"priority": 10}),
        ("priority below range", {"priority": -1}),
        ("max_attempts above range", {"max_attempts": 11}),
        ("max_attempts below range", {"max_attempts": 0}),
        ("attempts exceeds max_attempts", {"attempts": 4, "max_attempts": 3}),
        ("progress above range", {"progress": 101}),
        ("idempotency key too long", {"idempotency_key": "x" * 256}),
        ("result set while not completed", {"result": {"a": 1}}),
        ("scheduled without a time", {"status": JobStatus.SCHEDULED}),
        (
            "processing without a lease",
            {"status": JobStatus.PROCESSING, "worker_id": "w-1"},
        ),
        (
            "completed without a start",
            {
                "status": JobStatus.COMPLETED,
                "completed_at": datetime.now(UTC),
            },
        ),
    ],
)
async def test_l2_12_check_constraints_reject_impossible_rows(db_session, description, overrides):
    with pytest.raises(IntegrityError):
        await add_job(db_session, **overrides)


async def test_l2_13_duplicate_idempotency_key_declines_the_insert(repository, db_session):
    first = await repository.insert_if_absent(new_job(idempotency_key="order-1"))
    await db_session.flush()
    assert first is not None

    second = await repository.insert_if_absent(new_job(idempotency_key="order-1"))
    assert second is None

    existing = await repository.get_by_idempotency_key("order-1")
    assert existing is not None
    assert existing.id == first.id


async def test_l2_15_null_idempotency_keys_do_not_collide(repository, db_session):
    first = await repository.insert_if_absent(new_job(idempotency_key=None))
    second = await repository.insert_if_absent(new_job(idempotency_key=None))
    await db_session.flush()

    assert first is not None and second is not None
    assert first.id != second.id


async def test_l2_16_updated_at_trigger_fires_for_raw_updates(db_session):
    """The trigger must win over a value the statement supplies, because the
    claim and reaper statements in later specs bypass the ORM entirely."""
    job = await add_job(db_session)

    await db_session.execute(
        text("UPDATE jobs SET priority = 1, updated_at = :stale WHERE id = :id"),
        {"stale": datetime(2000, 1, 1, tzinfo=UTC), "id": job.id},
    )

    stored = (
        await db_session.execute(text("SELECT updated_at FROM jobs WHERE id = :id"), {"id": job.id})
    ).scalar_one()
    assert stored.year != 2000


async def test_l2_18_status_listing_can_use_its_index(db_session):
    """Index usability, asserted without seeding a table large enough for the
    planner to prefer it on its own."""
    await db_session.execute(text("SET LOCAL enable_seqscan = off"))
    plan = (
        (
            await db_session.execute(
                text(
                    "EXPLAIN SELECT * FROM jobs WHERE status = 'pending' "
                    "ORDER BY created_at DESC LIMIT 20"
                )
            )
        )
        .scalars()
        .all()
    )

    assert any("ix_jobs_status_created" in line for line in plan), plan


async def test_l2_19_job_logs_cascade_on_delete(repository, db_session):
    job = await add_job(db_session)
    await repository.add_log(job.id, "info", "created", {"k": "v"})
    await db_session.flush()

    assert (
        await db_session.execute(
            select(func.count()).select_from(JobLog).where(JobLog.job_id == job.id)
        )
    ).scalar_one() == 1

    await db_session.delete(job)
    await db_session.flush()

    assert (
        await db_session.execute(
            select(func.count()).select_from(JobLog).where(JobLog.job_id == job.id)
        )
    ).scalar_one() == 0


async def test_count_by_status_reports_every_status(repository, db_session):
    await add_job(db_session, status=JobStatus.PENDING)
    await add_job(db_session, status=JobStatus.PENDING)
    await add_job(db_session, status=JobStatus.CANCELLED)

    counts = await repository.count_by_status()

    assert set(counts) == {status.value for status in JobStatus}
    assert counts["pending"] == 2
    assert counts["cancelled"] == 1
    assert counts["completed"] == 0
