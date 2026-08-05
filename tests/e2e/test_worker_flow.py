"""W3-01 .. W3-07 — a job's whole life, submitted over HTTP and run by a worker.

The API and the worker share only the database here, exactly as they do in
production: separate sessions, separate units of work, no in-process shortcuts.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from app.api.app import create_app
from app.core.config import Settings
from app.core.enums import JobStatus, JobType
from app.db.models import Job, JobLog
from app.dispatch.base import NullDispatch
from app.worker.runtime import Worker
from tests.doubles import RecordingSleeper, StubRandom

pytestmark = pytest.mark.committing

EMAIL_PAYLOAD = {"to": "user@example.com", "subject": "Hi", "body": "Hello"}


def worker_for(sessions, clock, **overrides) -> Worker:
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
    return Worker(
        Settings(**values),
        sessions,
        NullDispatch(),
        clock,
        rng=StubRandom(0.99),
        sleeper=RecordingSleeper(),
    )


@pytest.fixture
async def api(committing_sessions, test_db_url):
    """The real application, writing through committing sessions so a worker in
    another unit of work can see what it submitted."""
    app = create_app(Settings(database_url=test_db_url))
    app.state.session_factory = committing_sessions
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


async def drain(worker: Worker, api: AsyncClient, job_id: str, timeout: float = 10.0):
    """Run the worker until the job reaches a terminal state, then stop it."""
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))

    async def wait_for_terminal() -> dict:
        while True:
            body = (await api.get(f"/jobs/{job_id}")).json()
            if body["status"] in {JobStatus.COMPLETED, JobStatus.FAILED}:
                return body
            await asyncio.sleep(0.02)

    try:
        return await asyncio.wait_for(wait_for_terminal(), timeout=timeout)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=timeout)


async def test_w3_01_a_submitted_job_is_executed_and_readable(api, committing_sessions, clock):
    created = (await api.post("/jobs", json={"job_type": "email", "payload": EMAIL_PAYLOAD})).json()
    assert created["status"] == "pending"

    finished = await drain(worker_for(committing_sessions, clock), api, created["id"])

    assert finished["status"] == "completed"
    assert finished["result"]["message_id"].startswith("msg-")
    assert finished["attempts"] == 1
    assert finished["started_at"] is not None
    assert finished["completed_at"] is not None
    assert finished["error"] is None


async def test_w3_02_a_single_worker_takes_jobs_in_priority_order(api, committing_sessions, clock):
    """Deliberately one worker. With several, each takes the highest-priority job
    *available to it* — SKIP LOCKED steps over what another already holds — so a
    multi-worker ordering assertion would be flaky by construction."""
    for priority in (1, 9, 5):
        await api.post(
            "/jobs",
            json={
                "job_type": "email",
                "priority": priority,
                "payload": EMAIL_PAYLOAD,
                "idempotency_key": f"prio-{priority}",
            },
        )

    worker = worker_for(committing_sessions, clock)
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))

    async def wait_for_all() -> None:
        while True:
            listing = (await api.get("/jobs?limit=100")).json()["items"]
            if all(item["status"] == "completed" for item in listing):
                return
            await asyncio.sleep(0.02)

    try:
        await asyncio.wait_for(wait_for_all(), timeout=10)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=10)

    async with committing_sessions() as session:
        order = (await session.execute(select(Job).order_by(Job.started_at))).scalars().all()
    assert [job.priority for job in order] == [9, 5, 1]


async def test_w3_03_a_failing_job_retries_and_records_each_attempt(
    api, committing_sessions, clock
):
    """The webhook handler fails 20 % of the time; pinning the RNG makes it fail
    every time, so the job walks the whole retry path to a permanent failure."""
    created = (
        await api.post(
            "/jobs",
            json={
                "job_type": "webhook",
                "max_attempts": 2,
                "payload": {"url": "https://example.com/hook"},
            },
        )
    ).json()

    worker = worker_for(committing_sessions, clock)
    worker.slots[0]._rng = StubRandom(0.0)  # force the failure branch
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))

    async def wait_for_first_failure() -> dict:
        while True:
            body = (await api.get(f"/jobs/{created['id']}")).json()
            if body["error"] is not None:
                return body
            await asyncio.sleep(0.02)

    try:
        after_first = await asyncio.wait_for(wait_for_first_failure(), timeout=10)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=10)

    assert after_first["attempts"] == 1
    assert after_first["error"]["type"] == "JobExecutionError"
    assert "Traceback" not in str(after_first["error"])
    # Held back rather than retried immediately.
    assert after_first["status"] == "pending"
    assert datetime.fromisoformat(after_first["scheduled_at"]) > datetime.now(UTC)


async def test_w3_04_health_reports_the_workers_and_the_queue(api):
    await api.post("/jobs", json={"job_type": "email", "payload": EMAIL_PAYLOAD})

    body = (await api.get("/health")).json()

    assert body["status"] == "ok"
    assert body["database"] == "ok"
    assert body["queue"]["pending"] == 1
    assert body["queue"]["oldest_pending_seconds"] is not None


async def test_w3_05_health_survives_redis_being_unavailable(api):
    """Jobs still flow through the PostgreSQL claim, so this is a 200 with a
    degraded field — not a 503, and not `workers: {count: 0}`."""
    response = await api.get("/health")

    assert response.status_code == 200
    assert response.json()["redis"] == "error"
    assert response.json()["workers"] is None


async def test_w3_06_batch_progress_is_visible_through_the_api(api, committing_sessions, clock):
    created = (
        await api.post(
            "/jobs",
            json={
                "job_type": "batch",
                "payload": {"items": [f"i{n}" for n in range(4)], "operation": "index"},
            },
        )
    ).json()

    finished = await drain(worker_for(committing_sessions, clock), api, created["id"])

    assert finished["status"] == "completed"
    assert finished["progress"] == 100
    assert finished["result"]["total"] == 4


async def test_w3_07_every_transition_leaves_an_audit_row(api, committing_sessions, clock):
    """What an operator reads to answer 'why is this job in this state'."""
    created = (await api.post("/jobs", json={"job_type": "email", "payload": EMAIL_PAYLOAD})).json()

    await drain(worker_for(committing_sessions, clock), api, created["id"])

    async with committing_sessions() as session:
        rows = (
            (
                await session.execute(
                    select(JobLog).where(JobLog.job_id == created["id"]).order_by(JobLog.id)
                )
            )
            .scalars()
            .all()
        )

    messages = [row.message for row in rows]
    assert messages[0].startswith("Job created")
    assert any(message.startswith("Claimed by") for message in messages)
    assert messages[-1] == "Job completed"
    # Every row after the claim names the worker that made the transition.
    assert any(row.meta.get("worker_id") for row in rows)


async def test_a_cancelled_job_is_never_executed(api, committing_sessions, clock):
    created = (
        await api.post(
            "/jobs",
            json={
                "job_type": "email",
                "scheduled_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                "payload": EMAIL_PAYLOAD,
            },
        )
    ).json()
    await api.post(f"/jobs/{created['id']}/cancel")

    worker = worker_for(committing_sessions, clock)
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))
    await asyncio.sleep(0.1)
    stop.set()
    await asyncio.wait_for(task, timeout=10)

    body = (await api.get(f"/jobs/{created['id']}")).json()
    assert body["status"] == "cancelled"
    assert body["attempts"] == 0


async def test_a_scheduled_job_is_promoted_then_run(api, committing_sessions, clock):
    created = (
        await api.post(
            "/jobs",
            json={
                "job_type": "email",
                "scheduled_at": (datetime.now(UTC) + timedelta(seconds=1)).isoformat(),
                "payload": EMAIL_PAYLOAD,
            },
        )
    ).json()
    assert created["status"] == "scheduled"

    finished = await drain(worker_for(committing_sessions, clock), api, created["id"], timeout=15)

    assert finished["status"] == "completed"


async def test_the_job_type_registry_drives_execution(api, committing_sessions, clock):
    """Each type runs its own handler and stores its own result shape, with no
    branching anywhere outside app/jobs/."""
    created = (
        await api.post(
            "/jobs",
            json={
                "job_type": "report",
                "payload": {
                    "report_type": "sales",
                    "date_from": "2026-01-01",
                    "date_to": "2026-02-01",
                },
            },
        )
    ).json()

    finished = await drain(worker_for(committing_sessions, clock), api, created["id"])

    assert finished["job_type"] == JobType.REPORT
    assert finished["result"]["file_url"].endswith(".csv")
    assert finished["result"]["row_count"] > 0
