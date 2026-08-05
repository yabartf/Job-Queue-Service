"""E2E-01 .. E2E-37 — the HTTP contract."""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from app.api.app import create_app
from app.api.deps import get_job_service, get_session
from app.api.schemas import JobResponse
from app.core.config import Settings
from app.core.enums import DeadLetterReason, JobStatus, JobType
from app.db.repository import Ownership
from app.jobs.base import JobExecutionError
from tests.factories import add_job

EMAIL_PAYLOAD = {"to": "user@example.com", "subject": "Hi", "body": "Hello"}

VALID_PAYLOADS = {
    JobType.EMAIL: EMAIL_PAYLOAD,
    JobType.WEBHOOK: {"url": "https://example.com/hook"},
    JobType.REPORT: {
        "report_type": "sales",
        "date_from": "2026-01-01",
        "date_to": "2026-02-01",
    },
    JobType.BATCH: {"items": ["a", "b"], "operation": "index"},
}

REQUIRED_FIELDS = {
    JobType.EMAIL: "subject",
    JobType.WEBHOOK: "url",
    JobType.REPORT: "report_type",
    JobType.BATCH: "items",
}


def body(**overrides) -> dict:
    payload = {"job_type": "email", "payload": EMAIL_PAYLOAD}
    payload.update(overrides)
    return payload


async def submit(client, **overrides):
    return await client.post("/jobs", json=body(**overrides))


# --------------------------------------------------------------------------
# Submission
# --------------------------------------------------------------------------


async def test_e2e_01_submit_a_valid_job(client):
    response = await submit(client)

    assert response.status_code == 201
    payload = response.json()
    assert payload["status"] == "pending"
    assert payload["attempts"] == 0
    assert response.headers["Location"] == f"/jobs/{payload['id']}"

    follow_up = await client.get(response.headers["Location"])
    assert follow_up.status_code == 200


@pytest.mark.parametrize("job_type", list(JobType))
async def test_e2e_01b_every_job_type_can_be_submitted(client, job_type):
    response = await submit(client, job_type=job_type.value, payload=VALID_PAYLOADS[job_type])
    assert response.status_code == 201
    assert response.json()["job_type"] == job_type.value


async def test_e2e_02_future_schedule_starts_scheduled(client, clock):
    when = clock.now() + timedelta(hours=3)
    response = await submit(client, scheduled_at=when.isoformat())

    assert response.status_code == 201
    assert response.json()["status"] == "scheduled"


async def test_e2e_03_past_schedule_starts_pending(client, clock):
    when = clock.now() - timedelta(hours=3)
    response = await submit(client, scheduled_at=when.isoformat())

    assert response.status_code == 201
    assert response.json()["status"] == "pending"


async def test_e2e_03b_explicit_null_schedule_is_pending(client):
    response = await client.post("/jobs", json=body(scheduled_at=None))

    assert response.status_code == 201
    assert response.json()["status"] == "pending"
    assert response.json()["scheduled_at"] is None


async def test_e2e_04_unknown_job_type(client):
    response = await submit(client, job_type="telepathy")

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "unknown_job_type"


@pytest.mark.parametrize("job_type", list(JobType))
async def test_e2e_05_missing_required_payload_field_names_it(client, job_type):
    field = REQUIRED_FIELDS[job_type]
    payload = {k: v for k, v in VALID_PAYLOADS[job_type].items() if k != field}

    response = await submit(client, job_type=job_type.value, payload=payload)

    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == "payload_invalid"
    assert any(detail["field"] == f"payload.{field}" for detail in error["details"])


async def test_e2e_06_extra_payload_field_is_rejected(client):
    response = await submit(client, payload=EMAIL_PAYLOAD | {"surprise": 1})

    assert response.status_code == 422
    assert "surprise" in str(response.json()["error"]["details"])


async def test_e2e_06b_extra_envelope_field_is_rejected(client):
    response = await client.post("/jobs", json=body(nonsense="x"))
    assert response.status_code == 422


async def test_e2e_07_oversized_body_is_rejected_before_parsing(client):
    huge = await client.post("/jobs", json=body(payload=EMAIL_PAYLOAD | {"body": "x" * 70_000}))

    assert huge.status_code == 413
    assert huge.json()["error"]["code"] == "payload_too_large"

    # Nothing was stored.
    listing = await client.get("/jobs")
    assert listing.json()["items"] == []


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/hook",
        "http://169.254.169.254/latest/meta-data/",
        "http://localhost/hook",
        "http://10.1.2.3/hook",
    ],
)
async def test_e2e_08_ssrf_targets_are_rejected(client, url):
    response = await submit(client, job_type="webhook", payload={"url": url})
    assert response.status_code == 422


@pytest.mark.parametrize("priority", [-1, 10, 99])
async def test_e2e_09_priority_outside_the_range(client, priority):
    response = await submit(client, priority=priority)
    assert response.status_code == 422


async def test_e2e_10_naive_scheduled_at_is_rejected(client):
    response = await submit(client, scheduled_at="2026-09-01T10:00:00")

    assert response.status_code == 422
    assert "timezone" in str(response.json()["error"]["details"])


async def test_e2e_10b_schedule_beyond_the_horizon_is_rejected(client, clock):
    far = clock.now() + timedelta(days=400)
    response = await submit(client, scheduled_at=far.isoformat())
    assert response.status_code == 422


@pytest.mark.parametrize("key", ["has space", "bad/slash", "x" * 256, ""])
async def test_e2e_10c_malformed_idempotency_keys_are_rejected(client, key):
    response = await submit(client, idempotency_key=key)
    assert response.status_code == 422


# --------------------------------------------------------------------------
# Idempotency
# --------------------------------------------------------------------------


async def test_e2e_11_repeated_key_returns_the_same_job(client):
    first = await submit(client, idempotency_key="order-42")
    second = await submit(client, idempotency_key="order-42")

    assert first.status_code == 201
    assert second.status_code == 200
    assert first.json()["id"] == second.json()["id"]

    listing = await client.get("/jobs")
    assert len(listing.json()["items"]) == 1


async def test_e2e_13_repeated_key_with_a_different_payload_returns_the_original(client):
    first = await submit(client, idempotency_key="order-43")
    second = await submit(
        client,
        idempotency_key="order-43",
        payload=EMAIL_PAYLOAD | {"subject": "Different"},
    )

    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]
    assert second.json()["payload"]["subject"] == "Hi"


async def test_e2e_12_concurrent_submissions_with_one_key_create_one_job(
    committing_sessions, test_db_url
):
    """Real connections and real commits over HTTP.

    The shared rolled-back session cannot be used here: it would serialise the
    concurrency this test exists to exercise.
    """
    app = create_app(Settings(database_url=test_db_url))
    app.state.session_factory = committing_sessions

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        responses = await asyncio.gather(
            *(client.post("/jobs", json=body(idempotency_key="race-http")) for _ in range(10))
        )

        assert {r.json()["id"] for r in responses} == {responses[0].json()["id"]}
        assert sum(1 for r in responses if r.status_code == 201) == 1
        assert sum(1 for r in responses if r.status_code == 200) == 9

        listing = await client.get("/jobs?limit=100")
        matching = [
            item for item in listing.json()["items"] if item["idempotency_key"] == "race-http"
        ]
        assert len(matching) == 1


# --------------------------------------------------------------------------
# Retrieval
# --------------------------------------------------------------------------


async def test_e2e_14_get_returns_every_documented_field(client):
    created = (await submit(client)).json()

    fetched = await client.get(f"/jobs/{created['id']}")

    assert fetched.status_code == 200
    assert set(fetched.json()) == set(JobResponse.model_fields)


async def test_e2e_15_get_unknown_id(client):
    response = await client.get(f"/jobs/{uuid4()}")

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "job_not_found"


async def test_e2e_16_get_malformed_uuid(client):
    response = await client.get("/jobs/not-a-uuid")
    assert response.status_code == 422


async def test_e2e_17_list_filtered_by_status(client):
    first = (await submit(client)).json()
    await submit(client, idempotency_key="k-b")
    await client.post(f"/jobs/{first['id']}/cancel")

    response = await client.get("/jobs", params={"status": "cancelled"})

    items = response.json()["items"]
    assert [item["id"] for item in items] == [first["id"]]


async def test_e2e_18_list_filtered_by_type(client):
    await submit(client)
    await submit(client, job_type="batch", payload=VALID_PAYLOADS[JobType.BATCH])

    response = await client.get("/jobs", params={"job_type": "batch"})

    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["job_type"] == "batch"


async def test_e2e_18b_unknown_filter_values_are_rejected(client):
    assert (await client.get("/jobs", params={"status": "sideways"})).status_code == 422
    assert (await client.get("/jobs", params={"job_type": "telepathy"})).status_code == 422


async def test_e2e_19_pagination_reports_more_without_overlap(client):
    for index in range(5):
        await submit(client, idempotency_key=f"page-{index}")

    first = (await client.get("/jobs", params={"limit": 2, "offset": 0})).json()
    second = (await client.get("/jobs", params={"limit": 2, "offset": 2})).json()
    last = (await client.get("/jobs", params={"limit": 2, "offset": 4})).json()

    assert (first["has_more"], second["has_more"], last["has_more"]) == (True, True, False)
    ids = [item["id"] for page in (first, second, last) for item in page["items"]]
    assert len(ids) == len(set(ids)) == 5


@pytest.mark.parametrize("limit", [0, 101, -1])
async def test_e2e_19b_out_of_range_limit_is_rejected(client, limit):
    response = await client.get("/jobs", params={"limit": limit})
    assert response.status_code == 422


# --------------------------------------------------------------------------
# Cancellation
# --------------------------------------------------------------------------


async def test_e2e_20_cancel_a_pending_job(client):
    created = (await submit(client)).json()

    response = await client.post(f"/jobs/{created['id']}/cancel")

    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"


async def test_e2e_21_cancel_a_scheduled_job(client, clock):
    when = clock.now() + timedelta(hours=1)
    created = (await submit(client, scheduled_at=when.isoformat())).json()

    response = await client.post(f"/jobs/{created['id']}/cancel")

    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"


async def test_e2e_22_cancelling_twice_conflicts_and_changes_nothing(client):
    created = (await submit(client)).json()
    await client.post(f"/jobs/{created['id']}/cancel")

    second = await client.post(f"/jobs/{created['id']}/cancel")

    assert second.status_code == 409
    assert second.json()["error"]["code"] == "job_not_cancellable"

    still = await client.get(f"/jobs/{created['id']}")
    assert still.json()["status"] == "cancelled"


async def test_e2e_23_cancel_unknown_id(client):
    response = await client.post(f"/jobs/{uuid4()}/cancel")

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "job_not_found"


# --------------------------------------------------------------------------
# Manual retry
# --------------------------------------------------------------------------


async def test_e2e_29_retrying_a_failed_job_requeues_it(client, db_session):
    job = await add_job(
        db_session,
        status=JobStatus.FAILED,
        attempts=3,
        error={"type": "JobExecutionError", "message": "downstream was down"},
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
    )

    response = await client.post(f"/jobs/{job.id}/retry")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "pending"
    assert body["attempts"] == 0
    assert body["error"] is None


async def test_e2e_30_retrying_a_job_that_did_not_fail_conflicts(client):
    created = (await submit(client)).json()

    response = await client.post(f"/jobs/{created['id']}/retry")

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "job_not_retryable"


async def test_e2e_31_retrying_a_dead_lettered_job_conflicts(client, db_session):
    """The queue refuses to re-arm work it knows cannot run."""
    job = await add_job(
        db_session,
        status=JobStatus.FAILED,
        attempts=1,
        dead_letter_reason=DeadLetterReason.UNPROCESSABLE_PAYLOAD,
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
    )

    response = await client.post(f"/jobs/{job.id}/retry")

    assert response.status_code == 409
    assert "dead-lettered" in response.json()["error"]["message"]


async def test_e2e_32_retrying_an_unknown_id(client):
    response = await client.post(f"/jobs/{uuid4()}/retry")

    assert response.status_code == 404


async def test_e2e_33_the_dead_letter_queue_is_listable(client, db_session):
    poison = await add_job(
        db_session,
        status=JobStatus.FAILED,
        attempts=1,
        dead_letter_reason=DeadLetterReason.WORKER_CRASH_LOOP,
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
    )
    await add_job(db_session, status=JobStatus.FAILED, attempts=3, started_at=datetime.now(UTC))

    listed = (await client.get("/jobs", params={"dead_lettered": True})).json()["items"]

    assert [item["id"] for item in listed] == [str(poison.id)]
    assert listed[0]["dead_letter_reason"] == "worker_crash_loop"


# --------------------------------------------------------------------------
# Job history — specs/10-job-history.md
# --------------------------------------------------------------------------


async def test_e2e_34_a_jobs_history_is_readable_oldest_first(client, db_session, repository):
    job_id = (await submit(client)).json()["id"]
    # Two rows written in one transaction share a created_at from now(); only
    # the sequence separates them, which is what the id tie-break is for.
    await repository.add_log(UUID(job_id), "info", "Claimed by w-1", {"priority": 5})
    await repository.add_log(
        UUID(job_id), "warning", "Attempt 1 failed; retrying", {"status": "pending"}
    )
    await db_session.flush()

    response = await client.get(f"/jobs/{job_id}/logs")

    assert response.status_code == 200
    entries = response.json()["items"]
    assert [entry["message"] for entry in entries] == [
        "Job created with status pending",
        "Claimed by w-1",
        "Attempt 1 failed; retrying",
    ]
    assert [entry["level"] for entry in entries] == ["info", "info", "warning"]
    assert entries[1]["meta"]["priority"] == 5


async def test_e2e_35_history_of_an_unknown_job_is_a_404(client):
    response = await client.get(f"/jobs/{uuid4()}/logs")

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "job_not_found"


async def test_e2e_36_history_pages_without_overlapping(client, db_session, repository):
    job_id = (await submit(client)).json()["id"]
    for index in range(3):
        await repository.add_log(UUID(job_id), "info", f"event {index}", {})
    await db_session.flush()

    first = (await client.get(f"/jobs/{job_id}/logs", params={"limit": 2})).json()
    second = (await client.get(f"/jobs/{job_id}/logs", params={"limit": 2, "offset": 2})).json()

    assert first["has_more"] is True
    assert second["has_more"] is False
    assert len(first["items"]) == 2 and len(second["items"]) == 2
    messages = [entry["message"] for entry in first["items"] + second["items"]]
    assert messages == ["Job created with status pending", "event 0", "event 1", "event 2"]


@pytest.mark.parametrize("limit", [0, 101])
async def test_e2e_36b_out_of_range_history_limits_are_rejected(client, limit):
    job_id = (await submit(client)).json()["id"]

    assert (await client.get(f"/jobs/{job_id}/logs", params={"limit": limit})).status_code == 422


async def test_e2e_37_history_never_carries_payload_contents(client, db_session, execution):
    """Every row a real run writes, not just the submission row.

    The endpoint returns `meta` as stored, so the property holds only as long as
    no writer puts payload contents there. Reading a freshly submitted job would
    inspect `payload_fingerprint` alone and pass while the claim, the handler's
    own note and the failure — the rows carrying worker-supplied values — went
    unexamined, which is the same shape of vacuous test this suite has been
    bitten by twice (AI_USAGE.md). So the job is driven through them first.

    The failure is raised with the payload in its message on purpose: `jobs.error`
    keeps that text and `job_logs` records the exception's class name instead,
    which is the distinction spec 10 section 3 rests on.
    """
    secret = {"to": "victim@example.com", "subject": "s3cret-subject", "body": "s3cret-body"}
    job_id = (await submit(client, payload=secret)).json()["id"]
    await db_session.flush()

    job = await execution.claim("w-0", 60)
    assert job is not None and job.id == UUID(job_id)
    await execution.note(job.id, "info", "Delivering", provider="smtp")
    await execution.fail(
        job,
        Ownership.of(job),
        JobExecutionError(f"SMTP rejected {secret['to']}: s3cret-body"),
    )
    await db_session.flush()
    # The failure write expired the row this session is holding. A real request
    # would load it fresh; here the API shares the session, so it is dropped from
    # the identity map rather than refreshed lazily under Pydantic.
    db_session.expunge_all()

    history = await client.get(f"/jobs/{job_id}/logs")

    # Asserted first, so the checks below cannot pass by inspecting rows that
    # were never written.
    assert [entry["message"] for entry in history.json()["items"]] == [
        "Job created with status pending",
        "Claimed by w-0",
        "Delivering",
        "Attempt 1 failed; retrying",
    ]
    for value in ("s3cret-subject", "s3cret-body", "victim@example.com"):
        assert value not in history.text
    # The same string is reachable one endpoint over, which is what makes its
    # absence above a property of `job_logs` rather than of the test's inputs.
    assert "s3cret-body" in (await client.get(f"/jobs/{job_id}")).text


async def test_reading_history_writes_nothing(client, db_session):
    job_id = (await submit(client)).json()["id"]
    before = (await client.get(f"/jobs/{job_id}")).json()

    await client.get(f"/jobs/{job_id}/logs")

    assert (await client.get(f"/jobs/{job_id}")).json() == before


# --------------------------------------------------------------------------
# Error handling and extensibility
# --------------------------------------------------------------------------


async def test_e2e_26_internal_errors_leak_nothing(db_session, clock, test_db_url):
    app = create_app(Settings(database_url=test_db_url))
    app.dependency_overrides[get_session] = lambda: db_session

    def explode():
        raise RuntimeError("SELECT secret FROM internal_table -- boom")

    app.dependency_overrides[get_job_service] = explode

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(f"/jobs/{uuid4()}")

    assert response.status_code == 500
    text = response.text
    assert response.json()["error"]["code"] == "internal_error"
    assert "Traceback" not in text
    assert "SELECT secret" not in text
    assert "internal_table" not in text


async def test_e2e_27_a_new_job_type_needs_no_changes_outside_app_jobs(client):
    """Registering a type and submitting it exercises the whole path without
    touching the API, service or persistence layers."""
    from app.jobs.base import BaseJob, JobPayload, JobResult
    from app.jobs.registry import JOB_REGISTRY, register

    class ProbePayload(JobPayload):
        note: str

    class ProbeResult(JobResult):
        note: str

    original = JOB_REGISTRY.pop(JobType.REPORT)
    try:

        @register
        class ProbeJob(BaseJob):
            job_type = JobType.REPORT
            Payload = ProbePayload
            Result = ProbeResult
            default_priority = 2

            async def run(self) -> ProbeResult:
                return ProbeResult(note=self.payload.note)

        response = await submit(client, job_type="report", payload={"note": "hello"})

        assert response.status_code == 201
        assert response.json()["payload"] == {"note": "hello"}
        # Defaults come from the job class, not from the API layer.
        assert response.json()["priority"] == 2
    finally:
        JOB_REGISTRY[JobType.REPORT] = original
