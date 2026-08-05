"""E2E-24, E2E-25, E2E-28 — health, correlation ids and the log stream."""

import io
import json

import pytest
import structlog

from app.core.logging import configure_logging

EMAIL_PAYLOAD = {"to": "user@example.com", "subject": "Hi", "body": "Hello"}
BODY = {"job_type": "email", "payload": EMAIL_PAYLOAD}


@pytest.fixture
def log_stream():
    """Redirect the real logging pipeline into a buffer.

    The production processors are reused rather than reimplemented, so this
    asserts against the output an operator would actually see.
    """
    configure_logging("INFO")
    config = structlog.get_config()
    buffer = io.StringIO()
    structlog.configure(
        processors=config["processors"],
        wrapper_class=config["wrapper_class"],
        logger_factory=structlog.PrintLoggerFactory(file=buffer),
        cache_logger_on_first_use=False,
    )
    yield buffer
    configure_logging("INFO")


async def test_e2e_24_health_reports_queue_depth(client):
    before = (await client.get("/health")).json()

    await client.post("/jobs", json=BODY)
    await client.post("/jobs", json={**BODY, "idempotency_key": "h-2"})

    after = (await client.get("/health")).json()

    assert after["status"] == "ok"
    assert after["database"] == "ok"
    assert after["queue"]["pending"] == before["queue"]["pending"] + 2
    assert {
        "scheduled",
        "pending",
        "processing",
        "completed",
        "failed",
        "cancelled",
        "oldest_pending_seconds",
        "ready_hints",
        "dead_lettered",
    } == set(after["queue"])
    assert after["uptime_seconds"] >= 0
    assert after["version"]


async def test_e2e_24c_queue_age_separates_a_busy_queue_from_a_stuck_one(client):
    """Depth alone cannot tell load from failure; depth with age can."""
    idle = (await client.get("/health")).json()["queue"]
    assert idle["oldest_pending_seconds"] is None

    await client.post("/jobs", json=BODY)

    busy = (await client.get("/health")).json()["queue"]
    assert busy["oldest_pending_seconds"] is not None
    assert busy["oldest_pending_seconds"] >= 0


async def test_e2e_24b_cancelled_jobs_move_between_counters(client):
    created = (await client.post("/jobs", json=BODY)).json()
    before = (await client.get("/health")).json()["queue"]

    await client.post(f"/jobs/{created['id']}/cancel")

    after = (await client.get("/health")).json()["queue"]
    assert after["pending"] == before["pending"] - 1
    assert after["cancelled"] == before["cancelled"] + 1


async def test_health_reports_a_failing_database_instead_of_raising(clock, test_db_url):
    """A health endpoint that 500s tells a load balancer less than one that says
    which dependency is down."""
    from httpx import ASGITransport, AsyncClient

    from app.api.app import create_app
    from app.api.deps import get_clock, get_session
    from app.core.config import Settings

    class BrokenSession:
        async def execute(self, *args, **kwargs):
            raise RuntimeError("connection refused")

    app = create_app(Settings(database_url=test_db_url))
    app.dependency_overrides[get_session] = lambda: BrokenSession()
    app.dependency_overrides[get_clock] = lambda: clock

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as broken_client:
        response = await broken_client.get("/health")

    assert response.status_code == 503
    payload = response.json()
    assert payload["status"] == "degraded"
    assert payload["database"] == "error"
    assert "connection refused" not in response.text


async def test_e2e_25e_redis_being_down_is_reported_but_is_not_a_503(client):
    """The database is a hard dependency; Redis is not. Jobs keep flowing
    through the PostgreSQL claim, so pulling the service out of a load balancer
    would turn a latency problem into an outage."""
    response = await client.get("/health")

    payload = response.json()
    assert response.status_code == 200
    # The test app runs on NullDispatch, which reports the same "unknown" a
    # dead Redis would.
    assert payload["redis"] == "error"
    assert payload["workers"] is None  # not count: 0
    assert payload["queue"]["ready_hints"] is None


async def test_e2e_25_request_id_is_generated_when_absent(client):
    response = await client.get("/health")

    request_id = response.headers.get("X-Request-ID")
    assert request_id
    assert len(request_id) <= 64


async def test_e2e_25b_inbound_request_id_is_echoed(client):
    response = await client.get("/health", headers={"X-Request-ID": "trace-abc-123"})
    assert response.headers["X-Request-ID"] == "trace-abc-123"


@pytest.mark.parametrize("hostile", ["has space", "x" * 200, "line\nbreak", "semi;colon"])
async def test_e2e_25c_malformed_inbound_request_id_is_replaced(client, hostile):
    """An inbound id is echoed into every log line, so an unbounded value would
    be a log injection vector."""
    response = await client.get("/health", headers={"X-Request-ID": hostile})

    assert response.headers["X-Request-ID"] != hostile
    assert "\n" not in response.headers["X-Request-ID"]


async def test_e2e_25d_error_responses_carry_the_request_id(client):
    response = await client.get(
        "/jobs/00000000-0000-0000-0000-000000000000",
        headers={"X-Request-ID": "trace-err"},
    )

    assert response.status_code == 404
    assert response.headers["X-Request-ID"] == "trace-err"
    assert response.json()["error"]["request_id"] == "trace-err"


async def test_e2e_28_every_log_line_is_json_and_carries_the_request_id(client, log_stream):
    response = await client.post("/jobs", json=BODY, headers={"X-Request-ID": "trace-log-1"})
    assert response.status_code == 201

    lines = [line for line in log_stream.getvalue().splitlines() if line.strip()]
    assert lines, "the request produced no log output"

    for line in lines:
        entry = json.loads(line)  # fails loudly if a line is not valid JSON
        assert entry["request_id"] == "trace-log-1"
        assert entry["service"]
        assert entry["ts"]
        assert entry["level"]

    events = {json.loads(line)["event"] for line in lines}
    assert "job.created" in events
    assert "http.request" in events


async def test_e2e_28b_payload_contents_never_reach_the_log_stream(client, log_stream):
    await client.post(
        "/jobs",
        json={
            "job_type": "email",
            "payload": {
                "to": "very.private@example.com",
                "subject": "Confidential",
                "body": "secret contents",
            },
        },
    )

    output = log_stream.getvalue()
    assert "very.private@example.com" not in output
    assert "secret contents" not in output
    assert "payload_sha256" in output
