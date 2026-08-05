"""L1-30 .. L1-36 — handler behaviour, with no infrastructure at all.

These are the tests that demonstrate job logic is verifiable independently of
the API and the database: a handler receives a JobContext, and here it is a fake.
"""

import pytest

from app.core.enums import JobType
from app.jobs import batch_job, email_job, report_job, webhook_job
from app.jobs.base import JobExecutionError
from app.jobs.registry import get_job_class
from tests.doubles import FakeJobContext, RecordingSleeper, StubRandom
from tests.unit.test_payload_validation import VALID_PAYLOADS

SLEEP_RANGES = {
    JobType.EMAIL: email_job.SLEEP_RANGE,
    JobType.WEBHOOK: webhook_job.SLEEP_RANGE,
    JobType.REPORT: report_job.SLEEP_RANGE,
}


def build(job_type: JobType, payload: dict | None = None, *, random_value: float = 0.99):
    cls = get_job_class(job_type)
    parsed = cls.parse_payload(payload or VALID_PAYLOADS[job_type])
    ctx = FakeJobContext()
    sleeper = RecordingSleeper()
    return cls(parsed, ctx, sleeper=sleeper, rng=StubRandom(random_value)), ctx, sleeper


@pytest.mark.parametrize("job_type", list(JobType))
async def test_l1_30_run_returns_the_declared_result_type(job_type):
    job, _, _ = build(job_type)
    result = await job.run()
    assert isinstance(result, get_job_class(job_type).Result)


@pytest.mark.parametrize("job_type", list(SLEEP_RANGES))
async def test_l1_31_sleep_falls_inside_the_specified_range(job_type):
    low, high = SLEEP_RANGES[job_type]
    job, _, sleeper = build(job_type)
    await job.run()
    assert len(sleeper.calls) == 1
    assert low <= sleeper.calls[0] <= high


async def test_l1_31b_batch_delays_once_per_item():
    job, _, sleeper = build(JobType.BATCH, {"items": ["a", "b", "c"], "operation": "index"})
    await job.run()
    assert sleeper.calls == [batch_job.ITEM_DELAY_SECONDS] * 3


async def test_l1_32_webhook_failure_raises_without_leaking_the_payload():
    url = "https://secret-tenant.example.com/private-hook"
    job, _, _ = build(JobType.WEBHOOK, {"url": url}, random_value=0.0)

    with pytest.raises(JobExecutionError) as excinfo:
        await job.run()

    message = str(excinfo.value)
    assert "secret-tenant" not in message
    assert url not in message


async def test_l1_33_webhook_success_returns_a_response():
    job, _, _ = build(JobType.WEBHOOK, random_value=0.99)
    result = await job.run()
    assert result.status_code == 200
    assert result.response_ms > 0


async def test_l1_34_batch_progress_is_monotonic_and_ends_at_100():
    job, ctx, _ = build(JobType.BATCH, {"items": [f"i{n}" for n in range(4)], "operation": "index"})
    await job.run()

    assert ctx.progress == [25, 50, 75, 100]
    assert ctx.progress == sorted(ctx.progress)
    assert ctx.heartbeats == len(ctx.progress)


async def test_l1_34b_batch_reports_each_percentage_at_most_once():
    """A 1000-item batch produces one update per percentage point reached, not
    1000 identical ones."""
    job, ctx, _ = build(JobType.BATCH, {"items": ["x"] * 1000, "operation": "index"})
    await job.run()

    assert len(ctx.progress) == len(set(ctx.progress))
    assert len(ctx.progress) <= 101  # 0 through 100 inclusive
    assert ctx.progress == sorted(ctx.progress)
    assert ctx.progress[-1] == 100


async def test_l1_35_batch_summary_accounts_for_every_item():
    job, _, _ = build(
        JobType.BATCH, {"items": ["a", "b", "c"], "operation": "index"}, random_value=0.99
    )
    result = await job.run()

    assert result.total == 3
    assert result.succeeded == 3
    assert result.failed == 0
    assert result.errors == []


async def test_l1_35b_batch_completes_even_when_items_fail():
    """Individual item failures are reported, not raised: a batch that processed
    most of its items has done useful work, and re-running it would redo them."""
    job, _, _ = build(
        JobType.BATCH, {"items": ["a", "b", "c"], "operation": "index"}, random_value=0.0
    )
    result = await job.run()

    assert (result.total, result.succeeded, result.failed) == (3, 0, 3)
    assert len(result.errors) == 3


async def test_l1_35c_reported_errors_are_capped():
    job, _, _ = build(JobType.BATCH, {"items": ["x"] * 200, "operation": "index"}, random_value=0.0)
    result = await job.run()

    assert result.failed == 200
    assert len(result.errors) == batch_job.MAX_REPORTED_ERRORS


async def test_l1_35d_batch_errors_do_not_contain_item_contents():
    job, _, _ = build(
        JobType.BATCH,
        {"items": ["secret-customer-id"], "operation": "index"},
        random_value=0.0,
    )
    result = await job.run()

    assert not any("secret-customer-id" in message for message in result.errors)


class _StrictContext:
    """A context that implements the protocol and nothing else.

    Any attribute a handler reaches for beyond JobContext raises here, so this
    test fails the moment a handler grows a dependency on a session or a
    repository.
    """

    job_id = "00000000-0000-0000-0000-000000000000"
    attempt = 1

    async def report_progress(self, pct: int) -> None: ...

    async def log(self, level: str, message: str, **fields: object) -> None: ...

    async def heartbeat(self) -> None: ...

    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"handler reached outside JobContext for {name!r}")


@pytest.mark.parametrize("job_type", list(JobType))
async def test_l1_36_handlers_touch_nothing_beyond_the_context(job_type):
    cls = get_job_class(job_type)
    payload = cls.parse_payload(VALID_PAYLOADS[job_type])
    job = cls(payload, _StrictContext(), sleeper=RecordingSleeper(), rng=StubRandom(0.99))
    assert await job.run() is not None
