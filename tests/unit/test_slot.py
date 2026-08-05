"""W1-07 .. W1-13 — the slot loop, with no database and no Redis.

The slot's only job is orchestration: claim, run, hand the outcome to the
service. Whether an outcome means a retry or a permanent failure is the
service's rule, and is tested there.
"""

import asyncio

import pytest

from app.core.enums import JobType
from app.dispatch.base import NullDispatch
from app.jobs.base import JobExecutionError, JobTimeoutError
from app.worker.identity import worker_id
from app.worker.slot import Slot
from tests.doubles import (
    BlockingSleeper,
    FakeExecutionService,
    RecordingSleeper,
    StubRandom,
    service_scope,
)
from tests.factories import BATCH_PAYLOAD, build_claimed_job

LEASE = 60


def build_slot(service, **overrides):
    settings = {
        "worker_id": "w-test-0",
        "scope": service_scope(service),
        "dispatch": NullDispatch(),
        "lease_seconds": LEASE,
        "heartbeat_seconds": 3600,  # never fires during a test unless overridden
        "poll_interval_seconds": 0.01,
        "rng": StubRandom(0.99),
        "sleeper": RecordingSleeper(),
    }
    settings.update(overrides)
    return Slot(**settings)


# ---------------------------------------------------------------------------
# W1-07 — identity
# ---------------------------------------------------------------------------


def test_w1_07_worker_ids_are_unique_per_slot():
    ids = {worker_id(slot) for slot in range(4)}

    assert len(ids) == 4
    assert all(identifier.endswith(f"-{slot}") for slot, identifier in enumerate(sorted(ids)))


def test_w1_07b_worker_id_names_the_host_and_process():
    """Per-process identity would be a correctness bug, not a cosmetic one; the
    host and pid are what make a log line traceable to a container."""
    import os

    assert str(os.getpid()) in worker_id(0)


# ---------------------------------------------------------------------------
# W1-08 / W1-13 — the happy path and the empty queue
# ---------------------------------------------------------------------------


async def test_w1_13_an_empty_queue_does_no_work():
    service = FakeExecutionService(jobs=[])
    slot = build_slot(service)

    assert await slot.run_once() is False
    assert service.completed == []
    assert service.failures == []


async def test_w1_08_a_claimed_job_runs_and_is_completed():
    job = build_claimed_job()
    service = FakeExecutionService(jobs=[job])
    slot = build_slot(service)

    assert await slot.run_once() is True

    assert [job_id for job_id, _ in service.completed] == [job.id]
    result = service.completed[0][1]
    assert result.message_id.startswith("msg-")
    assert service.failures == []


async def test_w1_08b_the_result_matches_the_handlers_declared_schema():
    job = build_claimed_job(job_type=JobType.BATCH, payload=BATCH_PAYLOAD)
    service = FakeExecutionService(jobs=[job])

    await build_slot(service).run_once()

    _, result = service.completed[0]
    assert result.total == len(BATCH_PAYLOAD["items"])


async def test_w1_08c_the_hint_is_passed_through_to_the_claim():
    job = build_claimed_job()
    service = FakeExecutionService(jobs=[job])
    slot = build_slot(service)

    await slot.run_once(hint=job.id)

    assert service.claims == [("w-test-0", job.id)]


async def test_w1_08d_progress_reaches_the_service():
    job = build_claimed_job(job_type=JobType.BATCH, payload=BATCH_PAYLOAD)
    service = FakeExecutionService(jobs=[job])

    await build_slot(service).run_once()

    assert [percent for _, percent in service.progress] == [50, 100]


async def test_the_slot_holds_no_ownership_once_a_job_is_done():
    service = FakeExecutionService(jobs=[build_claimed_job()])
    slot = build_slot(service)

    await slot.run_once()

    assert slot.current is None


# ---------------------------------------------------------------------------
# W1-09 / W1-10 — failures reach the service, which decides what they mean
# ---------------------------------------------------------------------------


async def test_w1_09_a_handler_exception_is_reported_as_retryable():
    job = build_claimed_job(job_type=JobType.WEBHOOK, payload={"url": "https://example.com/x"})
    service = FakeExecutionService(jobs=[job])
    # StubRandom(0.0) puts the webhook's simulated failure branch in control.
    slot = build_slot(service, rng=StubRandom(0.0))

    assert await slot.run_once() is True

    assert service.completed == []
    [(failed_id, exc, retryable)] = service.failures
    assert failed_id == job.id
    assert isinstance(exc, JobExecutionError)
    assert retryable is True


async def test_w1_10_the_retry_decision_is_not_the_slots_to_make():
    """Whether this was the last attempt is a rule about jobs, so the slot
    reports the failure identically and the service decides."""
    exhausted = build_claimed_job(
        job_type=JobType.WEBHOOK,
        payload={"url": "https://example.com/x"},
        attempts=3,
        max_attempts=3,
    )
    service = FakeExecutionService(jobs=[exhausted])

    await build_slot(service, rng=StubRandom(0.0)).run_once()

    [(_, _, retryable)] = service.failures
    assert retryable is True


# ---------------------------------------------------------------------------
# W1-11 — a payload that no longer parses
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("description", "overrides"),
    [
        ("payload no longer matches the schema", {"payload": {"unexpected": "shape"}}),
        ("job type no longer registered", {"job_type": "email", "payload": {}}),
    ],
)
async def test_w1_11_an_unusable_payload_fails_on_the_first_attempt(description, overrides):
    job = build_claimed_job(**overrides)
    service = FakeExecutionService(jobs=[job])

    assert await build_slot(service).run_once() is True

    [(failed_id, _, retryable)] = service.failures
    assert failed_id == job.id
    assert retryable is False  # not worth two more workers to learn the same thing
    assert service.completed == []


async def test_w1_11b_an_unusable_payload_never_starts_a_heartbeat():
    job = build_claimed_job(payload={"unexpected": "shape"})
    service = FakeExecutionService(jobs=[job])

    await build_slot(service, heartbeat_seconds=0).run_once()

    assert service.lease_extensions == 0


# ---------------------------------------------------------------------------
# W1-12 — losing the lease mid-run
# ---------------------------------------------------------------------------


async def test_w1_12_losing_the_lease_abandons_the_work():
    """The reaper handed this job to someone else. Carrying on would mean two
    workers running it in full; stopping bounds that to one heartbeat."""
    job = build_claimed_job()
    service = FakeExecutionService(jobs=[job])
    service.lease_alive = False
    slot = build_slot(service, heartbeat_seconds=0, sleeper=BlockingSleeper())

    assert await slot.run_once() is True

    assert service.completed == []
    assert service.failures == []  # the outcome belongs to whoever holds it now
    assert slot.current is None


async def test_a_handler_that_overruns_its_budget_is_failed(monkeypatch):
    """`timeout_seconds` existed on every job type and nothing read it. Now a
    wedged handler occupies a slot for a bounded time instead of until its lease
    expires."""
    from app.jobs.email_job import EmailJob

    monkeypatch.setattr(EmailJob, "timeout_seconds", 0.01)
    job = build_claimed_job()
    service = FakeExecutionService(jobs=[job])
    slot = build_slot(service, sleeper=BlockingSleeper())

    assert await slot.run_once() is True

    assert service.completed == []
    [(failed_id, exc, retryable)] = service.failures
    assert failed_id == job.id
    assert isinstance(exc, JobTimeoutError)
    # One slow run proves nothing about the next one; the service decides
    # whether exhausting attempts this way is poison.
    assert retryable is True


async def test_a_timeout_is_not_mistaken_for_losing_the_lease(monkeypatch):
    """Two different things cancel the same task. Confusing them would mean a
    timed-out job records no outcome at all and waits for the reaper."""
    from app.jobs.email_job import EmailJob

    monkeypatch.setattr(EmailJob, "timeout_seconds", 0.01)
    service = FakeExecutionService(jobs=[build_claimed_job()])
    slot = build_slot(service, sleeper=BlockingSleeper())

    await slot.run_once()

    assert len(service.failures) == 1
    assert slot.current is None


async def test_w1_12b_a_shutdown_cancellation_keeps_the_ownership_for_release():
    """Cancelled while healthy means the worker is stopping. The ownership must
    survive so the worker can release the lease instead of leaving it to lapse."""
    job = build_claimed_job()
    service = FakeExecutionService(jobs=[job])
    sleeper = BlockingSleeper()
    slot = build_slot(service, sleeper=sleeper)

    task = asyncio.create_task(slot.run_once())
    await asyncio.wait_for(sleeper.entered.wait(), timeout=2)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert slot.current is not None
    assert slot.current.job_id == job.id


# ---------------------------------------------------------------------------
# run_forever
# ---------------------------------------------------------------------------


async def test_run_forever_drains_without_waiting_between_jobs():
    jobs = [build_claimed_job() for _ in range(3)]
    service = FakeExecutionService(jobs=jobs)
    slot = build_slot(service)
    stop = asyncio.Event()

    async def stop_once_drained() -> None:
        while len(service.completed) < len(jobs):
            await asyncio.sleep(0)
        stop.set()

    await asyncio.gather(slot.run_forever(stop), stop_once_drained())

    assert len(service.completed) == 3


async def test_run_forever_stops_when_asked():
    service = FakeExecutionService(jobs=[])
    slot = build_slot(service)
    stop = asyncio.Event()
    stop.set()

    await asyncio.wait_for(slot.run_forever(stop), timeout=2)

    assert service.claims == []


async def test_w1_15_a_failed_cycle_does_not_end_the_slot():
    """Nothing awaits a slot task until shutdown, so an escaping exception would
    retire the slot in silence: the process stays up, keeps announcing itself as
    live, and claims nothing again. Transient database errors are ordinary."""
    job = build_claimed_job()
    service = FakeExecutionService(jobs=[job])
    original_claim = service.claim
    failed_once = False

    async def claim(*args, **kwargs):
        nonlocal failed_once
        if not failed_once:
            failed_once = True
            raise OSError("connection reset by peer")
        return await original_claim(*args, **kwargs)

    service.claim = claim  # type: ignore[method-assign]
    slot = build_slot(service)
    stop = asyncio.Event()

    async def stop_once_recovered() -> None:
        while not service.completed:
            await asyncio.sleep(0)
        stop.set()

    await asyncio.wait_for(asyncio.gather(slot.run_forever(stop), stop_once_recovered()), timeout=5)

    assert [job_id for job_id, _ in service.completed] == [job.id]


async def test_w1_15b_a_failing_cycle_waits_instead_of_spinning():
    """A dependency that is down stays down for longer than one iteration, and a
    loop that retried immediately would turn an outage into a hot loop against
    the very dependency that is struggling."""
    service = FakeExecutionService(jobs=[])
    attempts = 0

    async def always_fails(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        raise OSError("database is down")

    service.claim = always_fails  # type: ignore[method-assign]
    interval = 0.05
    slot = build_slot(service, poll_interval_seconds=interval)
    stop = asyncio.Event()

    task = asyncio.create_task(slot.run_forever(stop))
    await asyncio.sleep(interval * 4)
    stop.set()
    await asyncio.wait_for(task, timeout=2)

    # One attempt per interval, give or take scheduling — not thousands.
    assert 1 <= attempts <= 8
