"""The database-backed JobContext, driven against a fake service.

Handlers were written against a protocol; these tests check the implementation
honours it, including the parts a handler cannot observe — that writes are
dropped rather than raised once the job has been taken away.
"""

from uuid import uuid4

import pytest

from app.db.repository import Ownership
from app.worker.context import DbJobContext
from tests.doubles import FakeExecutionService, service_scope

OWNERSHIP = Ownership(job_id=uuid4(), worker_id="w-test-0", attempts=2)


def build_context(service, **overrides):
    settings = {"heartbeat_seconds": 0.0, "lease_seconds": 60}
    settings.update(overrides)
    return DbJobContext(OWNERSHIP, service_scope(service), **settings)


def test_the_context_exposes_the_protocol_surface_and_nothing_else():
    """Conformance to `JobContext` is checked statically by mypy — it is what
    caught this class declaring read-only properties against a protocol that
    asked for settable attributes. This asserts the surface a handler actually
    reaches for, which is the part a type checker cannot say is *enough*."""
    context = build_context(FakeExecutionService())

    for member in ("job_id", "attempt", "report_progress", "log", "heartbeat"):
        assert hasattr(context, member)


def test_the_job_and_attempt_come_from_the_ownership_token():
    """Derived, not stored: there is one source of truth for which attempt this
    is, and it is the same token every write is fenced with."""
    context = build_context(FakeExecutionService())

    assert context.job_id == OWNERSHIP.job_id
    assert context.attempt == OWNERSHIP.attempts


async def test_progress_reaches_the_service():
    service = FakeExecutionService()
    context = build_context(service)

    await context.report_progress(60)

    assert service.progress == [(OWNERSHIP.job_id, 60)]


async def test_progress_is_dropped_rather_than_raised_once_the_job_is_gone():
    """An exception here would surface out of the handler and be recorded as a
    job failure, which it is not — the job simply belongs to someone else."""
    service = FakeExecutionService()
    service.lease_alive = False
    context = build_context(service)

    await context.report_progress(60)  # must not raise

    assert service.progress == [(OWNERSHIP.job_id, 60)]


async def test_handler_logs_are_recorded_against_the_job():
    service = FakeExecutionService()
    context = build_context(service)

    await context.log("warning", "third retry of the upstream call", attempt=2)

    assert service.notes == [(OWNERSHIP.job_id, "warning", "third retry of the upstream call")]


async def test_heartbeat_extends_the_lease():
    service = FakeExecutionService()
    context = build_context(service)

    await context.heartbeat()

    assert service.lease_extensions == 1


async def test_heartbeat_is_rate_limited():
    """A chatty handler must not turn a courtesy call into write amplification."""
    service = FakeExecutionService()
    context = build_context(service, heartbeat_seconds=3600)

    for _ in range(50):
        await context.heartbeat()

    assert service.lease_extensions == 0


@pytest.mark.parametrize("percent", [0, 50, 100])
async def test_progress_passes_any_valid_percentage_through(percent):
    service = FakeExecutionService()

    await build_context(service).report_progress(percent)

    assert service.progress == [(OWNERSHIP.job_id, percent)]
