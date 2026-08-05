"""Test doubles.

These live in the test tree rather than in ``app`` so that production code
carries nothing that exists only for testing.
"""

import asyncio
import random
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4


class FrozenClock:
    """A clock that only moves when a test tells it to."""

    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> None:
        self._now += delta


class RecordingSleeper:
    """Returns immediately, but remembers what it was asked to wait for.

    This is what lets a test assert that an email job sleeps between one and
    three seconds without the suite spending three seconds proving it.
    """

    def __init__(self) -> None:
        self.calls: list[float] = []

    async def sleep(self, seconds: float) -> None:
        self.calls.append(seconds)


class BlockingSleeper:
    """Never returns. Used to hold a handler open until something cancels it."""

    def __init__(self) -> None:
        self.entered = asyncio.Event()

    async def sleep(self, seconds: float) -> None:
        self.entered.set()
        await asyncio.Event().wait()


class StubRandom(random.Random):
    """Deterministic randomness: ``random()`` always returns ``value``.

    ``uniform(a, b)`` is defined as ``a + (b - a) * random()``, so a stubbed
    ``random()`` also pins every sleep duration.
    """

    def __init__(self, value: float = 0.5) -> None:
        super().__init__(0)
        self._value = value

    def random(self) -> float:
        return self._value


class FakeExecutionService:
    """Stands in for the worker's service layer, with no database at all.

    The slot only ever talks to the service, so faking that seam — rather than
    the repository beneath it — keeps the double small and keeps the slot tests
    about orchestration, which is the only thing the slot is responsible for.
    """

    def __init__(self, jobs: list[Any] | None = None) -> None:
        self.queue: list[Any] = list(jobs or [])
        self.claims: list[tuple[str, UUID | None]] = []
        self.completed: list[tuple[UUID, Any]] = []
        self.failures: list[tuple[UUID, BaseException, bool]] = []
        self.progress: list[tuple[UUID, int]] = []
        self.notes: list[tuple[UUID, str, str]] = []
        self.released: list[UUID] = []
        self.lease_extensions = 0
        #: Flipped to False to simulate the reaper taking the job away.
        self.lease_alive = True

    async def claim(
        self, worker_id: str, lease_seconds: int, hint: UUID | None = None
    ) -> Any | None:
        self.claims.append((worker_id, hint))
        return self.queue.pop(0) if self.queue else None

    async def complete(self, job: Any, own: Any, result: Any) -> bool:
        self.completed.append((own.job_id, result))
        return True

    async def fail(self, job: Any, own: Any, exc: BaseException, *, retryable: bool = True) -> bool:
        self.failures.append((own.job_id, exc, retryable))
        return True

    async def extend_lease(self, own: Any, lease_seconds: int) -> bool:
        self.lease_extensions += 1
        return self.lease_alive

    async def report_progress(self, own: Any, percent: int) -> bool:
        self.progress.append((own.job_id, percent))
        return self.lease_alive

    async def release(self, own: Any) -> bool:
        self.released.append(own.job_id)
        return True

    async def note(self, job_id: UUID, level: str, message: str, **fields: Any) -> None:
        self.notes.append((job_id, level, message))


def service_scope(
    service: Any,
) -> Callable[[], AbstractAsyncContextManager[Any]]:
    """Wrap a service so it can be passed where a unit-of-work scope is wanted."""

    @asynccontextmanager
    async def scope() -> AsyncIterator[Any]:
        yield service

    return scope


class FakeJobContext:
    """Records everything a handler does through its context."""

    def __init__(self, job_id: UUID | None = None, attempt: int = 1) -> None:
        self.job_id = job_id or uuid4()
        self.attempt = attempt
        self.progress: list[int] = []
        self.logs: list[tuple[str, str, dict[str, Any]]] = []
        self.heartbeats = 0

    async def report_progress(self, pct: int) -> None:
        self.progress.append(pct)

    async def log(self, level: str, message: str, **fields: Any) -> None:
        self.logs.append((level, message, fields))

    async def heartbeat(self) -> None:
        self.heartbeats += 1
