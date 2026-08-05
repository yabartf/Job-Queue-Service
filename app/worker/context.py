"""The database-backed `JobContext` handlers receive.

Handlers were written against a protocol exposing three methods and nothing
else (spec 02 section 4). This is the implementation that satisfies it — and no
handler changes to accommodate it, which was the point of the protocol.
"""

import time
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Any
from uuid import UUID

from app.db.repository import Ownership
from app.services.execution_service import ExecutionService

#: A callable that opens a short unit of work. Passing the scope rather than a
#: session factory keeps the context testable with a fake service.
ServiceScope = Callable[[], AbstractAsyncContextManager[ExecutionService]]


class DbJobContext:
    """Everything a handler is allowed to reach, and nothing more.

    Writes that find the job no longer ours are dropped silently rather than
    raised. The handler is about to be cancelled anyway, and an exception
    surfacing out of ``report_progress`` would be recorded as a job failure —
    which it is not.
    """

    def __init__(
        self,
        ownership: Ownership,
        scope: ServiceScope,
        *,
        heartbeat_seconds: float,
        lease_seconds: int,
    ) -> None:
        self._ownership = ownership
        self._scope = scope
        self._heartbeat_seconds = heartbeat_seconds
        self._lease_seconds = lease_seconds
        # Monotonic: rate limiting must not be affected by a clock adjustment.
        self._last_heartbeat = time.monotonic()

    @property
    def job_id(self) -> UUID:
        return self._ownership.job_id

    @property
    def attempt(self) -> int:
        return self._ownership.attempts

    async def report_progress(self, pct: int) -> None:
        async with self._scope() as service:
            await service.report_progress(self._ownership, pct)

    async def log(self, level: str, message: str, **fields: Any) -> None:
        async with self._scope() as service:
            await service.note(self._ownership.job_id, level, message, **fields)

    async def heartbeat(self) -> None:
        """Extend the lease, at most once per interval.

        Redundant with the worker's background heartbeat, and deliberately so: a
        handler that occupies the event loop stops the timer from running at all,
        and an explicit ``await`` here is both a lease extension and a yield
        point. Rate limiting keeps a chatty handler from turning it into write
        amplification.
        """
        now = time.monotonic()
        if now - self._last_heartbeat < self._heartbeat_seconds:
            return
        self._last_heartbeat = now
        async with self._scope() as service:
            await service.extend_lease(self._ownership, self._lease_seconds)
