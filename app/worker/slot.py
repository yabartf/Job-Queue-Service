"""One claim-execute-write loop.

Pure orchestration. Every rule about what an outcome *means* — retry or give up,
what an error looks like once stored, which transitions matter — lives in
`ExecutionService`. This file decides only what happens next.
"""

import asyncio
import random
from typing import Any
from uuid import UUID

from app.core.clock import Sleeper
from app.core.errors import DomainError
from app.core.logging import get_logger
from app.db.models import Job
from app.db.repository import Ownership
from app.dispatch.base import Dispatch
from app.jobs.base import BaseJob, JobTimeoutError
from app.jobs.registry import get_job_class
from app.worker.context import DbJobContext, ServiceScope


class Slot:
    """A single unit of worker concurrency.

    ``current`` is published for the worker that owns this slot: on a forced
    shutdown it reads the in-flight ownership and releases the lease. The slot
    cannot do that itself — the release would have to be awaited inside the
    ``finally`` of a task that is already being cancelled, where the await is
    simply cancelled again.
    """

    def __init__(
        self,
        *,
        worker_id: str,
        scope: ServiceScope,
        dispatch: Dispatch,
        lease_seconds: int,
        heartbeat_seconds: float,
        poll_interval_seconds: float,
        rng: random.Random | None = None,
        sleeper: Sleeper | None = None,
        logger: Any | None = None,
    ) -> None:
        self.worker_id = worker_id
        self.current: Ownership | None = None
        self._scope = scope
        self._dispatch = dispatch
        self._lease_seconds = lease_seconds
        self._heartbeat_seconds = heartbeat_seconds
        self._poll_interval_seconds = poll_interval_seconds
        self._rng = rng
        self._sleeper = sleeper
        self._log = logger or get_logger(__name__)
        self._lease_lost = False

    async def run_once(self, hint: UUID | None = None) -> bool:
        """Claim and run at most one job. Returns whether work was done.

        Exists as its own method because almost every test drives it directly: a
        loop that can only be started and stopped forces every test to reason
        about timing, and this one does not.
        """
        async with self._scope() as service:
            job = await service.claim(self.worker_id, self._lease_seconds, hint)
        if job is None:
            return False
        await self._execute(job)
        return True

    async def run_forever(self, stop: asyncio.Event) -> None:
        """Drain continuously; wait only when the queue is genuinely empty.

        A hint arrives as an argument to the next ``run_once`` rather than
        opening a second execution path — there is one way a job gets run.
        """
        hint: UUID | None = None
        while not stop.is_set():
            if await self.run_once(hint):
                hint = None
                continue
            hint = await self._dispatch.next_hint(timeout=self._poll_interval_seconds)

    # ------------------------------------------------------------------

    async def _execute(self, job: Job) -> None:
        own = Ownership.of(job)
        self.current = own
        self._lease_lost = False

        try:
            handler = self._build_handler(job, own)
        except DomainError as exc:
            # The payload passed validation at submission, so failing to parse it
            # now means the schema moved underneath a stored job. Every remaining
            # attempt would fail identically; spending them is waste.
            async with self._scope() as service:
                await service.fail(job, own, exc, retryable=False)
            self.current = None
            return

        run_task = asyncio.create_task(handler.run())
        heartbeat = asyncio.create_task(self._heartbeat(own, run_task))
        shutting_down = False
        try:
            # A wedged handler occupies a slot for a bounded time rather than
            # until its lease expires. The budget belongs to the job type.
            result = await asyncio.wait_for(run_task, type(handler).timeout_seconds)
        except TimeoutError:
            async with self._scope() as service:
                await service.fail(
                    job,
                    own,
                    JobTimeoutError(
                        f"Handler exceeded its {type(handler).timeout_seconds}s budget"
                    ),
                )
        except asyncio.CancelledError:
            # Two cancellations look alike here. Losing the lease is ours to
            # absorb; anything else is the worker shutting us down, and the
            # ownership must survive so it can be released.
            shutting_down = not self._lease_lost
            if shutting_down:
                raise
        except Exception as exc:
            async with self._scope() as service:
                await service.fail(job, own, exc)
        else:
            validated = type(handler).Result.model_validate(result)
            async with self._scope() as service:
                await service.complete(job, own, validated)
        finally:
            heartbeat.cancel()
            if not shutting_down:
                self.current = None

    async def _heartbeat(self, own: Ownership, run_task: asyncio.Task[Any]) -> None:
        """Hold the lease, and stop the work the moment we no longer have it.

        Cancelling is worth doing even though the result would be rejected
        anyway: it bounds the window in which two workers run the same job to
        about one interval, instead of however long the handler had left.
        """
        while True:
            await asyncio.sleep(self._heartbeat_seconds)
            async with self._scope() as service:
                still_ours = await service.extend_lease(own, self._lease_seconds)
            if not still_ours:
                self._lease_lost = True
                self._log.warning(
                    "job.lease_lost",
                    job_id=str(own.job_id),
                    worker_id=own.worker_id,
                    attempt=own.attempts,
                )
                run_task.cancel()
                return

    def _build_handler(self, job: Job, own: Ownership) -> BaseJob:
        job_class = get_job_class(job.job_type)
        payload = job_class.parse_payload(job.payload)
        context = DbJobContext(
            own,
            self._scope,
            heartbeat_seconds=self._heartbeat_seconds,
            lease_seconds=self._lease_seconds,
        )
        return job_class(payload, context, sleeper=self._sleeper, rng=self._rng)
