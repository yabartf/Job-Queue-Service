"""The worker process: slots, maintenance, and an orderly way to stop.

Owns the lifecycle and nothing else — it does not run handlers and does not
build SQL. It is also the only thing that releases a lease on the way out, for a
reason that is not obvious: a slot cannot do it from the ``finally`` of a task
that is being cancelled, because the release would itself be an ``await`` inside
a cancelled task and would simply be cancelled again.
"""

import asyncio
import random
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.clock import Clock, Sleeper, sleep_unless_stopped
from app.core.config import Settings
from app.core.logging import get_logger
from app.dispatch.base import Dispatch
from app.services.execution_service import execution_scope
from app.worker.context import ServiceScope
from app.worker.identity import worker_id
from app.worker.maintenance import Maintenance
from app.worker.slot import Slot


class Worker:
    def __init__(
        self,
        settings: Settings,
        sessions: async_sessionmaker[AsyncSession],
        dispatch: Dispatch,
        clock: Clock,
        *,
        rng: random.Random | None = None,
        sleeper: Sleeper | None = None,
        logger: Any | None = None,
    ) -> None:
        self._settings = settings
        self._dispatch = dispatch
        self._log = logger or get_logger(__name__)
        self._scope: ServiceScope = lambda: execution_scope(
            sessions, clock, rng=rng, logger=self._log
        )

        self.slots = [
            Slot(
                worker_id=worker_id(index),
                scope=self._scope,
                dispatch=dispatch,
                lease_seconds=settings.worker_lease_seconds,
                heartbeat_seconds=settings.worker_heartbeat_seconds,
                poll_interval_seconds=settings.worker_poll_interval_seconds,
                rng=rng,
                sleeper=sleeper,
                logger=self._log,
            )
            for index in range(settings.worker_concurrency)
        ]
        self.maintenance = Maintenance(
            scope=self._scope,
            interval_seconds=settings.maintenance_interval_seconds,
            batch_size=settings.maintenance_batch_size,
            logger=self._log,
        )

    async def run(self, stop: asyncio.Event) -> None:
        """Run until ``stop`` is set, then wind down.

        Slots stop claiming as soon as the event fires but finish the job in
        hand, which is what makes the shutdown graceful rather than merely fast.
        """
        self._log.info(
            "worker.started",
            slots=len(self.slots),
            worker_ids=[slot.worker_id for slot in self.slots],
        )
        slot_tasks = [
            asyncio.create_task(slot.run_forever(stop), name=slot.worker_id) for slot in self.slots
        ]
        background = [
            asyncio.create_task(self.maintenance.run_forever(stop), name="maintenance"),
            asyncio.create_task(self._announce_liveness(stop), name="liveness"),
        ]

        try:
            await stop.wait()
            self._log.info("worker.stopping", in_flight=self._in_flight_count())

            _, unfinished = await asyncio.wait(
                slot_tasks, timeout=self._settings.shutdown_grace_seconds
            )
            if unfinished:
                await self._force_release(unfinished)
        finally:
            # Children do not stop just because this coroutine was cancelled.
            # Without this they would outlive the worker that owns them and
            # keep claiming jobs no one is supervising — so cancelling `run` is
            # a hard stop, which is what a caller cancelling it means.
            for task in (*slot_tasks, *background):
                task.cancel()
            await asyncio.gather(*slot_tasks, *background, return_exceptions=True)

        self._log.info("worker.stopped")

    async def _force_release(self, unfinished: set[asyncio.Task[None]]) -> None:
        """The grace period expired with work still running.

        Cancel it, then hand the leases back explicitly. Letting them lapse
        instead would leave every in-flight job invisible for a full lease
        duration — a minute of unexplained latency on every rolling deploy.
        """
        for task in unfinished:
            task.cancel()
        await asyncio.gather(*unfinished, return_exceptions=True)

        for slot in self.slots:
            ownership = slot.current
            if ownership is None:
                continue
            async with self._scope() as service:
                await service.release(ownership)

    async def _announce_liveness(self, stop: asyncio.Event) -> None:
        """Refresh each slot's Redis key so /health can list live workers.

        Expiring keys rather than rows: liveness is ephemeral observability data
        that nothing depends on for correctness, and "gone if not refreshed" is
        free in Redis and a cleanup sweep anywhere else.
        """
        ttl = self._settings.worker_heartbeat_seconds * 2
        while not stop.is_set():
            for slot in self.slots:
                await self._dispatch.heartbeat_worker(slot.worker_id, ttl)
            if await sleep_unless_stopped(stop, self._settings.worker_heartbeat_seconds):
                return

    def _in_flight_count(self) -> int:
        return sum(1 for slot in self.slots if slot.current is not None)
