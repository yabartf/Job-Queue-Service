"""The background sweep: recover abandoned jobs, promote due ones.

Runs inside every worker process rather than as its own service. Both sweeps are
bounded conditional updates using ``SKIP LOCKED``, so concurrent sweepers take
disjoint batches and never contend — which is what removes the need for a leader
among them, and one more thing to deploy and notice had died.
"""

import asyncio
from typing import Any

from app.core.clock import sleep_unless_stopped
from app.core.logging import get_logger
from app.services.execution_service import SweepResult
from app.worker.context import ServiceScope


class Maintenance:
    def __init__(
        self,
        *,
        scope: ServiceScope,
        interval_seconds: float,
        batch_size: int,
        logger: Any | None = None,
    ) -> None:
        self._scope = scope
        self._interval_seconds = interval_seconds
        self._batch_size = batch_size
        self._log = logger or get_logger(__name__)

    async def run_once(self) -> SweepResult:
        async with self._scope() as service:
            return await service.sweep(self._batch_size)

    async def run_forever(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.run_once()
            except Exception:
                # A sweep that fails must not take the worker down with it: the
                # slots are still processing, and the next pass will retry.
                self._log.exception("maintenance.sweep_failed")
            if await sleep_unless_stopped(stop, self._interval_seconds):
                return
