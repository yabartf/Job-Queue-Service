"""``python -m app.worker``.

The only place that touches signals. Everything below takes an
``asyncio.Event``, so no test ever has to raise one.
"""

import asyncio
import signal

from app.core.clock import SystemClock
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging, get_logger
from app.db.session import create_engine, make_session_factory
from app.dispatch.redis_dispatch import RedisDispatch
from app.worker.runtime import Worker

log = get_logger(__name__)


def install_signal_handlers(stop: asyncio.Event) -> None:
    """Ask the process to wind down on SIGTERM or SIGINT.

    ``loop.add_signal_handler`` is the correct mechanism and does not exist on
    Windows, where development happens; the fallback keeps `python -m app.worker`
    usable there without changing how the worker behaves in its container.
    """
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover - Windows only
            signal.signal(sig, lambda *_: stop.set())


async def run_worker(stop: asyncio.Event, settings: Settings | None = None) -> None:
    """Build the worker's dependencies, run it, and dispose of them.

    Separate from ``main`` so the wiring is exercised by tests without a test
    ever having to send a signal.
    """
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    engine = create_engine(settings.database_url)
    dispatch = RedisDispatch.from_url(settings.redis_url)
    worker = Worker(settings, make_session_factory(engine), dispatch, SystemClock())
    try:
        await worker.run(stop)
    finally:
        await dispatch.close()
        await engine.dispose()


async def main() -> None:  # pragma: no cover - wires signals to run_worker
    stop = asyncio.Event()
    install_signal_handlers(stop)
    await run_worker(stop)


if __name__ == "__main__":  # pragma: no cover - process entry point
    asyncio.run(main())
