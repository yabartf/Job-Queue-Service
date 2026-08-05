"""Injectable time.

Scheduling and retry backoff are both defined in terms of "now". Reading the
clock through an injected object rather than calling ``datetime.now`` directly
is what lets those behaviours be tested without the suite waiting in real time.
"""

import asyncio
from datetime import UTC, datetime
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime:
        """Current time, always timezone-aware and in UTC."""
        ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class Sleeper(Protocol):
    async def sleep(self, seconds: float) -> None: ...


class RealSleeper:
    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


async def sleep_unless_stopped(stop: asyncio.Event, seconds: float) -> bool:
    """Wait for ``seconds``, or return early once ``stop`` is set.

    Returns True when the stop event fired. Background loops use this instead of
    a plain sleep so that a shutdown does not have to wait out a full interval
    before anything notices.
    """
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        return False
    return True
