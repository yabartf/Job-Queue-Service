"""The dispatch surface: what Redis is asked to do, and what it means without it.

Redis holds the set of jobs that are ready to run and is the fast path workers
pull from. **It decides nothing.** Every id it hands out is revalidated against
PostgreSQL by a conditional UPDATE before any work begins, which is what lets
the whole system keep running correctly when Redis is not there.

See specs/07-redis-dispatch.md.
"""

import asyncio
from datetime import datetime
from typing import Protocol
from uuid import UUID

from app.core.enums import MAX_PRIORITY, MIN_PRIORITY

#: Multiplier that separates the priority band from the timestamp band inside a
#: single sorted-set score. 10^13 milliseconds is roughly 317 years, so the
#: timestamp cannot overflow into the priority band before the year 2286.
SCORE_PRIORITY_SCALE = 10**13

#: float64 represents integers exactly only up to 2^53. Sorted-set scores are
#: float64, and exceeding this does not raise — scores round, and FIFO ordering
#: within a priority degrades with no error anywhere. Asserted in tests.
MAX_EXACT_SCORE = 2**53


def dispatch_score(priority: int, created_at: datetime) -> float:
    """Encode priority and submission time into one orderable score.

    ``BZPOPMIN`` pops the lowest score, so priority is inverted to make the most
    urgent job pop first, and the submission timestamp orders oldest-first within
    a priority band. One key, one command, both orderings — matching the SQL in
    specs/04-claiming.md so the two claim paths agree on what comes next.
    """
    if not MIN_PRIORITY <= priority <= MAX_PRIORITY:
        raise ValueError(
            f"priority {priority} is outside {MIN_PRIORITY}..{MAX_PRIORITY}; "
            "the score encoding is only exact within that range"
        )
    inverted = MAX_PRIORITY - priority
    return float(inverted * SCORE_PRIORITY_SCALE + int(created_at.timestamp() * 1000))


class Dispatch(Protocol):
    """Redis, or a stand-in for it.

    **No method here raises.** A dispatch failure costs latency and nothing
    else — every caller has a working fallback through PostgreSQL — so
    implementations log and degrade rather than propagating, and callers stay
    free of error handling for a dependency that cannot break them.

    ``active_workers`` and ``ready_depth`` return ``None`` when the answer is
    unknown rather than zero. "I cannot see the workers" and "there are no
    workers" are different incidents, and the type says so.
    """

    async def announce(self, job_id: UUID, priority: int, created_at: datetime) -> None:
        """Publish a hint that a job is ready. Only ever called after commit."""
        ...

    async def next_hint(self, timeout: float | None = None) -> UUID | None:
        """Take the next hinted job id.

        ``timeout=None`` returns immediately; a timeout blocks for up to that
        long. None rather than 0 is the non-blocking value because Redis reads a
        zero timeout as "block forever".
        """
        ...

    async def heartbeat_worker(self, worker_id: str, ttl_seconds: int) -> None: ...

    async def active_workers(self) -> list[str] | None: ...

    async def ready_depth(self) -> int | None: ...

    async def close(self) -> None: ...


class NullDispatch:
    """Dispatch with no Redis behind it.

    Not only a test double: this is what the API and worker fall back to when
    Redis is unreachable. Jobs still flow, through the PostgreSQL claim, which is
    why the whole worker suite runs against this — the path that guarantees
    correctness is the one under test.
    """

    async def announce(self, job_id: UUID, priority: int, created_at: datetime) -> None:
        return None

    async def next_hint(self, timeout: float | None = None) -> UUID | None:
        # Honouring the timeout is what keeps the worker loop from spinning when
        # there is nothing to do; without it the fallback claim would hot-loop.
        if timeout is not None:
            await asyncio.sleep(timeout)
        return None

    async def heartbeat_worker(self, worker_id: str, ttl_seconds: int) -> None:
        return None

    async def active_workers(self) -> list[str] | None:
        return None

    async def ready_depth(self) -> int | None:
        return None

    async def close(self) -> None:
        return None
