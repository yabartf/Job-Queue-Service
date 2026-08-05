"""Redis as a dispatch hint.

Nothing here decides anything: every id handed out is revalidated against
PostgreSQL by a conditional UPDATE before work begins. That is what lets every
method below swallow its own failures — a Redis outage costs latency, never
correctness, and the caller has a working fallback either way.
"""

from datetime import datetime
from typing import Any
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.core.logging import get_logger
from app.dispatch.base import dispatch_score

READY_KEY = "jobs:ready"
WORKER_KEY_PREFIX = "worker:"

#: How much longer the socket may wait than the command it is carrying.
#:
#: redis-py defaults ``socket_timeout`` to 5 seconds, and a blocking pop that
#: asks the *server* to wait that long loses the race against its own socket:
#: the read times out first and the pop raises instead of returning empty. The
#: symptom is a warning on every idle poll — indistinguishable from the warning
#: that means Redis is actually gone — plus a discarded connection each time.
#: So the read timeout is derived from the longest block the caller will ask
#: for, rather than left to a library default nothing here can see.
BLOCK_TIMEOUT_MARGIN_SECONDS = 5.0


class RedisDispatch:
    def __init__(self, client: Redis, logger: Any | None = None) -> None:
        self._client = client
        self._log = logger or get_logger(__name__)

    @classmethod
    def from_url(
        cls,
        url: str,
        logger: Any | None = None,
        *,
        max_block_seconds: float = 0.0,
    ) -> "RedisDispatch":
        # decode_responses so members come back as str rather than bytes; the
        # only things stored are ids and worker names.
        #
        # max_block_seconds is 0 for the API, which never blocks; the worker
        # passes its poll interval.
        return cls(
            Redis.from_url(
                url,
                decode_responses=True,
                socket_timeout=max_block_seconds + BLOCK_TIMEOUT_MARGIN_SECONDS,
            ),
            logger,
        )

    async def announce(self, job_id: UUID, priority: int, created_at: datetime) -> None:
        try:
            await self._client.zadd(READY_KEY, {str(job_id): dispatch_score(priority, created_at)})
        except RedisError:
            # The job is already durably pending; the fallback claim will find
            # it. Failing a client's submission over a cache write would be the
            # wrong trade.
            self._log.warning("dispatch.announce_failed", job_id=str(job_id))

    async def next_hint(self, timeout: float | None = None) -> UUID | None:
        """Take the next hinted id.

        ``timeout=None`` is the non-blocking form. It cannot be ``0``: Redis
        reads a zero timeout on ``BZPOPMIN`` as "block forever", which would look
        exactly like a wedged worker.
        """
        member: str | None = None
        try:
            if timeout is None:
                popped = await self._client.zpopmin(READY_KEY)
                # redis-py types members loosely; decode_responses guarantees str.
                member = str(popped[0][0]) if popped else None
            else:
                result = await self._client.bzpopmin(READY_KEY, timeout=timeout)
                member = str(result[1]) if result else None
        except RedisError:
            self._log.warning("dispatch.next_hint_failed")
            return None

        return UUID(member) if member else None

    async def heartbeat_worker(self, worker_id: str, ttl_seconds: int) -> None:
        try:
            await self._client.set(f"{WORKER_KEY_PREFIX}{worker_id}", "alive", ex=ttl_seconds)
        except RedisError:
            self._log.warning("dispatch.worker_heartbeat_failed", worker_id=worker_id)

    async def active_workers(self) -> list[str] | None:
        """Live worker ids, or None when Redis cannot say.

        None rather than an empty list: "I cannot see the workers" and "there are
        no workers" are different incidents, and an operator told the second when
        the first is true will go and restart healthy workers.

        SCAN rather than KEYS — KEYS blocks the server for its whole duration,
        and a health endpoint must never be the thing that stalls Redis.
        """
        try:
            keys = [key async for key in self._client.scan_iter(match=f"{WORKER_KEY_PREFIX}*")]
        except RedisError:
            self._log.warning("dispatch.active_workers_failed")
            return None
        return sorted(key.removeprefix(WORKER_KEY_PREFIX) for key in keys)

    async def ready_depth(self) -> int | None:
        try:
            return int(await self._client.zcard(READY_KEY))
        except RedisError:
            self._log.warning("dispatch.ready_depth_failed")
            return None

    async def close(self) -> None:
        try:
            await self._client.aclose()
        except RedisError:  # pragma: no cover - closing a broken client
            self._log.warning("dispatch.close_failed")
