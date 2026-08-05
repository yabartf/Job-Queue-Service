"""W4-03 — exactly-once through the *Redis* hint path, under concurrency.

Every other concurrency test runs against ``NullDispatch``, so they exercise the
PostgreSQL fallback claim. That is deliberate — the fallback is what guarantees
correctness — but it leaves the path that actually runs in production as the one
never put under concurrent load.

Reproducing that path takes care. A slot consults Redis only when its own scan
came back empty (`Slot.run_forever`: drain continuously, block on a hint only
when the queue is genuinely empty), so seeding a backlog and *then* starting a
worker exercises the fallback with Redis merely attached — a test that passes
identically with no dispatch at all, which is precisely the failure this suite
has already been bitten by once (AI_USAGE.md).

So the worker starts first and settles into a blocking `BZPOPMIN`, and the work
arrives afterwards. The assertion that the hint path really ran is not "it still
works" but the `from_hint` flag the claim itself records in `job_logs`.
"""

import asyncio
import os

import pytest
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import func, select, text

from app.core.config import get_settings
from app.core.enums import JobStatus
from app.db.models import Job
from app.dispatch.redis_dispatch import RedisDispatch
from tests.load.test_throughput import build_worker, count_where, seed_backlog

pytestmark = pytest.mark.committing

JOBS = 40

#: Long enough that a slot spends most of its time inside BZPOPMIN rather than
#: between polls, which is what makes an arriving job reach it as a hint.
POLL = 1.0

#: The same budget `drain` allows in test_throughput. A wait with no ceiling is
#: the wrong failure for this test in particular: the defect it exists to catch
#: is a hint path that stops handing work out, and that is exactly the shape
#: that would leave the loop below spinning until CI kills the whole suite
#: instead of failing here with a name.
TIMEOUT = 60.0


@pytest.fixture
async def dispatch():
    url = os.getenv("TEST_REDIS_URL") or get_settings().test_redis_url
    admin = Redis.from_url(url, decode_responses=True)
    try:
        await admin.ping()
    except (RedisError, OSError):
        await admin.aclose()
        pytest.skip("no Redis reachable at TEST_REDIS_URL")

    await admin.flushdb()
    # Built through `from_url`, the way the worker builds it — not
    # `RedisDispatch(Redis.from_url(...))`, which inherits redis-py's socket
    # defaults instead of the read budget derived from the block this test asks
    # for. With no derived budget a regression there is invisible here, and the
    # one test billed as running the production path would not be running it.
    # Housekeeping goes through a separate client, since a dispatch exposes none.
    instance = RedisDispatch.from_url(url, max_block_seconds=POLL)
    try:
        yield instance
    finally:
        await instance.close()
        await admin.flushdb()
        await admin.aclose()


async def announce_everything(sessions, dispatch) -> None:
    """Hint every job in the table, exactly as submission does after its commit."""
    async with sessions() as session:
        rows = (await session.execute(select(Job.id, Job.priority, Job.created_at))).all()
    for job_id, priority, created_at in rows:
        await dispatch.announce(job_id, priority, created_at)


async def drained(sessions) -> None:
    while await count_where(sessions, "status IN ('pending','processing')"):
        await asyncio.sleep(0.02)


async def claims_from_a_hint(sessions) -> int:
    async with sessions() as session:
        return int(
            (
                await session.execute(
                    text("SELECT count(*) FROM job_logs WHERE (meta->>'from_hint')::boolean")
                )
            ).scalar_one()
        )


async def test_w4_03_a_backlog_arriving_at_idle_workers_runs_exactly_once(
    pooled_sessions, clock, executions, dispatch
):
    worker = build_worker(
        pooled_sessions, clock, dispatch=dispatch, worker_poll_interval_seconds=POLL
    )
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))
    # Let every slot find nothing and settle into its blocking wait.
    await asyncio.sleep(0.3)

    await seed_backlog(pooled_sessions, count=JOBS)
    await announce_everything(pooled_sessions, dispatch)

    try:
        await asyncio.wait_for(drained(pooled_sessions), timeout=TIMEOUT)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=TIMEOUT)

    assert len(executions) == JOBS, "some job ran more or fewer times than once"
    assert len(set(executions)) == JOBS, "a job was executed twice"

    async with pooled_sessions() as session:
        by_status = dict(
            (await session.execute(select(Job.status, func.count()).group_by(Job.status))).all()
        )
    assert by_status == {JobStatus.COMPLETED: JOBS}

    # The point of the test. Without this the whole thing passes against
    # NullDispatch: the fallback claim carries the work and the system looks
    # exactly as healthy as it does when the dispatch is wired correctly.
    #
    # Not an exact count, deliberately: once a slot is awake it drains by
    # scanning rather than going back to Redis for each job, so most of the
    # backlog arrives through the fallback and only the wake-ups are hints. Both
    # paths end in the same conditional UPDATE, which is why the exactly-once
    # assertion above holds regardless of the split.
    assert await claims_from_a_hint(pooled_sessions) > 0, "no job was claimed through Redis"
