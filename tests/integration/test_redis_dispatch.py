"""W2-18 — the Redis dispatch against a real Redis.

Skipped when no server is reachable. That is deliberate: Redis is not required
for the system to be correct, so it must not be required for the suite to run
either — and everything that *is* required is covered without it.
"""

import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from redis.asyncio import Redis
from redis.exceptions import RedisError
from structlog.testing import capture_logs

from app.core.config import get_settings
from app.dispatch.redis_dispatch import READY_KEY, RedisDispatch

NOW = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)

#: Long enough to be a real blocking wait, short enough to run in a suite.
BLOCK = 0.3


@pytest.fixture
def redis_url() -> str:
    return os.getenv("TEST_REDIS_URL") or get_settings().test_redis_url


@pytest.fixture
async def dispatch(redis_url):
    client = Redis.from_url(redis_url, decode_responses=True)
    try:
        await client.ping()
    except (RedisError, OSError):
        await client.aclose()
        pytest.skip("no Redis reachable at TEST_REDIS_URL")

    await client.flushdb()
    instance = RedisDispatch(client)
    yield instance
    await client.flushdb()
    await instance.close()


async def test_w2_18_an_announced_job_comes_back_as_a_hint(dispatch):
    job_id = uuid4()

    await dispatch.announce(job_id, 5, NOW)

    assert await dispatch.next_hint() == job_id


async def test_w2_18b_an_empty_set_hands_out_nothing(dispatch):
    assert await dispatch.next_hint() is None


async def test_w2_18c_hints_arrive_in_priority_then_age_order(dispatch):
    low = uuid4()
    high = uuid4()
    older_high = uuid4()
    await dispatch.announce(low, 1, NOW)
    await dispatch.announce(high, 9, NOW)
    await dispatch.announce(older_high, 9, NOW - timedelta(minutes=5))

    order = [await dispatch.next_hint() for _ in range(3)]

    assert order == [older_high, high, low]


async def test_w2_18d_a_hint_is_handed_out_once(dispatch):
    job_id = uuid4()
    await dispatch.announce(job_id, 5, NOW)

    assert await dispatch.next_hint() == job_id
    assert await dispatch.next_hint() is None


async def test_announcing_the_same_job_twice_leaves_one_entry(dispatch):
    """ZADD updates a member's score rather than adding a second copy, so a
    duplicate announcement is absorbed instead of producing a double hint."""
    job_id = uuid4()

    await dispatch.announce(job_id, 5, NOW)
    await dispatch.announce(job_id, 5, NOW)

    assert await dispatch.ready_depth() == 1


async def test_ready_depth_tracks_the_set(dispatch):
    assert await dispatch.ready_depth() == 0

    for _ in range(3):
        await dispatch.announce(uuid4(), 5, NOW)

    assert await dispatch.ready_depth() == 3


async def test_a_blocking_wait_times_out_without_work(dispatch):
    """The timeout must be honoured — a zero would mean 'block forever' to
    Redis, which is indistinguishable from a wedged worker."""
    assert await dispatch.next_hint(timeout=0.1) is None


async def test_a_blocking_wait_returns_work_that_is_already_there(dispatch):
    job_id = uuid4()
    await dispatch.announce(job_id, 5, NOW)

    assert await dispatch.next_hint(timeout=1) == job_id


async def test_w1_14b_an_idle_poll_is_not_reported_as_a_redis_failure(dispatch, redis_url):
    """Why `from_url` derives the read timeout instead of inheriting one.

    A client whose socket budget only matches the block it is carrying loses the
    race against its own read: the pop raises instead of returning empty, and an
    idle poll is reported as a Redis failure that never happened — the same log
    line that means the server is gone, emitted every interval by every slot
    against a perfectly healthy Redis.

    This shipped: redis-py's 5 s default against a 5 s poll interval. The
    invariant is pinned here at a duration the suite can afford, since what
    matters is the relationship between the two numbers, not their size.
    """
    inherited = RedisDispatch(
        Redis.from_url(redis_url, decode_responses=True, socket_timeout=BLOCK)
    )
    derived = RedisDispatch.from_url(redis_url, max_block_seconds=BLOCK)

    try:
        with capture_logs() as inherited_logs:
            assert await inherited.next_hint(timeout=BLOCK) is None
        with capture_logs() as derived_logs:
            assert await derived.next_hint(timeout=BLOCK) is None
    finally:
        await inherited.close()
        await derived.close()

    # Both degrade to "no hint" — only one of them had anything to degrade from.
    assert any(e["event"] == "dispatch.next_hint_failed" for e in inherited_logs)
    assert not any(e["event"] == "dispatch.next_hint_failed" for e in derived_logs)


async def test_worker_liveness_expires_on_its_own(dispatch):
    await dispatch.heartbeat_worker("host-1-0", ttl_seconds=60)
    await dispatch.heartbeat_worker("host-1-1", ttl_seconds=60)

    assert await dispatch.active_workers() == ["host-1-0", "host-1-1"]


async def test_no_workers_registered_reads_as_an_empty_list(dispatch):
    """Empty means 'Redis answered, and there are none' — distinct from the
    None that means 'Redis could not answer'."""
    assert await dispatch.active_workers() == []


async def test_worker_keys_do_not_collide_with_the_ready_set(dispatch):
    await dispatch.announce(uuid4(), 5, NOW)
    await dispatch.heartbeat_worker("host-1-0", ttl_seconds=60)

    assert await dispatch.active_workers() == ["host-1-0"]
    assert await dispatch.ready_depth() == 1


# ---------------------------------------------------------------------------
# Degradation — every method reports rather than raises
# ---------------------------------------------------------------------------


@pytest.fixture
async def broken():
    """A dispatch pointed at a port with nothing behind it."""
    instance = RedisDispatch.from_url("redis://127.0.0.1:6390/0")
    yield instance
    await instance.close()


async def test_announcing_to_a_dead_redis_does_not_raise(broken):
    await broken.announce(uuid4(), 5, NOW)


async def test_a_dead_redis_hands_out_no_hints(broken):
    assert await broken.next_hint() is None
    assert await broken.next_hint(timeout=0.1) is None


async def test_a_dead_redis_reports_unknown_rather_than_zero(broken):
    """The distinction the health contract depends on."""
    assert await broken.active_workers() is None
    assert await broken.ready_depth() is None


async def test_worker_heartbeats_to_a_dead_redis_do_not_raise(broken):
    await broken.heartbeat_worker("host-1-0", ttl_seconds=60)


async def test_the_ready_key_is_the_documented_one(dispatch):
    await dispatch.announce(uuid4(), 5, NOW)

    assert await dispatch._client.zcard(READY_KEY) == 1
