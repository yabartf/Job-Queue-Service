"""W1-04 .. W1-06 — score encoding and the no-Redis fallback."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from redis.exceptions import RedisError

from app.core.enums import MAX_PRIORITY, MIN_PRIORITY
from app.dispatch.base import (
    MAX_EXACT_SCORE,
    NullDispatch,
    dispatch_score,
)
from app.dispatch.redis_dispatch import BLOCK_TIMEOUT_MARGIN_SECONDS, RedisDispatch

NOW = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)


def test_w1_04_higher_priority_sorts_first():
    """BZPOPMIN pops the lowest score, so the most urgent job must score lowest."""
    scores = [dispatch_score(priority, NOW) for priority in range(MIN_PRIORITY, MAX_PRIORITY + 1)]

    assert scores == sorted(scores, reverse=True)
    assert dispatch_score(9, NOW) < dispatch_score(0, NOW)


def test_w1_04b_oldest_first_within_a_priority():
    older = dispatch_score(5, NOW)
    newer = dispatch_score(5, NOW + timedelta(seconds=1))

    assert older < newer


def test_w1_04c_priority_outranks_age():
    """A brand-new urgent job beats an ancient unimportant one."""
    ancient_low = dispatch_score(0, NOW - timedelta(days=365))
    fresh_high = dispatch_score(9, NOW)

    assert fresh_high < ancient_low


@pytest.mark.parametrize("priority", range(MIN_PRIORITY, MAX_PRIORITY + 1))
@pytest.mark.parametrize("year", [2026, 2100, 2285])
def test_w1_05_scores_stay_inside_float64s_exact_range(priority, year):
    """Beyond 2^53 the score would round and FIFO ordering would degrade with no
    error anywhere — the failure this bound exists to prevent is a silent one."""
    score = dispatch_score(priority, datetime(year, 1, 1, tzinfo=UTC))

    assert score < MAX_EXACT_SCORE
    assert float(int(score)) == score  # representable exactly


def test_w1_05b_the_timestamp_never_bleeds_into_the_priority_band():
    """Two adjacent priorities must not overlap however far in the future the
    timestamp is."""
    far_future = datetime(2285, 12, 31, tzinfo=UTC)

    assert dispatch_score(5, far_future) < dispatch_score(4, NOW)


@pytest.mark.parametrize("priority", [MIN_PRIORITY - 1, MAX_PRIORITY + 1, 100])
def test_w1_05c_priorities_outside_the_bound_are_rejected(priority):
    with pytest.raises(ValueError, match="outside"):
        dispatch_score(priority, NOW)


# ---------------------------------------------------------------------------
# W1-06 — NullDispatch
# ---------------------------------------------------------------------------


async def test_w1_06_null_dispatch_never_hands_out_work():
    dispatch = NullDispatch()

    await dispatch.announce(uuid4(), 5, NOW)

    assert await dispatch.next_hint() is None


async def test_w1_06b_null_dispatch_reports_unknown_not_zero():
    """The distinction the whole health contract rests on: no visibility into
    the workers is not the same as having no workers."""
    dispatch = NullDispatch()

    assert await dispatch.active_workers() is None
    assert await dispatch.ready_depth() is None


async def test_w1_06c_null_dispatch_honours_a_timeout(monkeypatch):
    """Without this the worker loop would spin instead of waiting."""
    slept: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr("app.dispatch.base.asyncio.sleep", fake_sleep)
    dispatch = NullDispatch()

    assert await dispatch.next_hint(timeout=5.0) is None
    assert slept == [5.0]


async def test_w1_06d_null_dispatch_closes_and_heartbeats_quietly():
    dispatch = NullDispatch()

    await dispatch.heartbeat_worker("w-0", 40)
    await dispatch.close()


# ---------------------------------------------------------------------------
# W1-14 — the socket outlives the command it carries
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("block", [0.0, 1.0, 5.0, 30.0])
def test_w1_14_the_read_timeout_clears_the_longest_block(block):
    """redis-py defaults `socket_timeout` to 5 s. A blocking pop asking the
    server to wait that long loses the race with its own socket, so every idle
    poll raises instead of returning empty — logged as the same failure that
    means Redis is gone, and costing a discarded connection each time. The
    client's read budget is therefore derived from the block, not inherited.
    """
    dispatch = RedisDispatch.from_url("redis://localhost:6379/0", max_block_seconds=block)

    socket_timeout = dispatch._client.connection_pool.connection_kwargs["socket_timeout"]

    assert socket_timeout > block
    assert socket_timeout == block + BLOCK_TIMEOUT_MARGIN_SECONDS


def test_w1_14b_the_default_matches_a_caller_that_never_blocks():
    """The API only ever announces and reads depth, so it keeps redis-py's own
    default rather than an inflated one."""
    dispatch = RedisDispatch.from_url("redis://localhost:6379/0")

    kwargs = dispatch._client.connection_pool.connection_kwargs

    assert kwargs["socket_timeout"] == BLOCK_TIMEOUT_MARGIN_SECONDS


# ---------------------------------------------------------------------------
# W1-14c — a failed hint still costs the caller its block
# ---------------------------------------------------------------------------


class FailingClient:
    """A client that cannot reach Redis. Both pops raise, as a refused
    connection does, and neither waits for the timeout it was handed."""

    async def zpopmin(self, key: str) -> object:
        raise RedisError("connection refused")

    async def bzpopmin(self, key: str, timeout: float) -> object:
        raise RedisError("connection refused")


@pytest.fixture
def paced(monkeypatch):
    """What `next_hint` waited, without the test waiting it too."""
    slept: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr("app.dispatch.redis_dispatch.asyncio.sleep", fake_sleep)
    return slept


def spend(monkeypatch, seconds: float) -> None:
    """Make the failing call appear to have taken `seconds`, instantly.

    The module imports `monotonic` by name so this replaces one name in one
    module. Reaching through `redis_dispatch.time.monotonic` would patch the
    stdlib module every other caller shares, pytest included.
    """
    readings = iter([100.0, 100.0 + seconds])
    monkeypatch.setattr("app.dispatch.redis_dispatch.monotonic", lambda: next(readings))


async def test_w1_14c_a_failed_hint_waits_out_the_block_it_was_given(paced, monkeypatch):
    """The hazard NullDispatch's timeout exists to avoid, in the class that
    actually runs in production.

    `Slot.run_forever` has no sleep in its idle path — this call is the only
    pacing it has. A refused connection fails in a round trip rather than in the
    interval the caller asked to wait, so returning immediately leaves the slot
    spinning: claim query, refused connect, repeat, for as long as Redis is down.
    Measured against a live stack before this was fixed, 49 idle cycles in twenty
    seconds where the poll interval intends 8.

    Latency is not the cost. The cost is the claim query each cycle fires at
    PostgreSQL — the one database the Redis outage has not taken away.
    """
    spend(monkeypatch, 0.0)
    dispatch = RedisDispatch(FailingClient())  # type: ignore[arg-type]

    assert await dispatch.next_hint(timeout=5.0) is None

    assert paced == [5.0]


async def test_a_failed_hint_waits_only_the_remainder(paced, monkeypatch):
    """A read that timed out has already spent the block. Sleeping the whole
    timeout again would double the idle interval every cycle."""
    spend(monkeypatch, 4.0)
    dispatch = RedisDispatch(FailingClient())  # type: ignore[arg-type]

    assert await dispatch.next_hint(timeout=5.0) is None

    assert paced == [1.0]


async def test_a_failure_that_used_the_whole_block_does_not_wait_again(paced, monkeypatch):
    spend(monkeypatch, 6.0)
    dispatch = RedisDispatch(FailingClient())  # type: ignore[arg-type]

    assert await dispatch.next_hint(timeout=5.0) is None

    assert paced == []


async def test_the_non_blocking_form_is_never_paced(paced, monkeypatch):
    """`timeout=None` is the API's form — a submission and a health check both
    use it. Only a caller that asked to wait may be made to."""
    spend(monkeypatch, 0.0)
    dispatch = RedisDispatch(FailingClient())  # type: ignore[arg-type]

    assert await dispatch.next_hint() is None

    assert paced == []
