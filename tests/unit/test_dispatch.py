"""W1-04 .. W1-06 — score encoding and the no-Redis fallback."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from app.core.enums import MAX_PRIORITY, MIN_PRIORITY
from app.dispatch.base import (
    MAX_EXACT_SCORE,
    NullDispatch,
    dispatch_score,
)

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
