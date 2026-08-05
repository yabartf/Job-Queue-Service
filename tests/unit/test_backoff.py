"""W1-01 .. W1-03 — retry backoff timing."""

import pytest

from app.services.backoff import (
    BASE_DELAY_SECONDS,
    GROWTH_FACTOR,
    MAX_DELAY_SECONDS,
    backoff_delay,
)
from tests.doubles import StubRandom

#: StubRandom(0.0) puts uniform() at its floor, StubRandom(1.0) at its ceiling.
LOWEST = StubRandom(0.0)
HIGHEST = StubRandom(1.0)


@pytest.mark.parametrize(
    ("failed_attempt", "base"),
    [(1, 30.0), (2, 120.0), (3, 480.0)],
)
def test_w1_01_delays_follow_the_specified_schedule(failed_attempt, base):
    """30 s then 2 minutes are the values the assignment names; the third comes
    out of the same formula rather than a separate rule."""
    assert backoff_delay(failed_attempt, HIGHEST) == base
    assert backoff_delay(failed_attempt, LOWEST) == base / 2


@pytest.mark.parametrize("failed_attempt", range(1, 6))
def test_w1_02_equal_jitter_stays_within_half_the_base(failed_attempt):
    base = min(BASE_DELAY_SECONDS * GROWTH_FACTOR ** (failed_attempt - 1), MAX_DELAY_SECONDS)

    for value in (0.0, 0.25, 0.5, 0.75, 1.0):
        delay = backoff_delay(failed_attempt, StubRandom(value))
        assert base / 2 <= delay <= base


def test_w1_02b_jitter_actually_varies():
    """Equal jitter, not a fixed delay: a downstream outage fails many jobs at
    once, and identical delays would have them all retry in lockstep."""
    delays = {backoff_delay(1, StubRandom(v)) for v in (0.0, 0.3, 0.6, 0.9)}

    assert len(delays) == 4


#: Attempts up to this point grow freely; beyond it the cap binds.
LAST_UNCAPPED_ATTEMPT = 4


def test_w1_03_delays_never_overlap_while_the_cap_is_not_binding():
    """Below the cap the ranges are disjoint: the longest a given attempt can
    wait is still shorter than the shortest the next one can."""
    for failed_attempt in range(1, LAST_UNCAPPED_ATTEMPT):
        latest = backoff_delay(failed_attempt, HIGHEST)
        earliest_next = backoff_delay(failed_attempt + 1, LOWEST)
        assert earliest_next >= latest


def test_w1_03b_the_cap_flattens_growth_rather_than_extending_it():
    """Once the cap binds, every further attempt draws from the same range.

    Monotonicity necessarily stops here — a capped attempt can wait less than an
    uncapped predecessor. That is the point of a cap, and the values involved
    are half an hour to an hour apart, so nothing meaningful is lost.
    """
    capped = [backoff_delay(n, HIGHEST) for n in range(6, 12)]

    assert capped == [MAX_DELAY_SECONDS] * len(capped)
    assert backoff_delay(20, LOWEST) == MAX_DELAY_SECONDS / 2


def test_attempt_numbers_below_one_are_rejected():
    """Attempts are 1-based because the claim increments before execution; a 0
    would mean the caller is reading the counter wrong."""
    with pytest.raises(ValueError, match="must be >= 1"):
        backoff_delay(0, LOWEST)
