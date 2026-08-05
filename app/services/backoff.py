"""Retry backoff timing.

See specs/05-retry-and-failure.md section 3 and DECISIONS.md section 4.
"""

import random

BASE_DELAY_SECONDS = 30.0
#: Growth factor 4, not the more usual 2, so that the two delays the assignment
#: specifies — 30 seconds then 2 minutes — fall out of one formula instead of
#: being a hard-coded lookup table with an exponential bolted on after it.
GROWTH_FACTOR = 4.0
MAX_DELAY_SECONDS = 3600.0


def backoff_delay(failed_attempt: int, rng: random.Random) -> float:
    """Seconds to wait after ``failed_attempt`` before trying again.

    Equal jitter: the result is drawn from ``[base/2, base]``. Jitter matters
    because a downstream outage fails every in-flight job at roughly the same
    moment; without it they all retry at the same instant, and again after the
    same delay, turning recovery into a self-inflicted load spike that stays
    synchronised across every round.

    Equal rather than full jitter (``[0, base]``) because the requirement is
    stated as a concrete duration, and a retry landing two seconds after a
    failure would look like the delay was never implemented. The property that
    matters — breaking the synchronisation — survives either way.
    """
    if failed_attempt < 1:
        raise ValueError(f"failed_attempt must be >= 1, got {failed_attempt}")

    base = min(
        BASE_DELAY_SECONDS * GROWTH_FACTOR ** (failed_attempt - 1),
        MAX_DELAY_SECONDS,
    )
    return base / 2 + rng.uniform(0.0, base / 2)
