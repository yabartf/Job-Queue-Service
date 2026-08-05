"""Job status and type enumerations, and the transitions between statuses.

This module is the authoritative definition; the CHECK constraints in the
migration mirror it. See specs/01-data-model.md section 3.
"""

from enum import StrEnum


class JobStatus(StrEnum):
    SCHEDULED = "scheduled"
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobType(StrEnum):
    EMAIL = "email"
    WEBHOOK = "webhook"
    REPORT = "report"
    BATCH = "batch"


class DeadLetterReason(StrEnum):
    """Why a failed job is considered un-runnable rather than merely failed.

    A job that exhausted its attempts on ordinary handler exceptions has none of
    these: it did its work and the thing it depended on was down, and retrying it
    later is exactly right. These three are the cases where retrying the job
    unchanged cannot help. See specs/09-hardening.md section 4.
    """

    #: The stored payload no longer parses, or its type is not registered.
    UNPROCESSABLE_PAYLOAD = "unprocessable_payload"
    #: Every attempt exceeded the job type's time budget.
    TIMEOUT_LOOP = "timeout_loop"
    #: Every attempt ended with the worker dying rather than reporting anything.
    WORKER_CRASH_LOOP = "worker_crash_loop"


#: Priority is a bounded, coarse class rather than a free integer. An unbounded
#: value would let a client pre-empt the queue permanently, and it is also what
#: keeps the Redis dispatch score inside float64's exact-integer range
#: (specs/01-data-model.md section 4, specs/07-redis-dispatch.md section 4).
#: The ck_jobs_priority constraint mirrors these bounds.
MIN_PRIORITY = 0
MAX_PRIORITY = 9


#: Statuses a job can be cancelled from. Cancelling work already in flight would
#: require cooperatively interrupting a running handler, which is out of scope.
CANCELLABLE_STATUSES: frozenset[JobStatus] = frozenset({JobStatus.PENDING, JobStatus.SCHEDULED})

#: Statuses from which no further transition is possible. FAILED is deliberately
#: absent: a failed job can be manually retried back into PENDING.
TERMINAL_STATUSES: frozenset[JobStatus] = frozenset({JobStatus.COMPLETED, JobStatus.CANCELLED})

ALLOWED_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.SCHEDULED: frozenset({JobStatus.PENDING, JobStatus.CANCELLED}),
    JobStatus.PENDING: frozenset({JobStatus.PROCESSING, JobStatus.CANCELLED}),
    # PROCESSING -> PENDING covers both a failed attempt with retries remaining
    # and a lease expiry released by the reaper.
    JobStatus.PROCESSING: frozenset({JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.PENDING}),
    JobStatus.FAILED: frozenset({JobStatus.PENDING}),
    JobStatus.COMPLETED: frozenset(),
    JobStatus.CANCELLED: frozenset(),
}


def can_transition(source: JobStatus, target: JobStatus) -> bool:
    """Whether ``source -> target`` is a legal state change."""
    return target in ALLOWED_TRANSITIONS[source]
