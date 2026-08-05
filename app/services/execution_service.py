"""Worker-side use cases: claiming a job and recording what became of it.

This is where the rules live — retry versus permanent failure, what an error
looks like once persisted, which transitions are worth an operator's attention.
The worker's loop is left as pure orchestration, and every rule here is testable
without starting a worker at all.
"""

import random
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.clock import Clock
from app.core.enums import DeadLetterReason, JobStatus
from app.core.logging import get_logger
from app.db.models import Job
from app.db.repository import JobRepository, Ownership
from app.db.session import transaction
from app.jobs.base import JobResult, JobTimeoutError
from app.services.backoff import backoff_delay
from app.services.transitions import record_event, record_transition

#: Persisted error messages are returned through the API, so they are bounded.
MAX_ERROR_MESSAGE_CHARS = 500

LEASE_EXPIRED_MESSAGE = "Worker stopped extending the lease"


@dataclass(frozen=True, slots=True)
class SweepResult:
    """What one maintenance pass changed."""

    released: list[UUID]
    failed: list[UUID]
    promoted: list[UUID]

    @property
    def total(self) -> int:
        return len(self.released) + len(self.failed) + len(self.promoted)


class ExecutionService:
    def __init__(
        self,
        repo: JobRepository,
        clock: Clock,
        rng: random.Random | None = None,
        logger: Any | None = None,
    ) -> None:
        self.repo = repo
        self.clock = clock
        # Injected so tests pin the retry jitter and assert exact delays.
        self.rng = rng or random.Random()
        self.log = logger or get_logger(__name__)

    async def claim(
        self, worker_id: str, lease_seconds: int, hint: UUID | None = None
    ) -> Job | None:
        """Take a job, preferring one Redis named, falling back to a scan.

        A hint that claims nothing is ordinary: it means the job was cancelled,
        already taken, or is not due yet. Both paths end in the same conditional
        UPDATE, so the exactly-once guarantee does not depend on which was used.
        """
        job = None
        if hint is not None:
            job = await self.repo.claim_by_id(hint, worker_id, lease_seconds)
        if job is None:
            job = await self.repo.claim_next(worker_id, lease_seconds)
        if job is None:
            return None

        await record_transition(
            self.repo,
            self.log,
            job,
            event="job.claimed",
            message=f"Claimed by {worker_id}",
            priority=job.priority,
            queued_seconds=self._queued_seconds(job),
            from_hint=hint is not None and job.id == hint,
        )
        return job

    async def complete(self, job: Job, own: Ownership, result: JobResult) -> bool:
        """Record a successful run. False means the job was taken from us."""
        stored = result.model_dump(mode="json")
        if not await self.repo.mark_completed(own, stored):
            await self._report_lost(job, own)
            return False

        await record_transition(
            self.repo,
            self.log,
            job,
            event="job.completed",
            message="Job completed",
            status=JobStatus.COMPLETED,
        )
        return True

    async def fail(
        self, job: Job, own: Ownership, exc: BaseException, *, retryable: bool = True
    ) -> bool:
        """Record a failed run, retrying it or giving up.

        The decision lives here rather than in the worker loop: it is a rule
        about jobs, not about how a worker happens to be structured.

        ``retryable=False`` forces the permanent branch. The one caller that
        needs it is a stored payload that no longer parses: it passed validation
        at submission, so it will fail identically on every remaining attempt,
        and burning a worker twice more to learn that is waste.
        """
        error = self._error_payload(job, exc)

        if not retryable or job.attempts >= job.max_attempts:
            reason = self._dead_letter_reason(exc, retryable=retryable)
            if not await self.repo.mark_failed(own, error, reason):
                await self._report_lost(job, own)
                return False
            await record_transition(
                self.repo,
                self.log,
                job,
                event="job.dead_lettered" if reason else "job.failed",
                message=(
                    f"Job cannot be run and was dead-lettered: {reason}"
                    if reason
                    else f"Job failed permanently after {job.attempts} attempts"
                ),
                level="error",
                status=JobStatus.FAILED,
                error_type=error["type"],
                dead_letter_reason=reason,
            )
            return True

        delay = backoff_delay(job.attempts, self.rng)
        if not await self.repo.reschedule_after_failure(own, error, delay):
            await self._report_lost(job, own)
            return False

        await record_transition(
            self.repo,
            self.log,
            job,
            event="job.failed_attempt",
            message=f"Attempt {job.attempts} failed; retrying",
            level="warning",
            status=JobStatus.PENDING,
            error_type=error["type"],
            retry_in_seconds=round(delay, 3),
        )
        return True

    async def extend_lease(self, own: Ownership, lease_seconds: int) -> bool:
        return await self.repo.extend_lease(own, lease_seconds)

    async def note(self, job_id: UUID, level: str, message: str, **fields: Any) -> None:
        """A line a handler chose to record, kept apart from state transitions."""
        await record_event(
            self.repo,
            self.log,
            job_id,
            event="job.note",
            message=message,
            level=level,
            **fields,
        )

    async def report_progress(self, own: Ownership, percent: int) -> bool:
        return await self.repo.set_progress(own, percent)

    async def release(self, own: Ownership) -> bool:
        """Hand a job back untouched, because this worker is shutting down.

        Doing this rather than letting the lease lapse is what keeps a rolling
        deploy from hiding every in-flight job for a full lease duration.
        """
        released = await self.repo.release_lease(own)
        if released:
            await record_event(
                self.repo,
                self.log,
                own.job_id,
                event="worker.forced_release",
                message="Released mid-flight during shutdown",
                level="warning",
                worker_id=own.worker_id,
                attempt=own.attempts,
                status=JobStatus.PENDING,
            )
        return released

    async def sweep(self, batch: int) -> SweepResult:
        """One maintenance pass: recover abandoned jobs, then promote due ones."""
        reaped = await self.repo.reap_expired_leases(
            batch,
            self._lease_expired_error(),
            # A job that used every attempt without a worker surviving to report
            # anything is the clearest poison there is.
            exhausted_reason=DeadLetterReason.WORKER_CRASH_LOOP,
        )
        promoted = await self.repo.promote_due_scheduled(batch)

        for job_id in reaped.released:
            await record_event(
                self.repo,
                self.log,
                job_id,
                event="job.reaped",
                message="Lease expired; returned to the queue",
                level="warning",
                status=JobStatus.PENDING,
            )
        for job_id in reaped.failed:
            await record_event(
                self.repo,
                self.log,
                job_id,
                event="job.dead_lettered",
                message="Lease expired with no attempts left; the job kills its workers",
                level="error",
                status=JobStatus.FAILED,
                dead_letter_reason=DeadLetterReason.WORKER_CRASH_LOOP,
            )
        for job_id in promoted:
            await record_event(
                self.repo,
                self.log,
                job_id,
                event="job.promoted",
                message="Scheduled time reached; queued",
                status=JobStatus.PENDING,
            )

        return SweepResult(released=reaped.released, failed=reaped.failed, promoted=promoted)

    # ------------------------------------------------------------------

    def _error_payload(self, job: Job, exc: BaseException) -> dict[str, Any]:
        """A failure as it is persisted and returned through the API.

        The type is recorded as a name so failures can be grouped without the
        API layer knowing which exception classes exist, and the message is
        truncated because it is client-visible and otherwise unbounded. No
        traceback: that stays in the log stream.

        No timestamp either. ``jobs.updated_at`` and ``job_logs.created_at``
        already record when this happened, and both are written by the database
        — embedding an application-clock time beside them would give the same
        event two answers that disagree by whatever the skew is.
        """
        return {
            "type": type(exc).__name__,
            "message": str(exc)[:MAX_ERROR_MESSAGE_CHARS],
            "attempt": job.attempts,
        }

    def _dead_letter_reason(self, exc: BaseException, *, retryable: bool) -> str | None:
        """Whether this failure means the job *cannot run*, not just that it failed.

        A handler that raised three times is a failure and nothing more — it did
        its work, the thing it called was down, and retrying it once that
        recovers is exactly right. What lands in the dead-letter set is work no
        retry can help: a payload that will not parse, or a job that has never
        once finished inside its budget.
        """
        if not retryable:
            return DeadLetterReason.UNPROCESSABLE_PAYLOAD
        if isinstance(exc, JobTimeoutError):
            return DeadLetterReason.TIMEOUT_LOOP
        return None

    def _lease_expired_error(self) -> dict[str, Any]:
        return {"type": "LeaseExpired", "message": LEASE_EXPIRED_MESSAGE}

    async def _report_lost(self, job: Job, own: Ownership) -> None:
        """A write matched nothing: the reaper handed this job to someone else.

        Not an error — the system corrected itself — but rare enough in a healthy
        deployment that a run of these means workers are dying or stalling.
        """
        self.log.warning(
            "job.lease_lost",
            job_id=str(own.job_id),
            job_type=job.job_type,
            worker_id=own.worker_id,
            attempt=own.attempts,
        )

    def _queued_seconds(self, job: Job) -> float:
        """How long the job waited to be picked up.

        Both timestamps come from the database, so this is one clock subtracted
        from itself — safe in a way that mixing in the application clock is not.
        """
        started = job.started_at or job.created_at
        return round((started - job.created_at).total_seconds(), 3)


@asynccontextmanager
async def execution_scope(
    sessions: async_sessionmaker[AsyncSession],
    clock: Clock,
    *,
    rng: random.Random | None = None,
    logger: Any | None = None,
) -> AsyncIterator[ExecutionService]:
    """One short unit of work with a service bound to it.

    The worker opens one of these per operation rather than holding a session
    for the length of a job: a handler can run for minutes, and a connection
    checked out across it is a connection the pool cannot reuse.
    """
    async with transaction(sessions) as session:
        yield ExecutionService(JobRepository(session), clock, rng=rng, logger=logger)
