"""Use cases: submit, read, list and cancel jobs.

Owns the business rules and the transaction's unit of work. Contains no HTTP
concepts — it raises domain errors and lets the API layer decide status codes.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from app.core.clock import Clock
from app.core.config import get_settings
from app.core.enums import JobStatus
from app.core.errors import JobNotCancellableError, JobNotFoundError, JobNotRetryableError
from app.core.logging import get_logger, payload_fingerprint
from app.db.models import Job
from app.db.repository import JobFilters, JobRepository, NewJob
from app.dispatch.base import Dispatch, NullDispatch
from app.jobs.registry import get_job_class
from app.services.transitions import record_transition


@dataclass(frozen=True, slots=True)
class SubmitJobCommand:
    job_type: str
    payload: dict[str, Any]
    priority: int | None = None
    max_attempts: int | None = None
    scheduled_at: datetime | None = None
    idempotency_key: str | None = None


class JobService:
    def __init__(
        self,
        repo: JobRepository,
        clock: Clock,
        logger: Any | None = None,
        dispatch: Dispatch | None = None,
    ) -> None:
        self.repo = repo
        self.clock = clock
        self.log = logger or get_logger(__name__)
        self.dispatch = dispatch or NullDispatch()

    async def submit(self, cmd: SubmitJobCommand) -> tuple[Job, bool]:
        """Create a job. Returns ``(job, created)``.

        ``created`` is False when an idempotency key matched an existing job, so
        the API can answer 200 rather than 201 and a client retrying after a
        timeout can tell whether its original request landed.
        """
        job_class = get_job_class(cmd.job_type)
        payload = job_class.parse_payload(cmd.payload)
        # mode="json" so values like dates and URLs are stored as the JSON types
        # they will be read back as, rather than as Python objects.
        normalized = payload.model_dump(mode="json")

        scheduled_at = cmd.scheduled_at
        is_future = scheduled_at is not None and scheduled_at > self.clock.now()

        candidate = NewJob(
            job_type=job_class.job_type,
            payload=normalized,
            status=JobStatus.SCHEDULED if is_future else JobStatus.PENDING,
            priority=(cmd.priority if cmd.priority is not None else job_class.default_priority),
            max_attempts=(
                cmd.max_attempts if cmd.max_attempts is not None else job_class.default_max_attempts
            ),
            scheduled_at=scheduled_at,
            idempotency_key=cmd.idempotency_key,
        )

        job = await self.repo.insert_if_absent(candidate)
        if job is None:
            # The key already exists. It cannot be absent here: the insert only
            # declines on a conflict, and rows are never deleted.
            assert cmd.idempotency_key is not None
            existing = await self.repo.get_by_idempotency_key(cmd.idempotency_key)
            if existing is None:  # pragma: no cover - defensive
                raise JobNotFoundError("Idempotency key conflicted but no job was found")
            await self._on_idempotent_replay(existing, normalized)
            return existing, False

        await self._record(
            job,
            message=f"Job created with status {job.status}",
            event="job.created",
            **payload_fingerprint(normalized),
        )

        # Commit, then announce — never the other way round. Announcing first
        # lets a worker pop the id and query a row that is not yet visible; it
        # discards the hint as stale and the job waits for the fallback poll
        # instead. Intermittent, load-dependent, and invisible afterwards.
        await self.repo.commit()
        if job.status == JobStatus.PENDING:
            await self.dispatch.announce(job.id, job.priority, job.created_at)
        return job, True

    async def get(self, job_id: UUID) -> Job:
        job = await self.repo.get(job_id)
        if job is None:
            raise JobNotFoundError(f"No job with id {job_id}")
        return job

    async def list_jobs(
        self, filters: JobFilters, limit: int, offset: int
    ) -> tuple[list[Job], bool]:
        # Defence in depth: the API rejects an over-large limit with 422, but a
        # non-HTTP caller must not be able to ask for an unbounded page either.
        capped = min(limit, get_settings().max_page_size)
        return await self.repo.list_jobs(filters, capped, offset)

    async def cancel(self, job_id: UUID) -> Job:
        """Cancel a pending or scheduled job.

        The conditional UPDATE decides the outcome. Only when it matches nothing
        do we read the row, and then solely to explain why — a job that does not
        exist and a job in the wrong state are different answers to the caller.
        """
        job = await self.repo.cancel(job_id)
        if job is not None:
            await self._record(job, message="Job cancelled", event="job.cancelled")
            return job

        existing = await self.repo.get(job_id)
        if existing is None:
            raise JobNotFoundError(f"No job with id {job_id}")
        raise JobNotCancellableError(f"Job is {existing.status} and can no longer be cancelled")

    async def retry(self, job_id: UUID) -> Job:
        """Put a failed job back in the queue.

        Same shape as cancel: the conditional UPDATE decides, and the row is
        read afterwards only to explain why nothing matched.
        """
        job = await self.repo.retry_failed(job_id)
        if job is not None:
            await self._record(
                job,
                message="Job requeued by manual retry; attempts reset",
                event="job.retried",
                status=JobStatus.PENDING,
            )
            await self.repo.commit()
            await self.dispatch.announce(job.id, job.priority, job.created_at)
            return job

        existing = await self.repo.get(job_id)
        if existing is None:
            raise JobNotFoundError(f"No job with id {job_id}")
        if existing.dead_letter_reason is not None:
            raise JobNotRetryableError(
                f"Job was dead-lettered ({existing.dead_letter_reason}) and cannot "
                "be retried; the payload or the job itself needs fixing first"
            )
        raise JobNotRetryableError(f"Job is {existing.status}, not failed")

    async def _on_idempotent_replay(self, existing: Job, submitted_payload: dict[str, Any]) -> None:
        if existing.payload == submitted_payload:
            return
        # The assignment specifies returning the existing job for a repeated key,
        # so the differing payload is not an error here. It is almost always a
        # client bug, so it is surfaced rather than swallowed.
        await self._record(
            existing,
            message="Idempotency key reused with a different payload; returning the original job",
            event="job.idempotent_replay_mismatch",
            level="warning",
        )

    async def _record(
        self,
        job: Job,
        *,
        message: str,
        event: str,
        level: str = "info",
        **meta: Any,
    ) -> None:
        await record_transition(
            self.repo, self.log, job, event=event, message=message, level=level, **meta
        )
