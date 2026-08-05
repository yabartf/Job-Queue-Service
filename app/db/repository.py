"""Every SQL statement in the system lives here.

Layers above this one never construct SQL. All statements are built from
SQLAlchemy constructs with bound parameters — there is no string interpolation
into SQL anywhere in the codebase, which is what makes injection structurally
impossible rather than merely filtered against.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import ColumnElement, extract, func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import CANCELLABLE_STATUSES, JobStatus
from app.db.models import Job, JobLog


@dataclass(frozen=True, slots=True)
class NewJob:
    job_type: str
    payload: dict[str, Any]
    status: JobStatus
    priority: int
    max_attempts: int
    scheduled_at: datetime | None
    idempotency_key: str | None


@dataclass(frozen=True, slots=True)
class JobFilters:
    status: JobStatus | None = None
    job_type: str | None = None
    #: None applies no filter; True and False select and exclude dead letters.
    dead_lettered: bool | None = None


@dataclass(frozen=True, slots=True)
class Ownership:
    """Proof that a worker still holds a job.

    ``attempts`` is the fencing token. It increments on every claim, so a value
    read at claim time can never describe a later claim of the same job — which
    is what stops a worker displaced by the reaper from overwriting the result
    of whoever took over. ``worker_id`` alone is not sufficient, because slots
    inside one process could otherwise share it. See specs/04-claiming.md §5.
    """

    job_id: UUID
    worker_id: str
    attempts: int

    @classmethod
    def of(cls, job: Job) -> "Ownership":
        if job.worker_id is None:  # pragma: no cover - claim always sets it
            raise ValueError(f"Job {job.id} has no worker_id to take ownership of")
        return cls(job_id=job.id, worker_id=job.worker_id, attempts=job.attempts)


@dataclass(frozen=True, slots=True)
class ReapResult:
    """Outcome of one reaper sweep."""

    released: list[UUID]
    failed: list[UUID]


class JobRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def insert_if_absent(self, new: NewJob) -> Job | None:
        """Insert a job, or return ``None`` if its idempotency key already exists.

        Correct under concurrent submission by construction: of two simultaneous
        requests with the same key, one inserts and the other blocks on the
        unique index until the first commits, then returns no row. There is no
        check-then-write window in application code, so there is no race.

        ``index_where`` is required because ux_jobs_idempotency is a partial
        index — without it PostgreSQL cannot infer which index to arbitrate on.
        """
        stmt = (
            pg_insert(Job)
            .values(
                job_type=new.job_type,
                payload=new.payload,
                status=new.status,
                priority=new.priority,
                max_attempts=new.max_attempts,
                scheduled_at=new.scheduled_at,
                idempotency_key=new.idempotency_key,
            )
            .on_conflict_do_nothing(
                index_elements=["idempotency_key"],
                index_where=text("idempotency_key IS NOT NULL"),
            )
            .returning(Job)
        )
        result = await self.session.execute(stmt)
        return result.scalars().first()

    async def get(self, job_id: UUID) -> Job | None:
        return await self.session.get(Job, job_id)

    async def get_by_idempotency_key(self, key: str) -> Job | None:
        stmt = select(Job).where(Job.idempotency_key == key)
        return (await self.session.execute(stmt)).scalars().first()

    async def list_jobs(
        self, filters: JobFilters, limit: int, offset: int
    ) -> tuple[list[Job], bool]:
        """Return a page of jobs and whether another page follows.

        ``has_more`` comes from fetching one extra row rather than a COUNT(*)
        over the filtered set, which would degrade linearly as the table grows —
        on the endpoint an operator is most likely to refresh repeatedly.
        """
        stmt = select(Job).order_by(Job.created_at.desc(), Job.id.desc())
        if filters.status is not None:
            stmt = stmt.where(Job.status == filters.status)
        if filters.job_type is not None:
            stmt = stmt.where(Job.job_type == filters.job_type)
        if filters.dead_lettered is not None:
            stmt = stmt.where(
                Job.dead_letter_reason.is_not(None)
                if filters.dead_lettered
                else Job.dead_letter_reason.is_(None)
            )

        rows = list(
            (await self.session.execute(stmt.offset(offset).limit(limit + 1))).scalars().all()
        )
        return rows[:limit], len(rows) > limit

    async def cancel(self, job_id: UUID) -> Job | None:
        """Cancel a job, returning it, or ``None`` if it was not cancellable.

        The conditional UPDATE is the authority: naming the expected statuses in
        the WHERE clause means a cancellation racing the worker's claim is
        arbitrated by the row lock rather than by application timing. A caller
        that gets ``None`` must read the row separately to learn whether the job
        was missing or merely in the wrong state — reading first and then
        updating would reintroduce the race this shape exists to avoid.
        """
        stmt = (
            update(Job)
            .where(Job.id == job_id, Job.status.in_(sorted(CANCELLABLE_STATUSES)))
            .values(status=JobStatus.CANCELLED)
            .returning(Job)
        )
        result = await self.session.execute(stmt)
        return result.scalars().first()

    async def count_by_status(self) -> dict[str, int]:
        """Queue depth per status, as one grouped aggregate."""
        stmt = select(Job.status, func.count()).group_by(Job.status)
        rows = (await self.session.execute(stmt)).all()
        counts = {status.value: 0 for status in JobStatus}
        counts.update({str(status): int(count) for status, count in rows})
        return counts

    async def count_dead_lettered(self) -> int:
        """How many failures are un-runnable rather than merely failed.

        A number that should be zero, and is worth alerting on when it is not.
        """
        stmt = select(func.count()).select_from(Job).where(Job.dead_letter_reason.is_not(None))
        return int((await self.session.execute(stmt)).scalar_one())

    async def add_log(
        self,
        job_id: UUID,
        level: str,
        message: str,
        meta: dict[str, Any] | None = None,
    ) -> None:
        self.session.add(JobLog(job_id=job_id, level=level, message=message, meta=meta or {}))

    async def commit(self) -> None:
        """Make the current unit of work durable.

        Exposed because submission must commit *before* the job is announced to
        Redis: announcing first lets a worker pop the id and query a row that is
        not yet visible (specs/07-redis-dispatch.md section 5).
        """
        await self.session.commit()

    # ------------------------------------------------------------------
    # Worker statements — specs/04-claiming.md and specs/06-crash-recovery.md
    #
    # Every time value below is computed by the database with func.now(), never
    # by the application. Leases are written and compared against one clock, so
    # skew between application hosts cannot make the reaper act early or late.
    # ------------------------------------------------------------------

    def _eligible(self) -> tuple[ColumnElement[bool], ...]:
        """A job that may be claimed right now."""
        return (
            Job.status == JobStatus.PENDING,
            or_(Job.scheduled_at.is_(None), Job.scheduled_at <= func.now()),
        )

    def _claim_values(self, worker_id: str, lease_seconds: int) -> dict[str, Any]:
        """The SET clause both claim paths share.

        ``started_at`` records when the *current attempt* started, which is the
        useful value when diagnosing a job that is taking too long; the original
        submission time is already in ``created_at``.
        """
        return {
            "status": JobStatus.PROCESSING,
            "worker_id": worker_id,
            "lease_until": func.now() + timedelta(seconds=lease_seconds),
            "started_at": func.now(),
            "attempts": Job.attempts + 1,
        }

    def _owned(self, own: Ownership) -> tuple[ColumnElement[bool], ...]:
        """The predicate every post-claim write repeats.

        ``attempts`` is the fencing token — see the Ownership docstring.
        """
        return (
            Job.id == own.job_id,
            Job.worker_id == own.worker_id,
            Job.attempts == own.attempts,
            Job.status == JobStatus.PROCESSING,
        )

    async def _update_owned(self, own: Ownership, **values: Any) -> bool:
        """Apply a write only if the caller still owns the job.

        Returns False when ownership was lost — not an error, and never a reason
        to retry the write. The caller discards whatever it was about to record.

        Ownership is decided by whether a row came back, not by ``rowcount``:
        RETURNING is what the rest of this module uses, and it says exactly what
        was touched rather than how many rows a driver counted.
        """
        stmt = update(Job).where(*self._owned(own)).values(**values).returning(Job.id)
        return (await self.session.execute(stmt)).scalars().first() is not None

    async def _sweep(
        self,
        *,
        where: tuple[ColumnElement[bool], ...],
        order_by: Any,
        limit: int,
        **values: Any,
    ) -> list[UUID]:
        """Update a bounded batch of rows and return what was touched.

        ``SKIP LOCKED`` means several worker processes can sweep at the same
        time: each takes a disjoint set instead of contending, which is what
        removes the need for a leader among them.
        """
        candidates = (
            select(Job.id)
            .where(*where)
            .order_by(order_by)
            .limit(limit)
            .with_for_update(skip_locked=True)
            .scalar_subquery()
        )
        stmt = update(Job).where(Job.id.in_(candidates)).values(**values).returning(Job.id)
        return list((await self.session.execute(stmt)).scalars().all())

    async def claim_next(self, worker_id: str, lease_seconds: int) -> Job | None:
        """Claim the highest-priority eligible job, or return None.

        ``FOR UPDATE SKIP LOCKED`` makes a worker step over a row another worker
        has locked rather than queue behind it, so N workers claim N distinct
        jobs. Two workers cannot hold the same job because the row lock is held
        for the whole statement and whoever arrives second finds a row whose
        status no longer matches — a property of the engine, not of the order in
        which our code happens to run.
        """
        candidate = (
            select(Job.id)
            .where(*self._eligible())
            .order_by(Job.priority.desc(), Job.created_at.asc())
            .limit(1)
            .with_for_update(skip_locked=True)
            .scalar_subquery()
        )
        stmt = (
            update(Job)
            .where(Job.id == candidate)
            .values(**self._claim_values(worker_id, lease_seconds))
            .returning(Job)
        )
        return (await self.session.execute(stmt)).scalars().first()

    async def claim_by_id(self, job_id: UUID, worker_id: str, lease_seconds: int) -> Job | None:
        """Claim one specific job, for the Redis hint path.

        Returning None means the hint was stale — cancelled, already claimed, or
        not yet due. That is expected steady-state behaviour, not an error: it is
        how cancelled entries are cleaned out of the dispatch set.
        """
        stmt = (
            update(Job)
            .where(Job.id == job_id, *self._eligible())
            .values(**self._claim_values(worker_id, lease_seconds))
            .returning(Job)
        )
        return (await self.session.execute(stmt)).scalars().first()

    async def extend_lease(self, own: Ownership, lease_seconds: int) -> bool:
        return await self._update_owned(
            own, lease_until=func.now() + timedelta(seconds=lease_seconds)
        )

    async def set_progress(self, own: Ownership, percent: int) -> bool:
        return await self._update_owned(own, progress=percent)

    async def mark_completed(self, own: Ownership, result: dict[str, Any]) -> bool:
        # status and result must move in one statement: ck_jobs_result_when_completed
        # forbids a result on any other status, so writing them separately fails.
        return await self._update_owned(
            own,
            status=JobStatus.COMPLETED,
            result=result,
            completed_at=func.now(),
            worker_id=None,
            lease_until=None,
        )

    async def mark_failed(
        self,
        own: Ownership,
        error: dict[str, Any],
        dead_letter_reason: str | None = None,
    ) -> bool:
        return await self._update_owned(
            own,
            status=JobStatus.FAILED,
            error=error,
            dead_letter_reason=dead_letter_reason,
            completed_at=func.now(),
            worker_id=None,
            lease_until=None,
        )

    async def retry_failed(self, job_id: UUID) -> Job | None:
        """Put a failed job back in the queue, or return None if it cannot be.

        ``attempts`` resets to 0: without it the job would be claimed once, find
        its attempts already spent, and fail again immediately.

        The previous error, progress and timestamps are cleared because the job
        is starting a fresh life — its old one is in ``job_logs``, where history
        belongs and where it will not be mistaken for the current state.
        ``started_at`` and ``completed_at`` clear together so that
        ck_jobs_completed_after_started cannot be violated.

        Dead-lettered jobs are excluded. An operator draining an incident by
        retrying everything that failed must not re-arm a job that takes a
        worker down with it.
        """
        stmt = (
            update(Job)
            .where(
                Job.id == job_id,
                Job.status == JobStatus.FAILED,
                Job.dead_letter_reason.is_(None),
            )
            .values(
                status=JobStatus.PENDING,
                attempts=0,
                error=None,
                scheduled_at=None,
                started_at=None,
                completed_at=None,
                worker_id=None,
                lease_until=None,
                progress=0,
            )
            .returning(Job)
        )
        return (await self.session.execute(stmt)).scalars().first()

    async def reschedule_after_failure(
        self, own: Ownership, error: dict[str, Any], delay_seconds: float
    ) -> bool:
        """Return a failed job to the queue, held back for ``delay_seconds``.

        The job becomes ``pending`` rather than ``scheduled``: it is mid-lifecycle
        and already queued, and the claim predicate is what holds it back.

        The delay is a duration rather than an instant on purpose. The claim
        predicate compares ``scheduled_at`` against the database's ``now()``, so
        the deadline is computed there too — an instant calculated from an
        application clock would fire early or late by exactly the skew between
        the two machines.
        """
        return await self._update_owned(
            own,
            status=JobStatus.PENDING,
            error=error,
            scheduled_at=func.now() + timedelta(seconds=delay_seconds),
            worker_id=None,
            lease_until=None,
        )

    async def release_lease(self, own: Ownership) -> bool:
        """Hand a job back without recording an outcome, on forced shutdown."""
        return await self._update_owned(
            own, status=JobStatus.PENDING, worker_id=None, lease_until=None
        )

    async def reap_expired_leases(
        self,
        limit: int,
        expiry_error: dict[str, Any],
        exhausted_reason: str | None = None,
    ) -> ReapResult:
        """Recover jobs whose holder stopped extending the lease.

        Two statements, because a job that expires with no attempts left cannot
        go back to the queue: the next claim would compute attempts + 1 beyond
        max_attempts and violate ck_jobs_attempts. The predicates are disjoint,
        so the order between them does not matter.
        """
        expired = (
            Job.status == JobStatus.PROCESSING,
            Job.lease_until < func.now(),
        )
        exhausted = await self._sweep(
            where=(*expired, Job.attempts >= Job.max_attempts),
            order_by=Job.lease_until,
            limit=limit,
            status=JobStatus.FAILED,
            error=expiry_error,
            dead_letter_reason=exhausted_reason,
            completed_at=func.now(),
            worker_id=None,
            lease_until=None,
        )
        released = await self._sweep(
            where=(*expired, Job.attempts < Job.max_attempts),
            order_by=Job.lease_until,
            limit=limit,
            status=JobStatus.PENDING,
            worker_id=None,
            lease_until=None,
        )
        return ReapResult(released=released, failed=exhausted)

    async def promote_due_scheduled(self, limit: int) -> list[UUID]:
        """Move scheduled jobs into the queue once their time has arrived."""
        return await self._sweep(
            where=(
                Job.status == JobStatus.SCHEDULED,
                Job.scheduled_at <= func.now(),
            ),
            order_by=Job.scheduled_at,
            limit=limit,
            status=JobStatus.PENDING,
        )

    async def oldest_pending_age_seconds(self) -> float | None:
        """Age of the oldest waiting job, or None when nothing is waiting.

        Depth alone cannot separate load from failure; depth with age can. High
        depth and low age is a busy system keeping up, low depth and high age is
        a stuck one.
        """
        stmt = select(extract("epoch", func.now() - func.min(Job.created_at))).where(
            Job.status == JobStatus.PENDING
        )
        value = (await self.session.execute(stmt)).scalar_one()
        return float(value) if value is not None else None
