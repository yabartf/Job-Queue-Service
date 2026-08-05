"""ORM models.

One flat ``jobs`` table for every job type — see specs/01-data-model.md section 2
for why there is no ORM inheritance here.

Statuses and types are stored as text with CHECK constraints rather than native
PostgreSQL enums: altering a native enum inside a migration is awkward and
partially non-transactional, while a CHECK gives the same guarantee and edits
like any other constraint. ``JobStatus``/``JobType`` are ``StrEnum``, so enum
members can be assigned to these columns directly.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    SmallInteger,
    Text,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    job_type: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)

    priority: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("5"))
    attempts: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("0"))
    max_attempts: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, server_default=text("3")
    )
    progress: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("0"))

    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    error: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    # Why a failure is un-runnable rather than merely failed. A classification,
    # not a status: `failed` stays the single terminal failure state, so the
    # state machine and every status filter are untouched by this.
    dead_letter_reason: Mapped[str | None] = mapped_column(Text)
    idempotency_key: Mapped[str | None] = mapped_column(Text)

    scheduled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # Written by the worker from spec 10 onward. Created here so the schema is
    # migrated once rather than altered under code that already depends on it.
    worker_id: Mapped[str | None] = mapped_column(Text)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('scheduled','pending','processing','completed','failed','cancelled')",
            name="ck_jobs_status",
        ),
        CheckConstraint("job_type IN ('email','webhook','report','batch')", name="ck_jobs_type"),
        CheckConstraint("priority BETWEEN 0 AND 9", name="ck_jobs_priority"),
        CheckConstraint("max_attempts BETWEEN 1 AND 10", name="ck_jobs_max_attempts"),
        CheckConstraint("attempts >= 0 AND attempts <= max_attempts", name="ck_jobs_attempts"),
        CheckConstraint("progress BETWEEN 0 AND 100", name="ck_jobs_progress"),
        CheckConstraint(
            "idempotency_key IS NULL OR char_length(idempotency_key) <= 255",
            name="ck_jobs_idem_len",
        ),
        # State invariants, enforced by the database so that a bug in claim,
        # retry or recovery logic raises at the bad write instead of quietly
        # producing a row that cannot be interpreted.
        CheckConstraint(
            "result IS NULL OR status = 'completed'",
            name="ck_jobs_result_when_completed",
        ),
        CheckConstraint(
            "status <> 'scheduled' OR scheduled_at IS NOT NULL",
            name="ck_jobs_scheduled_has_time",
        ),
        CheckConstraint(
            "status <> 'processing' OR (worker_id IS NOT NULL AND lease_until IS NOT NULL)",
            name="ck_jobs_processing_has_lease",
        ),
        CheckConstraint(
            "completed_at IS NULL OR started_at IS NOT NULL",
            name="ck_jobs_completed_after_started",
        ),
        CheckConstraint(
            "dead_letter_reason IS NULL OR status = 'failed'",
            name="ck_jobs_dead_letter_is_failed",
        ),
        CheckConstraint(
            "dead_letter_reason IS NULL OR dead_letter_reason IN "
            "('unprocessable_payload','timeout_loop','worker_crash_loop')",
            name="ck_jobs_dead_letter_reason",
        ),
        # Partial so it does not collide on the many jobs submitted without a key.
        Index(
            "ux_jobs_idempotency",
            "idempotency_key",
            unique=True,
            postgresql_where=text("idempotency_key IS NOT NULL"),
        ),
        # Claim path (spec 10). Partial: terminal rows are the majority over
        # time, and excluding them keeps the hot index small enough to stay cached.
        Index(
            "ix_jobs_claim",
            text("priority DESC"),
            "created_at",
            postgresql_where=text("status = 'pending'"),
        ),
        Index(
            "ix_jobs_scheduled",
            "scheduled_at",
            postgresql_where=text("status = 'scheduled'"),
        ),
        Index(
            "ix_jobs_lease",
            "lease_until",
            postgresql_where=text("status = 'processing'"),
        ),
        Index("ix_jobs_status_created", "status", text("created_at DESC")),
        Index("ix_jobs_created", text("created_at DESC")),
        # No index on job_type: four distinct values is too low-selectivity for
        # the planner to prefer over a scan, so it would cost write throughput
        # on the claim path and buy nothing.
    )


class JobLog(Base):
    """Append-only audit trail: one row per state transition.

    This is what an operator reads when asked why a specific job is in the state
    it is in.
    """

    __tablename__ = "job_logs"

    # Never addressed externally, so a sequential key is preferred here for
    # insert locality — unlike jobs.id, which is client-visible.
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    level: Mapped[str] = mapped_column(Text, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    # Named `meta`, not `metadata`: `metadata` is reserved on declarative classes.
    meta: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint("level IN ('info','warning','error')", name="ck_job_logs_level"),
        Index("ix_job_logs_job", "job_id", "created_at"),
    )
