"""Request and response models for the HTTP layer."""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.core.config import get_settings
from app.core.enums import MAX_PRIORITY, MIN_PRIORITY, JobType

_IDEMPOTENCY_KEY_PATTERN = r"^[A-Za-z0-9_.:-]{1,255}$"


class SubmitJobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # Typed as str rather than JobType so that an unrecognised type is answered
    # by the registry with `unknown_job_type` instead of a generic schema error.
    # The registry is the single authority on which types exist.
    job_type: str = Field(
        description=f"One of: {', '.join(t.value for t in JobType)}",
        examples=["email"],
    )
    payload: dict[str, Any]
    priority: int | None = Field(default=None, ge=MIN_PRIORITY, le=MAX_PRIORITY)
    max_attempts: int | None = Field(default=None, ge=1, le=10)
    scheduled_at: datetime | None = None
    idempotency_key: str | None = Field(default=None, pattern=_IDEMPOTENCY_KEY_PATTERN)

    @field_validator("scheduled_at")
    @classmethod
    def _must_be_tz_aware_and_near(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return value
        if value.tzinfo is None or value.utcoffset() is None:
            # A naive datetime would be interpreted in whatever timezone the
            # server happens to run in — a scheduling bug that only appears in
            # production.
            raise ValueError("scheduled_at must include a timezone offset")
        horizon = timedelta(days=get_settings().max_schedule_horizon_days)
        if value > datetime.now(UTC) + horizon:
            raise ValueError(f"scheduled_at must be within {horizon.days} days")
        return value


class JobResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    job_type: str
    status: str
    priority: int
    attempts: int
    max_attempts: int
    progress: int
    payload: dict[str, Any]
    result: dict[str, Any] | None
    error: dict[str, Any] | None
    #: Set only when the failure means the job cannot run at all. A job that
    #: simply failed its attempts has None here and can be retried.
    dead_letter_reason: str | None
    idempotency_key: str | None
    scheduled_at: datetime | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None


class JobListResponse(BaseModel):
    items: list[JobResponse]
    limit: int
    offset: int
    # No `total`: an exact count requires a full scan of the filtered set on
    # every request. See specs/02-api-core.md section 3.
    has_more: bool


class QueueDepth(BaseModel):
    scheduled: int
    pending: int
    processing: int
    completed: int
    failed: int
    cancelled: int
    #: Depth alone cannot separate load from failure; depth with age can. High
    #: depth and low age is a busy system keeping up, low depth and high age is
    #: a stuck one. None when nothing is waiting.
    oldest_pending_seconds: float | None = None
    #: Size of the Redis dispatch set. Should track `pending`; a large gap means
    #: announcements are failing and everything is arriving via the fallback.
    #: None when Redis cannot answer.
    ready_hints: int | None = None
    #: Failures that cannot be retried. Should be zero; worth alerting on.
    dead_lettered: int = 0


class WorkerStatus(BaseModel):
    count: int
    ids: list[str]


class HealthResponse(BaseModel):
    status: str
    version: str
    uptime_seconds: int
    database: str
    redis: str
    queue: QueueDepth
    #: None means Redis could not be asked — deliberately not `count: 0`. "I
    #: cannot see the workers" and "there are no workers" are different
    #: incidents, and reporting the second when the first is true sends an
    #: operator to restart healthy workers.
    workers: WorkerStatus | None = None


class ErrorBody(BaseModel):
    code: str
    message: str
    request_id: str | None = None
    details: list[dict[str, Any]] = Field(default_factory=list)


class ErrorResponse(BaseModel):
    error: ErrorBody
