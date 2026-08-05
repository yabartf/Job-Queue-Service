"""The job type abstraction.

Two hierarchies exist in this system and they are deliberately separate: the
``jobs`` table is flat (one row shape for every type), while behaviour is
polymorphic through ``BaseJob``. The table describes *a job*; the class
describes *what the job does*. See specs/01-data-model.md section 2.
"""

import random
from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any, ClassVar, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, ValidationError

from app.core.clock import RealSleeper, Sleeper
from app.core.enums import JobType
from app.core.errors import PayloadValidationError


class JobPayload(BaseModel):
    """Base for every job's input schema.

    ``extra="forbid"`` lives here rather than on each subclass so a new job type
    cannot forget it. Silently accepting unknown fields would let a client
    believe it configured something it did not.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class JobResult(BaseModel):
    """Base for every job's output schema."""

    model_config = ConfigDict(extra="forbid")


class JobExecutionError(Exception):
    """A handler failed in a way that may be worth retrying.

    Messages must not embed payload contents: they are persisted on the job row
    and returned through the API.
    """


class JobTimeoutError(JobExecutionError):
    """A handler exceeded the time budget its job type declares.

    Retryable — a job that ran long once may be fine next time. A job that
    exceeds its budget on *every* attempt is a different matter, and is
    dead-lettered when its attempts run out (spec 09 section 4).
    """


class JobContext(Protocol):
    """Everything a handler is allowed to touch.

    Deliberately no session, no repository, no ORM object — a handler cannot
    reach the database, so it cannot become coupled to it, so it can be tested
    by passing a fake that records calls.
    """

    # Read-only: a handler is told which job it is running, never asked to
    # decide. Declaring these as properties rather than plain attributes is what
    # lets an implementation derive them instead of storing them.
    @property
    def job_id(self) -> UUID: ...

    @property
    def attempt(self) -> int: ...

    async def report_progress(self, pct: int) -> None: ...

    async def log(self, level: str, message: str, **fields: Any) -> None: ...

    async def heartbeat(self) -> None: ...


class BaseJob(ABC):
    """One subclass per job type: its schemas, its defaults, and what it does."""

    job_type: ClassVar[JobType]
    Payload: ClassVar[type[JobPayload]]
    Result: ClassVar[type[JobResult]]

    default_priority: ClassVar[int] = 5
    default_max_attempts: ClassVar[int] = 3
    timeout_seconds: ClassVar[int] = 60

    def __init__(
        self,
        payload: JobPayload,
        ctx: JobContext,
        *,
        sleeper: Sleeper | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self.payload = payload
        self.ctx = ctx
        # Sleeping and randomness are injected so handler behaviour is testable
        # without the suite waiting in real time or depending on chance. Every
        # subclass is constructed identically, so the worker never needs to know
        # which type it is building.
        self._sleeper = sleeper or RealSleeper()
        self._rng = rng or random.Random()

    @classmethod
    def parse_payload(cls, raw: Mapping[str, Any]) -> JobPayload:
        """Validate a raw payload against this job type's schema."""
        try:
            return cls.Payload.model_validate(raw)
        except ValidationError as exc:
            raise PayloadValidationError(
                "Payload failed validation", details=_field_errors(exc)
            ) from exc

    @abstractmethod
    async def run(self) -> JobResult:
        """Execute the job and return its result."""

    async def _sleep_between(self, low: float, high: float) -> None:
        await self._sleeper.sleep(self._rng.uniform(low, high))


def _field_errors(exc: ValidationError) -> list[dict[str, Any]]:
    """Flatten pydantic errors into the API's ``details`` shape."""
    return [
        {
            "field": ".".join(["payload", *(str(part) for part in err["loc"])]),
            "message": err["msg"],
        }
        for err in exc.errors()
    ]
