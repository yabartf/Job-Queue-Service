"""Domain errors.

These carry a stable machine-readable ``code`` but no HTTP status: mapping to a
response is the API layer's job (app/api/errors.py), so the service layer stays
free of HTTP concepts.
"""

from typing import Any, ClassVar


class DomainError(Exception):
    """Base for errors that are part of the domain contract, not bugs."""

    code: ClassVar[str] = "domain_error"

    def __init__(self, message: str, details: list[dict[str, Any]] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or []


class JobNotFoundError(DomainError):
    code = "job_not_found"


class JobNotCancellableError(DomainError):
    """The job exists but is not in a status that can be cancelled."""

    code = "job_not_cancellable"


class JobNotRetryableError(DomainError):
    """The job exists but cannot be put back in the queue.

    Either it is not in a failed state, or it was dead-lettered — retrying a job
    that cannot run would only take another worker down with it.
    """

    code = "job_not_retryable"


class UnknownJobTypeError(DomainError):
    code = "unknown_job_type"


class PayloadValidationError(DomainError):
    code = "payload_invalid"


class PayloadTooLargeError(DomainError):
    code = "payload_too_large"
