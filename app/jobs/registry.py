"""Job type registry.

Adding a job type is one new module plus the ``@register`` decorator. Nothing in
the API, service or persistence layers changes.
"""

import inspect

from app.core.enums import JobType
from app.core.errors import UnknownJobTypeError
from app.jobs.base import BaseJob, JobPayload, JobResult

JOB_REGISTRY: dict[JobType, type[BaseJob]] = {}


def register(cls: type[BaseJob]) -> type[BaseJob]:
    """Register a job class, validating it at import time.

    A misconfigured job type fails when the module is imported — at process
    start — rather than on the first request that happens to use it.
    """
    job_type = getattr(cls, "job_type", None)
    if not isinstance(job_type, JobType):
        raise TypeError(f"{cls.__name__} must set job_type to a JobType member")
    if job_type in JOB_REGISTRY:
        raise TypeError(
            f"{cls.__name__} duplicates job_type {job_type!r}, already registered "
            f"by {JOB_REGISTRY[job_type].__name__}"
        )
    payload_model = getattr(cls, "Payload", None)
    if not (isinstance(payload_model, type) and issubclass(payload_model, JobPayload)):
        raise TypeError(f"{cls.__name__}.Payload must be a JobPayload subclass")
    result_model = getattr(cls, "Result", None)
    if not (isinstance(result_model, type) and issubclass(result_model, JobResult)):
        raise TypeError(f"{cls.__name__}.Result must be a JobResult subclass")
    if inspect.isabstract(cls):
        raise TypeError(f"{cls.__name__} does not implement run()")

    JOB_REGISTRY[job_type] = cls
    return cls


def get_job_class(job_type: str) -> type[BaseJob]:
    """Look up the handler for a job type, or raise ``UnknownJobTypeError``."""
    try:
        key = JobType(job_type)
    except ValueError:
        raise UnknownJobTypeError(f"Unknown job type: {job_type!r}") from None
    try:
        return JOB_REGISTRY[key]
    except KeyError:
        raise UnknownJobTypeError(f"No handler registered for job type: {job_type!r}") from None
