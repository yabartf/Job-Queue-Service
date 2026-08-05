"""Recording a job state transition.

One helper, used by both the API and the worker, so the ``job_logs`` audit trail
and the structured log stream cannot drift apart. Two call sites would
eventually disagree about what a transition looks like; one cannot.
"""

from typing import Any
from uuid import UUID

from app.db.models import Job
from app.db.repository import JobRepository


async def record_event(
    repo: JobRepository,
    logger: Any,
    job_id: UUID,
    *,
    event: str,
    message: str,
    level: str = "info",
    **fields: Any,
) -> None:
    """Write the audit row and emit the log line together.

    Used directly when only an id is available — the maintenance sweeps touch
    jobs in batches and never load the rows.
    """
    await repo.add_log(job_id, level=level, message=message, meta=fields)
    getattr(logger, level)(event, job_id=str(job_id), **fields)


async def record_transition(
    repo: JobRepository,
    logger: Any,
    job: Job,
    *,
    event: str,
    message: str,
    level: str = "info",
    **fields: Any,
) -> None:
    """Record a transition, taking the job's context from the row itself.

    Explicit ``fields`` win over the derived ones, which is how a caller reports
    the status a write just moved the job *to* — the in-memory row still holds
    the status it moved *from*.
    """
    context: dict[str, Any] = {
        "job_type": job.job_type,
        "status": job.status,
        "attempt": job.attempts,
    }
    if job.worker_id is not None:
        context["worker_id"] = job.worker_id

    await record_event(
        repo,
        logger,
        job.id,
        event=event,
        message=message,
        level=level,
        **(context | fields),
    )
