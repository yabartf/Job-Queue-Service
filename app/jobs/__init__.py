"""Job type definitions.

Importing this package registers every job type. The imports below exist for
their registration side effect, which is why they are re-exported rather than
removed as unused.
"""

from app.jobs.base import (
    BaseJob,
    JobContext,
    JobExecutionError,
    JobPayload,
    JobResult,
)
from app.jobs.batch_job import BatchJob
from app.jobs.email_job import EmailJob
from app.jobs.registry import JOB_REGISTRY, get_job_class, register
from app.jobs.report_job import ReportJob
from app.jobs.webhook_job import WebhookJob

__all__ = [
    "JOB_REGISTRY",
    "BaseJob",
    "BatchJob",
    "EmailJob",
    "JobContext",
    "JobExecutionError",
    "JobPayload",
    "JobResult",
    "ReportJob",
    "WebhookJob",
    "get_job_class",
    "register",
]
