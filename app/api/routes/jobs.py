"""Job endpoints."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query, Response, status

from app.api.deps import JobServiceDep
from app.api.schemas import JobListResponse, JobResponse, SubmitJobRequest
from app.core.config import get_settings
from app.core.enums import JobStatus, JobType
from app.db.repository import JobFilters
from app.services.job_service import SubmitJobCommand

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post(
    "",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Submit a job",
    responses={200: {"description": "Existing job returned for a repeated idempotency key"}},
)
async def submit_job(
    body: SubmitJobRequest, response: Response, service: JobServiceDep
) -> JobResponse:
    job, created = await service.submit(
        SubmitJobCommand(
            job_type=body.job_type,
            payload=body.payload,
            priority=body.priority,
            max_attempts=body.max_attempts,
            scheduled_at=body.scheduled_at,
            idempotency_key=body.idempotency_key,
        )
    )
    # 200 rather than 201 on an idempotent replay: nothing was created, and a
    # client retrying after a timeout can tell whether its first attempt landed.
    response.status_code = status.HTTP_201_CREATED if created else status.HTTP_200_OK
    if created:
        response.headers["Location"] = f"/jobs/{job.id}"
    return JobResponse.model_validate(job)


@router.get("/{job_id}", response_model=JobResponse, summary="Get a job")
async def get_job(job_id: UUID, service: JobServiceDep) -> JobResponse:
    return JobResponse.model_validate(await service.get(job_id))


@router.get("", response_model=JobListResponse, summary="List jobs")
async def list_jobs(
    service: JobServiceDep,
    status_filter: Annotated[JobStatus | None, Query(alias="status")] = None,
    job_type: Annotated[JobType | None, Query()] = None,
    dead_lettered: Annotated[bool | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=get_settings().max_page_size)] = (
        get_settings().default_page_size
    ),
    offset: Annotated[int, Query(ge=0)] = 0,
) -> JobListResponse:
    jobs, has_more = await service.list_jobs(
        JobFilters(status=status_filter, job_type=job_type, dead_lettered=dead_lettered),
        limit,
        offset,
    )
    return JobListResponse(
        items=[JobResponse.model_validate(job) for job in jobs],
        limit=limit,
        offset=offset,
        has_more=has_more,
    )


@router.post("/{job_id}/cancel", response_model=JobResponse, summary="Cancel a job")
async def cancel_job(job_id: UUID, service: JobServiceDep) -> JobResponse:
    return JobResponse.model_validate(await service.cancel(job_id))


@router.post(
    "/{job_id}/retry",
    response_model=JobResponse,
    summary="Retry a failed job",
    responses={409: {"description": "Not failed, or dead-lettered"}},
)
async def retry_job(job_id: UUID, service: JobServiceDep) -> JobResponse:
    return JobResponse.model_validate(await service.retry(job_id))
