"""Health, queue statistics and worker liveness.

Designed around one question: how does an operator diagnose a stuck or slow
queue at 3am, without reading the code. See specs/08-worker-observability.md.
"""

import time

from fastapi import APIRouter, Request, Response, status

from app.api.deps import DispatchDep, SessionDep
from app.api.schemas import HealthResponse, QueueDepth, WorkerStatus
from app.core.logging import get_logger
from app.db.repository import JobRepository

router = APIRouter(tags=["health"])
log = get_logger(__name__)

_EMPTY_QUEUE = QueueDepth(scheduled=0, pending=0, processing=0, completed=0, failed=0, cancelled=0)


@router.get("/health", response_model=HealthResponse, summary="Health and queue depth")
async def health(
    request: Request, response: Response, session: SessionDep, dispatch: DispatchDep
) -> HealthResponse:
    uptime = int(time.monotonic() - request.app.state.started_at)
    version = request.app.version

    # Redis is asked first and never fails the check: the system processes jobs
    # without it, so a degraded cache must not pull a working service out of a
    # load balancer.
    worker_ids = await dispatch.active_workers()
    ready_hints = await dispatch.ready_depth()
    redis_state = "ok" if worker_ids is not None else "error"
    workers = (
        WorkerStatus(count=len(worker_ids), ids=worker_ids) if worker_ids is not None else None
    )

    try:
        repo = JobRepository(session)
        counts = await repo.count_by_status()
        oldest_pending = await repo.oldest_pending_age_seconds()
        dead_lettered = await repo.count_dead_lettered()
    except Exception:
        # Reported rather than raised: a health endpoint that 500s tells a load
        # balancer less than one that says which dependency is down.
        log.exception("health.database_unavailable")
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return HealthResponse(
            status="degraded",
            version=version,
            uptime_seconds=uptime,
            database="error",
            redis=redis_state,
            queue=_EMPTY_QUEUE,
            workers=workers,
        )

    return HealthResponse(
        status="ok",
        version=version,
        uptime_seconds=uptime,
        database="ok",
        redis=redis_state,
        queue=QueueDepth(
            **counts,
            oldest_pending_seconds=oldest_pending,
            ready_hints=ready_hints,
            dead_lettered=dead_lettered,
        ),
        workers=workers,
    )
