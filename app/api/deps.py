"""Dependency wiring.

Construction happens here so that the service layer takes plain constructor
arguments and can be built directly in tests without FastAPI involved.
"""

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clock import Clock, SystemClock
from app.db.repository import JobRepository
from app.db.session import session_scope
from app.dispatch.base import Dispatch
from app.services.job_service import JobService


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async for session in session_scope(request.app.state.session_factory):
        yield session


def get_clock() -> Clock:
    return SystemClock()


def get_dispatch(request: Request) -> Dispatch:
    return request.app.state.dispatch  # type: ignore[no-any-return]


async def get_job_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    clock: Annotated[Clock, Depends(get_clock)],
    dispatch: Annotated[Dispatch, Depends(get_dispatch)],
) -> JobService:
    return JobService(JobRepository(session), clock, dispatch=dispatch)


JobServiceDep = Annotated[JobService, Depends(get_job_service)]
SessionDep = Annotated[AsyncSession, Depends(get_session)]
DispatchDep = Annotated[Dispatch, Depends(get_dispatch)]
