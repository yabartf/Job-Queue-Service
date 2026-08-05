"""Async engine and session management."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import get_settings


def create_engine(url: str | None = None) -> AsyncEngine:
    settings = get_settings()
    return create_async_engine(
        url or settings.database_url,
        pool_pre_ping=True,
        # expire_on_commit is disabled on the sessionmaker below rather than
        # here; see make_session_factory.
    )


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    # expire_on_commit=False: with async sessions, touching an expired attribute
    # after commit triggers a lazy refresh that cannot run outside a greenlet
    # context and raises. Keeping objects usable after commit avoids that whole
    # class of error.
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


@asynccontextmanager
async def transaction(
    factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """One session per unit of work, committed on success, rolled back on error.

    The worker opens one of these per operation rather than holding a session
    for the length of a job: a handler can run for minutes, and a checked-out
    connection idling through it is a connection the pool cannot reuse.
    """
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def session_scope(
    factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """The same unit of work as a generator, which is the shape FastAPI's
    dependency system needs. One implementation, two shapes."""
    async with transaction(factory) as session:
        yield session
