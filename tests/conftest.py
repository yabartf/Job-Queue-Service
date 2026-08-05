"""Shared fixtures.

Integration and end-to-end tests run against a real PostgreSQL instance. SQLite
is not an option: it has no JSONB, does not enforce partial unique indexes with
the same semantics, and has no ``FOR UPDATE SKIP LOCKED`` — a suite passing
against it would assert nothing about the mechanisms this service depends on.
"""

import os
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.api.app import create_app
from app.api.deps import get_clock, get_session
from app.core.config import Settings, get_settings
from app.db.repository import JobRepository
from app.services.execution_service import ExecutionService
from app.services.job_service import JobService
from tests.doubles import FrozenClock, StubRandom

FIXED_NOW = datetime(2026, 8, 4, 12, 0, 0, tzinfo=UTC)


@pytest.fixture(scope="session")
def test_db_url() -> str:
    return os.getenv("TEST_DATABASE_URL") or get_settings().test_database_url


@pytest.fixture(scope="session", autouse=True)
def migrated_database(test_db_url: str) -> Iterator[None]:
    """Bring the test database to head once per session.

    Synchronous on purpose: Alembic's async env.py calls asyncio.run, which
    cannot be nested inside a running event loop.
    """
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", test_db_url)
    command.upgrade(config, "head")
    yield


@pytest.fixture
async def engine(test_db_url: str) -> AsyncIterator[object]:
    eng = create_async_engine(test_db_url, poolclass=NullPool)
    yield eng
    await eng.dispose()


@pytest.fixture
async def db_session(engine) -> AsyncIterator[AsyncSession]:  # type: ignore[no-untyped-def]
    """A session whose work is discarded when the test ends.

    The session joins an outer transaction as a savepoint, so code under test
    can call ``commit()`` normally while the outer rollback still removes
    everything.
    """
    connection = await engine.connect()
    transaction = await connection.begin()
    session = AsyncSession(
        bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint"
    )
    try:
        yield session
    finally:
        await session.close()
        await transaction.rollback()
        await connection.close()


@pytest.fixture
def clock() -> FrozenClock:
    return FrozenClock(FIXED_NOW)


@pytest.fixture
def repository(db_session: AsyncSession) -> JobRepository:
    return JobRepository(db_session)


@pytest.fixture
def service(repository: JobRepository, clock: FrozenClock) -> JobService:
    return JobService(repository, clock)


@pytest.fixture
def execution(repository: JobRepository, clock: FrozenClock) -> ExecutionService:
    """Worker-side use cases with the jitter pinned to its maximum, so retry
    delays are the exact base values rather than a range."""
    return ExecutionService(repository, clock, rng=StubRandom(1.0))


@pytest.fixture
async def client(
    db_session: AsyncSession, clock: FrozenClock, test_db_url: str
) -> AsyncIterator[AsyncClient]:
    """The real application, wired to the rolled-back test session.

    ASGITransport does not run the lifespan, so no engine is created and the
    overridden session is the only database access path.
    """
    app = create_app(Settings(database_url=test_db_url))
    app.dependency_overrides[get_session] = lambda: db_session
    app.dependency_overrides[get_clock] = lambda: clock
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as http_client:
        yield http_client
    app.dependency_overrides.clear()


@asynccontextmanager
async def _committing_factory(
    url: str, **engine_options: Any
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Sessions that commit for real, truncating on the way out."""
    engine = create_async_engine(url, **engine_options)
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    try:
        yield factory
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("TRUNCATE jobs, job_logs CASCADE"))
        await engine.dispose()


@pytest.fixture
async def committing_sessions(test_db_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Real, independently committing sessions for concurrency tests.

    The standard ``db_session`` fixture cannot be used to test concurrent
    behaviour: sharing one rolled-back transaction would serialise exactly the
    concurrency the test exists to exercise.
    """
    async with _committing_factory(test_db_url, poolclass=NullPool) as factory:
        yield factory


@pytest.fixture
async def pooled_sessions(test_db_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """The same, over a connection pool, for tests that open many sessions.

    A worker opens a short unit of work per operation, so draining a backlog
    means thousands of them. With ``NullPool`` each one is a fresh TCP connection
    and authentication, and the test ends up measuring the harness rather than
    the queue. Production uses a pool, so this is also the more faithful shape.
    """
    async with _committing_factory(test_db_url, pool_size=20, max_overflow=10) as factory:
        yield factory
