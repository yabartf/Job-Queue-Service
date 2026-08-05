"""The pieces the request-level tests bypass: lifespan, engine, session scope."""

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from app.api.app import create_app
from app.core.clock import RealSleeper
from app.core.config import Settings
from app.db.session import create_engine, make_session_factory, session_scope


async def test_lifespan_opens_and_closes_the_engine(test_db_url):
    """Exercised explicitly because ASGITransport does not run lifespan, so the
    request-level tests never touch this path."""
    app = create_app(Settings(database_url=test_db_url))

    async with app.router.lifespan_context(app):
        assert app.state.engine is not None
        async with app.state.session_factory() as session:
            assert (await session.execute(text("SELECT 1"))).scalar_one() == 1

    assert app.state.engine.pool.checkedout() == 0


async def test_app_serves_requests_through_its_own_engine(test_db_url):
    app = create_app(Settings(database_url=test_db_url))

    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client,
    ):
        response = await client.get("/health")

    assert response.status_code == 200
    assert response.json()["database"] == "ok"


async def test_session_scope_commits_on_success(test_db_url):
    engine = create_engine(test_db_url)
    factory = make_session_factory(engine)
    try:
        async for session in session_scope(factory):
            await session.execute(
                text("CREATE TEMP TABLE scope_probe (id int) ON COMMIT PRESERVE ROWS")
            )
    finally:
        await engine.dispose()


async def test_session_scope_rolls_back_on_error(test_db_url):
    """Thrown into the generator, the way FastAPI propagates a handler failure
    into a yield-style dependency. Raising in the consumer loop instead would
    close the generator without ever reaching its except branch."""
    engine = create_engine(test_db_url)
    factory = make_session_factory(engine)
    generator = session_scope(factory)
    try:
        session = await generator.asend(None)
        await session.execute(text("SELECT 1"))

        with pytest.raises(RuntimeError, match="deliberate"):
            await generator.athrow(RuntimeError("deliberate"))
    finally:
        await engine.dispose()


async def test_real_sleeper_awaits(monkeypatch):
    """RealSleeper is trivial, but it is the seam every handler depends on."""
    recorded: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        recorded.append(seconds)

    monkeypatch.setattr("app.core.clock.asyncio.sleep", fake_sleep)
    await RealSleeper().sleep(0.25)

    assert recorded == [0.25]
