"""Application factory.

A factory rather than a module-level singleton so tests can build an app bound
to the test database without importing side effects.
"""

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app import __version__
from app.api.errors import register_error_handlers
from app.api.middleware import BodySizeLimitMiddleware, RequestContextMiddleware
from app.api.routes import health, jobs
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging, get_logger
from app.db.session import create_engine, make_session_factory
from app.dispatch.base import NullDispatch
from app.dispatch.redis_dispatch import RedisDispatch

log = get_logger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(settings.database_url)
        app.state.engine = engine
        app.state.session_factory = make_session_factory(engine)
        app.state.dispatch = RedisDispatch.from_url(settings.redis_url)
        log.info("service.started", version=__version__)
        try:
            yield
        finally:
            await app.state.dispatch.close()
            await engine.dispose()
            log.info("service.stopped")

    app = FastAPI(
        title="Job Queue Service",
        version=__version__,
        description=(
            "Submit background jobs and inspect their state. Jobs are executed "
            "by separate worker processes."
        ),
        lifespan=lifespan,
    )
    app.state.started_at = time.monotonic()
    # Replaced by the Redis client once the lifespan runs. Set here so an app
    # built without a lifespan — every test that uses ASGITransport — has a
    # working dispatch rather than an AttributeError.
    app.state.dispatch = NullDispatch()

    # Order matters: add_middleware puts the most recently added outermost, so
    # RequestContextMiddleware runs first and a 413 from the size limit still
    # carries a request id.
    app.add_middleware(BodySizeLimitMiddleware)
    app.add_middleware(RequestContextMiddleware)

    register_error_handlers(app)

    app.include_router(jobs.router)
    app.include_router(health.router)
    return app
