"""Cross-cutting request handling: correlation ids and body size limits."""

import re
import time
from collections.abc import Awaitable, Callable
from uuid import uuid4

from fastapi import Request, Response, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from app.core.config import get_settings
from app.core.logging import bind_request_id, get_logger

log = get_logger(__name__)

# An inbound id is echoed into every log line, so it is constrained rather than
# trusted: an unbounded header value would be a log injection vector.
_SAFE_REQUEST_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")

Dispatch = Callable[[Request], Awaitable[Response]]


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Assign a request id, bind it for logging, and echo it to the client."""

    async def dispatch(self, request: Request, call_next: Dispatch) -> Response:
        inbound = request.headers.get("x-request-id")
        request_id = inbound if inbound and _SAFE_REQUEST_ID.match(inbound) else uuid4().hex
        request.state.request_id = request_id
        bind_request_id(request_id)

        started = time.perf_counter()
        response = await call_next(request)
        duration_ms = round((time.perf_counter() - started) * 1000, 2)

        response.headers["X-Request-ID"] = request_id
        log.info(
            "http.request",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=duration_ms,
        )
        return response


class BodySizeLimitMiddleware(BaseHTTPMiddleware):
    """Reject oversized bodies before anything tries to parse them.

    A validator cannot defend against a payload that is hostile by size, because
    rejecting it would require parsing it first.

    Limitation: this reads Content-Length. A client using chunked transfer
    encoding sends no such header and slips past — in a real deployment the
    reverse proxy enforces the hard limit, and this is the application-level
    backstop that keeps the error shape consistent.
    """

    async def dispatch(self, request: Request, call_next: Dispatch) -> Response:
        limit = get_settings().max_request_body_bytes
        declared = request.headers.get("content-length")
        if declared is not None and declared.isdigit() and int(declared) > limit:
            request_id = getattr(request.state, "request_id", None)
            return JSONResponse(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                content={
                    "error": {
                        "code": "payload_too_large",
                        "message": f"Request body exceeds {limit} bytes",
                        "request_id": request_id,
                        "details": [],
                    }
                },
            )
        return await call_next(request)
