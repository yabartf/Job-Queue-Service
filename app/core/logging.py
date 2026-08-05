"""Structured JSON logging.

Every line is one JSON object on stdout. Context that applies to a whole request
(``request_id``, ``service``) is bound to contextvars rather than threaded
through call signatures, so any log line emitted while handling a request
carries it without the caller doing anything.
"""

import hashlib
import json
import logging
from typing import Any

import structlog

from app.core.config import get_settings


def configure_logging(level: str | None = None) -> None:
    """Install the JSON logging pipeline. Safe to call more than once."""
    settings = get_settings()
    level_name = (level or settings.log_level).upper()
    min_level = logging.getLevelNamesMapping().get(level_name, logging.INFO)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True, key="ts"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(min_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
    structlog.contextvars.bind_contextvars(service=settings.service_name)


def get_logger(name: str | None = None) -> Any:
    return structlog.get_logger(name)


def bind_request_id(request_id: str) -> None:
    """Attach a request id to every log line emitted while handling a request.

    No explicit unbind: each ASGI request is handled in its own task, and a task
    gets its own copy of the context, so bindings cannot leak between requests.
    Unbinding here would instead strip the id from the error handler, which runs
    after the middleware unwinds — exactly where the id is most needed.
    """
    structlog.contextvars.bind_contextvars(request_id=request_id)


def payload_fingerprint(payload: dict[str, Any]) -> dict[str, Any]:
    """Loggable stand-in for a payload.

    Payloads carry user data — email addresses, message bodies — so they are
    never logged verbatim. Size and a short digest are enough to correlate two
    identical submissions without storing their contents in the log stream.
    """
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return {
        "payload_bytes": len(encoded),
        "payload_sha256": hashlib.sha256(encoded).hexdigest()[:16],
    }
