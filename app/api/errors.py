"""Domain errors to HTTP responses, decided in one place."""

from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.core.errors import (
    DomainError,
    JobNotCancellableError,
    JobNotFoundError,
    JobNotRetryableError,
    PayloadTooLargeError,
    PayloadValidationError,
    UnknownJobTypeError,
)
from app.core.logging import get_logger

log = get_logger(__name__)

_STATUS_BY_ERROR: dict[type[DomainError], int] = {
    JobNotFoundError: status.HTTP_404_NOT_FOUND,
    JobNotCancellableError: status.HTTP_409_CONFLICT,
    JobNotRetryableError: status.HTTP_409_CONFLICT,
    UnknownJobTypeError: status.HTTP_422_UNPROCESSABLE_CONTENT,
    PayloadValidationError: status.HTTP_422_UNPROCESSABLE_CONTENT,
    PayloadTooLargeError: status.HTTP_413_CONTENT_TOO_LARGE,
}


def error_response(
    status_code: int,
    code: str,
    message: str,
    request: Request,
    details: list[dict[str, Any]] | None = None,
) -> JSONResponse:
    request_id = getattr(request.state, "request_id", None)
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": code,
                "message": message,
                "request_id": request_id,
                "details": details or [],
            }
        },
        headers={"X-Request-ID": request_id} if request_id else None,
    )


async def handle_domain_error(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, DomainError)
    status_code = _STATUS_BY_ERROR.get(type(exc), status.HTTP_422_UNPROCESSABLE_CONTENT)
    return error_response(status_code, exc.code, exc.message, request, exc.details)


async def handle_request_validation(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, RequestValidationError)
    details = [
        {
            "field": ".".join(str(part) for part in err["loc"][1:]) or str(err["loc"]),
            "message": err["msg"],
        }
        for err in exc.errors()
    ]
    return error_response(
        status.HTTP_422_UNPROCESSABLE_CONTENT,
        "payload_invalid",
        "Request failed validation",
        request,
        details,
    )


async def handle_unexpected(request: Request, exc: Exception) -> JSONResponse:
    # The detail stays server-side: response bodies never carry tracebacks,
    # driver text or SQL. The request_id is how an operator joins the two.
    log.exception(
        "request.unhandled_error",
        path=request.url.path,
        method=request.method,
        exc_info=exc,
    )
    return error_response(
        status.HTTP_500_INTERNAL_SERVER_ERROR,
        "internal_error",
        "An internal error occurred",
        request,
    )


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(DomainError, handle_domain_error)
    app.add_exception_handler(RequestValidationError, handle_request_validation)
    app.add_exception_handler(Exception, handle_unexpected)
