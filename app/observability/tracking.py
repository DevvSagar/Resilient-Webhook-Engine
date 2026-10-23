import logging
import time
import uuid
from contextvars import ContextVar
from typing import Callable, Optional

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger("relayhook.tracking")

# Context-local storage for request trace IDs across asynchronous tasks.
# Each async coroutine gets its own isolated request_id without thread-safety bugs.
request_id_ctx: ContextVar[Optional[str]] = ContextVar("request_id", default=None)


def get_request_id() -> Optional[str]:
    """Retrieves the current request trace ID from async context."""
    return request_id_ctx.get()


def set_request_id(req_id: str) -> None:
    """Sets the request trace ID for the current async task context."""
    request_id_ctx.set(req_id)


class RequestTracingMiddleware(BaseHTTPMiddleware):
    """
    Middleware for distributed request tracing and structured latency tracking.
    1. Extracts client-supplied X-Request-Id or generates a new trace ID.
    2. Injects the trace ID into the async ContextVar.
    3. Measures request execution duration in milliseconds.
    4. Attaches X-Request-Id to outgoing response headers.
    5. Emits structured lifecycle logs with timing data.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Extract existing trace ID or generate a unique ID
        request_id = request.headers.get("X-Request-Id") or f"req_{uuid.uuid4().hex[:12]}"
        set_request_id(request_id)

        start_time = time.time()
        is_health = request.url.path in ["/health", "/"]

        if not is_health:
            logger.info(
                "Incoming HTTP request",
                extra={
                    "http_method": request.method,
                    "http_path": request.url.path,
                    "client_ip": request.client.host if request.client else None,
                },
            )

        try:
            response: Response = await call_next(request)
            duration_ms = round((time.time() - start_time) * 1000, 2)
            response.headers["X-Request-Id"] = request_id

            if not is_health:
                logger.info(
                    "HTTP request completed",
                    extra={
                        "http_method": request.method,
                        "http_path": request.url.path,
                        "status_code": response.status_code,
                        "duration_ms": duration_ms,
                    },
                )
            return response

        except Exception as exc:
            duration_ms = round((time.time() - start_time) * 1000, 2)
            logger.error(
                "HTTP request failed with unhandled exception",
                exc_info=True,
                extra={
                    "http_method": request.method,
                    "http_path": request.url.path,
                    "duration_ms": duration_ms,
                },
            )
            raise exc
