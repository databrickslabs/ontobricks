"""Lightweight request timing middleware.

Logs ``method``, ``path``, ``status_code``, and ``duration_ms`` for every
request so slow endpoints are easy to spot in the log stream (especially
useful with ``LOG_FORMAT=json``).

Static assets and health checks are excluded to reduce noise.
"""

import time
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from back.core.logging import get_logger

logger = get_logger(__name__)

_SKIP_PREFIXES = ("/static/", "/health", "/favicon.ico")


class RequestTimingMiddleware(BaseHTTPMiddleware):
    """Log request duration for every non-static endpoint."""

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path
        if any(path.startswith(p) for p in _SKIP_PREFIXES):
            return await call_next(request)

        start = time.perf_counter()
        response = await call_next(request)
        duration_ms = (time.perf_counter() - start) * 1000

        logger.info(
            "%s %s → %d (%.1fms)",
            request.method,
            path,
            response.status_code,
            duration_ms,
        )
        return response
