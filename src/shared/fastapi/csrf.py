"""Double-submit cookie CSRF protection middleware.

On every response a random CSRF token is set in a cookie (``csrf_token``).
State-changing requests (POST, PUT, PATCH, DELETE) must include the same
value in an ``X-CSRF-Token`` header.  JavaScript reads the cookie and
attaches the header automatically.

Safe methods (GET, HEAD, OPTIONS) and bypass paths (static, health, API
docs, external API) are exempt.
"""

import os
import secrets
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from back.core.logging import get_logger

logger = get_logger(__name__)

_CSRF_COOKIE = "csrf_token"
_CSRF_HEADER = "x-csrf-token"
_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

_BYPASS_PREFIXES = (
    "/static/",
    "/health",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/api/",
    "/graphql/",
)


class CSRFMiddleware(BaseHTTPMiddleware):
    """Enforce double-submit cookie CSRF for state-changing requests.

    Set ``CSRF_DISABLED=1`` to skip enforcement (e.g. in automated tests).
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path

        if os.getenv("CSRF_DISABLED"):
            return await call_next(request)

        if any(path.startswith(p) for p in _BYPASS_PREFIXES):
            return await call_next(request)

        if request.method not in _SAFE_METHODS:
            cookie_token = request.cookies.get(_CSRF_COOKIE, "")
            header_token = request.headers.get(_CSRF_HEADER, "")
            if cookie_token and cookie_token != header_token:
                logger.warning(
                    "CSRF validation failed: %s %s", request.method, path
                )
                return JSONResponse(
                    {"error": "csrf", "message": "CSRF token missing or invalid"},
                    status_code=403,
                )

        response = await call_next(request)

        if _CSRF_COOKIE not in request.cookies:
            is_app = bool(os.getenv("DATABRICKS_APP_PORT"))
            response.set_cookie(
                key=_CSRF_COOKIE,
                value=secrets.token_hex(32),
                path="/",
                httponly=False,
                samesite="lax",
                secure=is_app,
            )

        return response
