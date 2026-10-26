"""Per-connection scoping for the shared MCP server.

The OntoBricks MCP server binds ONE :class:`~server.session.MCPServerSession`
to the FastMCP app for the whole process (connection pooling, one registry).
Some of that state is per-connection — which domain the client selected and the
label/action caches populated for it. Without isolation, two clients hitting the
one process would clobber each other's selected domain and get wrong-domain
answers.

:data:`CURRENT_SESSION_ID` is a context variable holding the MCP session id of
the call in flight; :class:`SessionScopeMiddleware` sets it from
``ctx.session_id`` around every MCP request, so tools and resources resolve the
same per-connection state (keyed by this id in :class:`MCPServerSession`).
Nothing here imports :mod:`server.session`, so the session module can import
``CURRENT_SESSION_ID`` from here without a cycle.
"""

from __future__ import annotations

import contextvars
import logging
from typing import Any

from fastmcp.server.middleware import Middleware, MiddlewareContext

logger = logging.getLogger(__name__)

# Sentinel used when no MCP session is in scope (direct in-process calls,
# tests, resource reads). All such calls share one bucket, matching the
# pre-isolation single-session behaviour.
DEFAULT_SESSION_ID = "__default__"

CURRENT_SESSION_ID: contextvars.ContextVar[str] = contextvars.ContextVar(
    "ontobricks_mcp_session_id", default=DEFAULT_SESSION_ID
)


class SessionScopeMiddleware(Middleware):
    """Bind each MCP request to its session id for the request duration.

    ``ctx.session_id`` is stable across tool calls within one client session
    (the StreamableHTTP ``mcp-session-id``), so keying per-connection state by
    it isolates concurrent clients that share the one server process.
    """

    def __init__(self) -> None:
        self._missing_session_warned = False

    async def on_request(
        self, context: MiddlewareContext, call_next: Any
    ) -> Any:
        fmcp = getattr(context, "fastmcp_context", None)
        # ``session_id`` is a property that RAISES outside a request context
        # (e.g. in-process calls / tests), not a missing attribute — so guard
        # with try/except, not getattr's default.
        sid = None
        if fmcp is not None:
            try:
                sid = fmcp.session_id
            except RuntimeError:
                sid = None
        if not sid and not self._missing_session_warned:
            logger.warning(
                "MCP request has no session id; using shared default scope"
            )
            self._missing_session_warned = True

        # Capture the end-user identity that Databricks Apps injects on the
        # inbound HTTP request so the outbound ``http_client`` forwards it to
        # the main app (OBO + Team gating on data-plane routes). Best-effort:
        # in-process calls / tests have no HTTP request and forward nothing.
        self._bind_forwarded_identity()

        token = CURRENT_SESSION_ID.set(sid or DEFAULT_SESSION_ID)
        try:
            return await call_next(context)
        finally:
            CURRENT_SESSION_ID.reset(token)
            self._clear_forwarded_identity()

    @staticmethod
    def _bind_forwarded_identity() -> None:
        """Bind inbound ``x-forwarded-*`` user identity for outbound forwarding."""
        try:
            from server import http_client as _http
            from fastmcp.server.dependencies import get_http_headers

            headers = get_http_headers()
        except Exception:  # pragma: no cover - defensive; no HTTP request in scope
            return
        _http.set_forwarded_identity(
            email=headers.get("x-forwarded-email", ""),
            user_token=headers.get("x-forwarded-access-token", ""),
        )

    @staticmethod
    def _clear_forwarded_identity() -> None:
        try:
            from server import http_client as _http

            _http.clear_forwarded_identity()
        except Exception:  # pragma: no cover
            return
