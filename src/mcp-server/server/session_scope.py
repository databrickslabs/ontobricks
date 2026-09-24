"""Per-connection scoping for the shared MCP server.

The OntoBricks MCP server binds ONE :class:`~server.session.MCPServerSession`
to the FastMCP app for the whole process (connection pooling, one registry).
Some of that state is per-connection — which domain the client selected and the
label/action caches populated for it. Without isolation, two clients hitting the
one process would clobber each other's selected domain and get wrong-domain
answers.

:data:`CURRENT_SESSION_ID` is a context variable holding the MCP session id of
the call in flight; :class:`SessionScopeMiddleware` sets it from
``ctx.session_id`` around every tool call, so the session's per-connection state
(keyed by this id in :class:`MCPServerSession`) is naturally isolated. Nothing
here imports :mod:`server.session`, so the session module can import
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
    """Bind each tool call to its MCP session id for the duration of the call.

    ``ctx.session_id`` is stable across tool calls within one client session
    (the StreamableHTTP ``mcp-session-id``), so keying per-connection state by
    it isolates concurrent clients that share the one server process.
    """

    async def on_call_tool(
        self, context: MiddlewareContext, call_next: Any
    ) -> Any:
        token = None
        fmcp = getattr(context, "fastmcp_context", None)
        # ``session_id`` is a property that RAISES outside a request context
        # (e.g. in-process calls / tests), not a missing attribute — so guard
        # with try/except, not getattr's default.
        sid = None
        if fmcp is not None:
            try:
                sid = fmcp.session_id
            except Exception:  # noqa: BLE001 - absence is expected off-request
                sid = None
        if sid:
            token = CURRENT_SESSION_ID.set(sid)
        else:
            logger.debug("on_call_tool: no session id in context; using default scope")
        try:
            return await call_next(context)
        finally:
            if token is not None:
                CURRENT_SESSION_ID.reset(token)
