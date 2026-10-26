"""Request-scoped caller identity for on-behalf-of (OBO) data access.

``PermissionMiddleware`` stamps the forwarded user token and resolved roles
here so deep call sites (graph query pipeline, credential factory) can mint a
user-scoped Databricks client without threading the token through every
signature. Backed by a ``ContextVar`` so it is isolated per request/task.
"""

from __future__ import annotations

import contextvars
from dataclasses import dataclass


@dataclass(frozen=True)
class RequestIdentity:
    """Immutable snapshot of the caller for the current request."""

    email: str = ""
    user_token: str = ""
    app_role: str = ""
    domain_role: str = ""


_EMPTY = RequestIdentity()
_CURRENT: "contextvars.ContextVar[RequestIdentity]" = contextvars.ContextVar(
    "ontobricks_request_identity", default=_EMPTY
)


def set_request_identity(identity: RequestIdentity) -> contextvars.Token:
    """Bind *identity* to the current context; return a reset token."""
    return _CURRENT.set(identity)


def get_request_identity() -> RequestIdentity:
    """Return the current request identity (empty when unset)."""
    return _CURRENT.get()


def reset_request_identity(token: contextvars.Token) -> None:
    """Restore the identity captured before :func:`set_request_identity`."""
    _CURRENT.reset(token)
