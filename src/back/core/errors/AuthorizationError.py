"""Caller lacks permission (403)."""

from __future__ import annotations

from back.core.errors.OntoBricksError import OntoBricksError


class AuthorizationError(OntoBricksError):
    """Caller lacks permission (403)."""

    def __init__(self, message: str = "Access denied", **kw):
        super().__init__(message, status_code=403, **kw)
