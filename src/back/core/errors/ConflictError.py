"""State conflict, e.g. duplicate entity (409)."""

from __future__ import annotations

from back.core.errors.OntoBricksError import OntoBricksError


class ConflictError(OntoBricksError):
    """State conflict, e.g. duplicate entity (409)."""

    def __init__(self, message: str = "Conflict", **kw):
        super().__init__(message, status_code=409, **kw)
