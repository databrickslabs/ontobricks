"""Resource does not exist (404)."""
from __future__ import annotations

from back.core.errors.OntoBricksError import OntoBricksError


class NotFoundError(OntoBricksError):
    """Resource does not exist (404)."""

    def __init__(self, message: str = "Resource not found", **kw):
        super().__init__(message, status_code=404, **kw)
