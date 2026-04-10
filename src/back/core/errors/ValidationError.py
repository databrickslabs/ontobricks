"""Client-supplied data is invalid (400)."""
from __future__ import annotations

from back.core.errors.OntoBricksError import OntoBricksError


class ValidationError(OntoBricksError):
    """Client-supplied data is invalid (400)."""

    def __init__(self, message: str = "Validation failed", **kw):
        super().__init__(message, status_code=400, **kw)
