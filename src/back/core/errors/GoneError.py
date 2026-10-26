"""Resource/route permanently removed, e.g. a decommissioned legacy route (410)."""

from __future__ import annotations

from back.core.errors.OntoBricksError import OntoBricksError


class GoneError(OntoBricksError):
    """Resource/route permanently removed, e.g. a decommissioned legacy route (410)."""

    def __init__(self, message: str = "Gone", **kw):
        super().__init__(message, status_code=410, **kw)
