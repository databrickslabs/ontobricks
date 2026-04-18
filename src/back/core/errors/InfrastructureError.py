"""An external dependency (Databricks, network, ...) failed (502)."""

from __future__ import annotations

from back.core.errors.OntoBricksError import OntoBricksError


class InfrastructureError(OntoBricksError):
    """An external dependency (Databricks, network, ...) failed (502)."""

    def __init__(self, message: str = "Service unavailable", **kw):
        super().__init__(message, status_code=502, **kw)
