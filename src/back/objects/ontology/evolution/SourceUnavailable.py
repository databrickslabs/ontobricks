"""Raised when a mapped source table cannot be inspected."""

from __future__ import annotations

from back.core.errors import InfrastructureError


class SourceUnavailable(InfrastructureError):
    """The table is missing or the warehouse failed; reported as SOURCE_UNAVAILABLE."""
