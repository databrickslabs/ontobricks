"""Raised when the caller may not read a mapped source table."""

from __future__ import annotations

from back.core.errors import AuthorizationError


class SourceAccessDenied(AuthorizationError):
    """The caller lacks permission on a source table; reported as INACCESSIBLE."""
