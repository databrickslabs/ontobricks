"""A Generate draft no longer matches the current source fingerprint."""

from __future__ import annotations

from back.core.errors import ConflictError


class DraftStaleError(ConflictError):
    """A draft's ``source_fingerprint`` no longer matches the current source (409)."""
