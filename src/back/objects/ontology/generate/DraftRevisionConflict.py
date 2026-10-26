"""A Generate draft write supplied a stale revision (optimistic concurrency)."""

from __future__ import annotations

from back.core.errors import ConflictError


class DraftRevisionConflict(ConflictError):
    """A write supplied a stale ``draft_revision`` (409, optimistic concurrency)."""
