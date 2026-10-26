"""A Generate draft or entity violates a structural or business invariant."""

from __future__ import annotations

from back.core.errors import ValidationError


class DraftValidationError(ValidationError):
    """A draft/entity violates a structural or business invariant (400)."""
