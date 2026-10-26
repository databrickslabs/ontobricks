"""Raised when the Lakebase connection pool cannot serve a connection."""

from __future__ import annotations


class LakebaseConnectionError(RuntimeError):
    """Raised when the pool cannot serve a Lakebase connection."""
