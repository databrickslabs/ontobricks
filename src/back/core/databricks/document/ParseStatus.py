"""Lifecycle status of a document parse submission."""

from __future__ import annotations

from enum import Enum


class ParseStatus(str, Enum):
    """Durable document parse states (mirrors the DB ``status`` column)."""

    PENDING = "pending"
    READY = "ready"
    FAILED = "failed"
