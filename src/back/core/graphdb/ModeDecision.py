"""Outcome of resolving a requested materialization mode against readiness."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ModeDecision:
    """What a domain asked for, what will run, and why they differ.

    ``reason`` is ``""`` when ``effective == requested``; otherwise it names
    the first fallback that fired (e.g. ``serverless_unavailable``).
    """

    requested: str
    effective: str
    reason: str = ""
