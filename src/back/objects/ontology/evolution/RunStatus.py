"""Lifecycle states of an Ontology Evolve run."""

from __future__ import annotations

from enum import Enum


class RunStatus(str, Enum):
    """Where a run is in its lifecycle. Terminal states never change again."""

    CREATED = "CREATED"
    RUNNING = "RUNNING"
    ASSESSED = "ASSESSED"
    PROPOSED = "PROPOSED"
    EVALUATED = "EVALUATED"
    ACCEPTED = "ACCEPTED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    INTERRUPTED = "INTERRUPTED"

    @property
    def terminal(self) -> bool:
        return self in (RunStatus.ACCEPTED, RunStatus.CANCELLED, RunStatus.FAILED)
