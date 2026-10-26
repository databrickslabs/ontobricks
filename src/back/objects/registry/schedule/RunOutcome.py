"""Outcome of one scheduled task run."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class RunOutcome:
    """What a task executor reports back to the scheduler harness.

    ``count`` is the generic "how much did this run write" number shown
    in the schedule table and history; ``detail`` carries the
    type-specific counters that do not fit it.
    """

    status: str = "success"
    message: str = ""
    count: int = 0
    detail: Dict[str, Any] = field(default_factory=dict)
    task_result: Optional[Dict[str, Any]] = None
