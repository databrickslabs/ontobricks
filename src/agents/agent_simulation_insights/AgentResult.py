"""Outcome of one business-insights run."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from agents.engine_base import AgentStep


@dataclass
class AgentResult:
    """Outcome of one insights run."""

    success: bool
    reply: str = ""
    steps: List[AgentStep] = field(default_factory=list)
    iterations: int = 0
    error: str = ""
    usage: Dict[str, int] = field(default_factory=dict)
