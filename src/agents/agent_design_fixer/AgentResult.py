"""Outcome of a design-fixer agent run."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from agents.engine_base import AgentStep


@dataclass
class AgentResult:
    """Outcome of a design-fixer run."""

    success: bool
    plan: Dict[str, Any] = field(default_factory=dict)
    steps: List[AgentStep] = field(default_factory=list)
    error: str = ""
    llm_available: bool = True
    usage: Dict[str, Any] = field(default_factory=dict)
