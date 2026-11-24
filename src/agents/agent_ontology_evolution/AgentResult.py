"""Outcome of an ontology-evolution agent run."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from agents.engine_base import AgentStep


@dataclass
class AgentResult:
    """``output`` is the validated submission: an intent or proposal alternatives."""

    success: bool
    output: Dict[str, Any] = field(default_factory=dict)
    steps: List[AgentStep] = field(default_factory=list)
    error: str = ""
    usage: Dict[str, Any] = field(default_factory=dict)
