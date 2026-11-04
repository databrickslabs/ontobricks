"""Outcome of one simulation-facts agent turn."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from agents.engine_base import AgentStep


@dataclass
class AgentResult:
    """Outcome of one simulation-facts turn."""

    success: bool
    reply: str = ""
    proposed_patches: Optional[list] = None
    proposed_description: str = ""
    steps: List[AgentStep] = field(default_factory=list)
    iterations: int = 0
    error: str = ""
    usage: Dict[str, int] = field(default_factory=dict)
