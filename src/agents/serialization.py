"""Shared serialization for agent results (task payloads, APIs)."""

from __future__ import annotations

from typing import Any, Dict, List


def serialize_agent_steps(steps: Any) -> List[Dict[str, Any]]:
    """Normalize agent step records for JSON task results.

    Output matches the ontology wizard UI: ``type``, ``tool``, ``content``, ``ms``.
    Accepts any iterable of objects with ``step_type``, ``tool_name``, ``content``,
    ``duration_ms`` (all OntoBricks agent ``AgentStep`` dataclasses).
    """
    if not steps:
        return []
    out: List[Dict[str, Any]] = []
    for s in steps:
        out.append(
            {
                "type": getattr(s, "step_type", "") or "",
                "tool": getattr(s, "tool_name", "") or "",
                "content": getattr(s, "content", "") or "",
                "ms": int(getattr(s, "duration_ms", 0) or 0),
            }
        )
    return out
