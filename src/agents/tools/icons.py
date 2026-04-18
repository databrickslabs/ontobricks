"""
Icon tools – used by the auto-icon-assign agent.

Provides a tool to save icon assignments into the ToolContext so the
caller (route) can apply them to the ontology.
"""

import json
from typing import Callable, Dict, List

from back.core.logging import get_logger
from agents.tools.context import ToolContext

logger = get_logger(__name__)


# =====================================================
# Tool implementation
# =====================================================


def tool_assign_icons(ctx: ToolContext, *, icons: dict, **_kwargs) -> str:
    """Persist a {entity_name: emoji} mapping into the context for the caller."""
    if not isinstance(icons, dict):
        logger.warning("tool_assign_icons: 'icons' is not a dict (%s)", type(icons))
        return json.dumps({"error": "icons must be a JSON object {name: emoji}"})

    logger.info("tool_assign_icons: saving %d icon mapping(s)", len(icons))
    logger.debug("tool_assign_icons: %s", icons)

    ctx.icon_results.update(icons)

    return json.dumps(
        {
            "saved": len(icons),
            "total_assigned": len(ctx.icon_results),
        }
    )


# =====================================================
# OpenAI function-calling definitions
# =====================================================

ICON_TOOL_DEFINITIONS: List[dict] = [
    {
        "type": "function",
        "function": {
            "name": "assign_icons",
            "description": (
                "Save the chosen emoji icons for ontology entities. "
                "Pass a JSON object mapping each entity name to a single Unicode emoji. "
                'Example: {"Customer": "🧑", "Order": "📋"}'
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "icons": {
                        "type": "object",
                        "description": "Mapping of entity name → single emoji character",
                        "additionalProperties": {"type": "string"},
                    },
                },
                "required": ["icons"],
            },
        },
    },
]

ICON_TOOL_HANDLERS: Dict[str, Callable] = {
    "assign_icons": tool_assign_icons,
}
