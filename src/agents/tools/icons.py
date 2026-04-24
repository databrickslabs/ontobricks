"""
Icon tools – used by the auto-icon-assign agent.

Provides a tool to save icon assignments into the ToolContext so the
caller (route) can apply them to the ontology.
"""

import json
from typing import Callable, Dict, List, Optional

from back.core.logging import get_logger
from agents.tools.context import ToolContext

logger = get_logger(__name__)


_ALT_ICON_KEYS = ("mapping", "icon_map", "assignments", "emoji", "emojis")


# =====================================================
# Tool implementation
# =====================================================


def tool_assign_icons(
    ctx: ToolContext, *, icons: Optional[dict] = None, **_kwargs
) -> str:
    """Persist a {entity_name: emoji} mapping into the context for the caller.

    Robust to common LLM deviations:
      - ``icons`` missing / ``None`` because the tool-call JSON was truncated
        (big-ontology case).  Returns a structured error telling the model to
        retry with a smaller payload, instead of raising ``TypeError``.
      - ``icons`` shipped under a different key (``mapping``, ``icon_map`` …).
      - The mapping flattened as top-level kwargs
        (``{"Customer": "🧑", "Order": "📋"}`` instead of ``{"icons": {...}}``).
    """
    if icons is None:
        for alt in _ALT_ICON_KEYS:
            candidate = _kwargs.pop(alt, None)
            if isinstance(candidate, dict):
                icons = candidate
                logger.info("tool_assign_icons: recovered icons from '%s' key", alt)
                break

    if (
        icons is None
        and _kwargs
        and all(isinstance(v, str) for v in _kwargs.values())
    ):
        icons = dict(_kwargs)
        _kwargs = {}
        logger.info(
            "tool_assign_icons: recovered icons from flattened kwargs (%d keys)",
            len(icons),
        )

    if not isinstance(icons, dict) or not icons:
        logger.warning(
            "tool_assign_icons: 'icons' missing/invalid (got %s); "
            "tool-call arguments were likely truncated by the token budget",
            type(icons).__name__,
        )
        return json.dumps(
            {
                "error": (
                    "Missing or empty 'icons' argument. Your tool-call JSON may "
                    "have been truncated. Call assign_icons again with a JSON "
                    "object of the form "
                    '{"icons": {"Customer": "🧑", "Order": "📋"}}. If the '
                    "ontology is large, assign entities in smaller batches "
                    "across successive tool calls."
                )
            }
        )

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
