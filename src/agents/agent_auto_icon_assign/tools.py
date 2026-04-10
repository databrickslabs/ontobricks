"""
Tool assembly for the Auto Icon Assign Agent.

Composes the set of tools available to this agent from the shared
``agents.tools`` package.
"""

from typing import Callable, Dict, List

from agents.tools.context import ToolContext
from agents.tools.ontology import (
    ONTOLOGY_TOOL_DEFINITIONS,
    ONTOLOGY_TOOL_HANDLERS,
)
from agents.tools.metadata import (
    tool_get_metadata,
)
from agents.tools.icons import (
    ICON_TOOL_DEFINITIONS,
    ICON_TOOL_HANDLERS,
)

__all__ = ["ToolContext", "TOOL_DEFINITIONS", "TOOL_HANDLERS"]

_METADATA_DEF = {
    "type": "function",
    "function": {
        "name": "get_metadata",
        "description": (
            "Get the project's database table metadata: table names, "
            "column names, data types, and descriptions. Useful to understand "
            "what each entity represents in the data."
        ),
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
}

TOOL_DEFINITIONS: List[dict] = (
    [_METADATA_DEF]
    + ONTOLOGY_TOOL_DEFINITIONS
    + ICON_TOOL_DEFINITIONS
)

TOOL_HANDLERS: Dict[str, Callable] = {
    "get_metadata": tool_get_metadata,
    **ONTOLOGY_TOOL_HANDLERS,
    **ICON_TOOL_HANDLERS,
}
