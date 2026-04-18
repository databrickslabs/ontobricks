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
    GET_METADATA_DEF,
    tool_get_metadata,
)
from agents.tools.icons import (
    ICON_TOOL_DEFINITIONS,
    ICON_TOOL_HANDLERS,
)

__all__ = ["ToolContext", "TOOL_DEFINITIONS", "TOOL_HANDLERS"]

TOOL_DEFINITIONS: List[dict] = (
    [GET_METADATA_DEF] + ONTOLOGY_TOOL_DEFINITIONS + ICON_TOOL_DEFINITIONS
)

TOOL_HANDLERS: Dict[str, Callable] = {
    "get_metadata": tool_get_metadata,
    **ONTOLOGY_TOOL_HANDLERS,
    **ICON_TOOL_HANDLERS,
}
