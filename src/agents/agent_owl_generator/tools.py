"""
Tool assembly for the OWL Generator Agent.

Composes the set of tools available to this agent from the shared
``agents.tools`` package.
"""

from typing import Callable, Dict, List

from agents.tools.context import ToolContext
from agents.tools.metadata import (
    METADATA_TOOL_DEFINITIONS,
    METADATA_TOOL_HANDLERS,
)
from agents.tools.documents import (
    DOCUMENT_TOOL_DEFINITIONS,
    DOCUMENT_TOOL_HANDLERS,
)

__all__ = ["ToolContext", "TOOL_DEFINITIONS", "TOOL_HANDLERS"]

TOOL_DEFINITIONS: List[dict] = DOCUMENT_TOOL_DEFINITIONS + METADATA_TOOL_DEFINITIONS

TOOL_HANDLERS: Dict[str, Callable] = {
    **DOCUMENT_TOOL_HANDLERS,
    **METADATA_TOOL_HANDLERS,
}
