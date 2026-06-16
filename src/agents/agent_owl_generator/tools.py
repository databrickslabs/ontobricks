"""
Tool assembly for the OWL Generator Agent.

Composes the set of tools available to this agent from the shared
``agents.tools`` package.

Tools available to the agent:
  • list_documents / read_document  — discover and read domain documents
  • get_metadata / get_table_detail — inspect the database schema

Note: check_owl_pitfalls is NOT offered to the LLM.  The engine invokes it
directly after each OWL text output so the Turtle never enters LLM context.
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

# check_owl_pitfalls is intentionally NOT included here.
# The engine calls it directly after each OWL text output (external loop),
# which avoids the overhead of routing the full Turtle through the LLM context.
TOOL_DEFINITIONS: List[dict] = DOCUMENT_TOOL_DEFINITIONS + METADATA_TOOL_DEFINITIONS

TOOL_HANDLERS: Dict[str, Callable] = {
    **DOCUMENT_TOOL_HANDLERS,
    **METADATA_TOOL_HANDLERS,
}
