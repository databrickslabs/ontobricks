"""
Tool assembly for the Auto-Mapping Agent.

Composes the set of tools available to this agent from the shared
``agents.tools`` package.
"""

from typing import Callable, Dict, List

from agents.tools.context import ToolContext
from agents.tools.metadata import (
    tool_get_metadata,
)
from agents.tools.ontology import (
    ONTOLOGY_TOOL_DEFINITIONS,
    ONTOLOGY_TOOL_HANDLERS,
)
from agents.tools.sql import (
    SQL_TOOL_DEFINITIONS,
    SQL_TOOL_HANDLERS,
)
from agents.tools.mapping import (
    MAPPING_TOOL_DEFINITIONS,
    MAPPING_TOOL_HANDLERS,
)
from agents.tools.documents import (
    GET_DOCUMENTS_CONTEXT_DEF,
    tool_get_documents_context,
)

__all__ = ["ToolContext", "TOOL_DEFINITIONS", "TOOL_HANDLERS"]

# Only include get_metadata (not get_table_detail) for this agent
# Uses imported metadata only — no Unity Catalog queries
_METADATA_DEF = {
    "type": "function",
    "function": {
        "name": "get_metadata",
        "description": (
            "Get the project's imported table metadata (loaded in Project settings): "
            "table names (full catalog.schema.table), column names, types, comments. "
            "Does NOT query Unity Catalog. Call this first to understand available data."
        ),
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
}

TOOL_DEFINITIONS: List[dict] = (
    [_METADATA_DEF, GET_DOCUMENTS_CONTEXT_DEF]
    + ONTOLOGY_TOOL_DEFINITIONS
    + SQL_TOOL_DEFINITIONS
    + MAPPING_TOOL_DEFINITIONS
)

TOOL_HANDLERS: Dict[str, Callable] = {
    "get_metadata": tool_get_metadata,
    "get_documents_context": tool_get_documents_context,
    **ONTOLOGY_TOOL_HANDLERS,
    **SQL_TOOL_HANDLERS,
    **MAPPING_TOOL_HANDLERS,
}
