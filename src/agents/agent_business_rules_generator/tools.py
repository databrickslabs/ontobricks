"""
Tool assembly for the Business Rules Generator Agent.

The agent grounds its rule proposals in the domain's uploaded documents
(``list_documents`` / ``read_document`` from the shared document tools) and
the live ontology design (``get_ontology_design`` — a read-only snapshot of
classes/attributes and object-property relationships parked on the
:class:`ToolContext`).

The agent does NOT write anything: the four rule lists are emitted as a final
JSON object that the engine parses; persistence stays user-driven (review +
accept) in the Business Rules page.
"""

import json
from typing import Callable, Dict, List

from agents.tools.context import ToolContext
from agents.tools.documents import (
    DOCUMENT_TOOL_DEFINITIONS,
    DOCUMENT_TOOL_HANDLERS,
)

__all__ = ["ToolContext", "TOOL_DEFINITIONS", "TOOL_HANDLERS"]


def tool_get_ontology_design(ctx: ToolContext, **_kwargs) -> str:
    """Return the active domain's ontology design snapshot.

    Reads ``ctx.ontology`` which the engine populates from
    ``Ontology.agent_ontology_context()`` (entities with their datatype
    attributes + object-property relationships). The LLM uses class and
    property *names* from here to build rules that reference real ontology
    terms.
    """
    design = ctx.ontology or {}
    entities = design.get("entities", [])
    relationships = design.get("relationships", [])
    if not entities and not relationships:
        return json.dumps(
            {
                "entities": [],
                "relationships": [],
                "message": (
                    "The ontology is empty. Define classes and properties "
                    "first (Ontology designer or the Generate wizard) before "
                    "generating business rules."
                ),
            }
        )
    return json.dumps(
        {
            "entities": entities,
            "relationships": relationships,
            "entity_count": len(entities),
            "relationship_count": len(relationships),
        }
    )


_GET_ONTOLOGY_DESIGN_DEF = {
    "type": "function",
    "function": {
        "name": "get_ontology_design",
        "description": (
            "Return the active domain's ontology design: classes (with their "
            "datatype attributes) and object-property relationships (with "
            "domain/range). Always call this first so every rule references "
            "real class and property names from the ontology."
        ),
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
}

TOOL_DEFINITIONS: List[dict] = [
    _GET_ONTOLOGY_DESIGN_DEF
] + DOCUMENT_TOOL_DEFINITIONS

TOOL_HANDLERS: Dict[str, Callable] = {
    "get_ontology_design": tool_get_ontology_design,
    **DOCUMENT_TOOL_HANDLERS,
}
