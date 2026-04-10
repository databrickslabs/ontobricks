"""
Shared ontology tools used by the auto-mapping and auto-icon-assign agents.

Provides a tool to retrieve ontology entities and relationships from the ToolContext.
Limits attribute detail per entity to avoid context overflow when ontology is large.
"""

import json
from typing import Callable, Dict, List

from back.core.logging import get_logger
from agents.tools.context import ToolContext

logger = get_logger(__name__)

# Limit attributes per entity to avoid context overflow
_MAX_ATTRIBUTES_PER_ENTITY = 30


# =====================================================
# Tool implementation
# =====================================================

def tool_get_ontology(ctx: ToolContext, **_kwargs) -> str:
    """Return ontology entities and relationships that need mapping.
    When entities have many attributes, only the first N are included to avoid context overflow."""
    logger.info("tool_get_ontology: retrieving ontology data")
    ontology = ctx.ontology or {}
    entities = ontology.get("entities", [])
    relationships = ontology.get("relationships", [])

    # Trim attributes per entity if too many
    trimmed_entities: List[dict] = []
    for e in entities:
        attrs = e.get("attributes", [])
        if len(attrs) > _MAX_ATTRIBUTES_PER_ENTITY:
            trimmed = attrs[:_MAX_ATTRIBUTES_PER_ENTITY]
            trimmed_entities.append({
                **e,
                "attributes": trimmed,
                "_note": f"Showing first {len(trimmed)} of {len(attrs)} attributes",
            })
            logger.debug("tool_get_ontology: entity '%s' — trimmed %d → %d attributes",
                         e.get("name", "?"), len(attrs), len(trimmed))
        else:
            trimmed_entities.append(e)

    logger.info(
        "tool_get_ontology: returning %d entities, %d relationships",
        len(trimmed_entities), len(relationships),
    )
    return json.dumps({
        "entities": trimmed_entities,
        "relationships": relationships,
        "entity_count": len(trimmed_entities),
        "relationship_count": len(relationships),
    })


# =====================================================
# OpenAI function-calling definition
# =====================================================

ONTOLOGY_TOOL_DEFINITIONS: List[dict] = [
    {
        "type": "function",
        "function": {
            "name": "get_ontology",
            "description": (
                "Get the ontology entities (classes with their data-property attributes) and "
                "object-property relationships (with domain, range, direction) that need SQL mappings."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
]

ONTOLOGY_TOOL_HANDLERS: Dict[str, Callable] = {
    "get_ontology": tool_get_ontology,
}
