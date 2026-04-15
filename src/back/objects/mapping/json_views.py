"""Read-model helpers for mapping / assignment data inside domain JSON dicts."""
from __future__ import annotations

from typing import Any, Dict

from back.objects.domain.payload import resolve_domain_slice


def get_mapping_info(domain_data: Dict[str, Any]) -> Dict[str, Any]:
    """Get mapping details for the active domain version."""
    sl = resolve_domain_slice(domain_data)
    assignment = sl["assignment"]

    entities = assignment.get("entities", assignment.get("data_source_mappings", []))
    relationships = assignment.get(
        "relationships", assignment.get("relationship_mappings", [])
    )

    return {
        "entities": entities,
        "relationships": relationships,
        "statistics": {
            "entity_count": len(entities),
            "relationship_count": len(relationships),
        },
    }
