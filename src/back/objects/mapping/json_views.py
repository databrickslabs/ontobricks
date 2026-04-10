"""Read-model helpers for mapping / assignment data inside project JSON dicts."""
from __future__ import annotations

from typing import Any, Dict

from back.objects.project.payload import resolve_project_slice


def get_mapping_info(project_data: Dict[str, Any]) -> Dict[str, Any]:
    """Get mapping details for the active project version."""
    sl = resolve_project_slice(project_data)
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
