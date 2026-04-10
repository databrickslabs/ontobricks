"""Pure functions over on-disk / API project JSON (versioned or legacy)."""
from __future__ import annotations

from typing import Any, Dict

def resolve_project_slice(project_data: Dict[str, Any]) -> Dict[str, Any]:
    """Return the active ontology, assignment, and related fields for API/query use.

    For versioned documents, uses the highest numeric version key (same rule as
    :func:`get_project_info`). For legacy documents, reads top-level keys.

    Returns:
        Dict with keys: ``version``, ``ontology``, ``assignment``, ``constraints``, ``swrl_rules``.
    """
    info = project_data.get("info", {})
    version_key: str
    ontology: Dict[str, Any]
    assignment: Dict[str, Any]

    if "versions" in project_data:
        versions = project_data["versions"]
        version_keys = sorted(versions.keys(), reverse=True)
        if version_keys:
            version_key = version_keys[0]
            vd = versions[version_key]
            ontology = vd.get("ontology", {})
            assignment = vd.get("assignment", vd.get("mapping", {}))
            constraints = vd.get("constraints", project_data.get("constraints", []))
            swrl_rules = vd.get("swrl_rules", project_data.get("swrl_rules", []))
        else:
            version_key = "1"
            ontology = {}
            assignment = {}
            constraints = project_data.get("constraints", [])
            swrl_rules = project_data.get("swrl_rules", [])
    else:
        version_key = str(info.get("version", "1"))
        ontology = project_data.get("ontology", {})
        assignment = project_data.get("assignment", project_data.get("mapping", {}))
        constraints = project_data.get("constraints", [])
        swrl_rules = project_data.get("swrl_rules", [])

    return {
        "version": version_key,
        "ontology": ontology,
        "assignment": assignment,
        "constraints": constraints,
        "swrl_rules": swrl_rules,
    }


def get_project_info(project_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract project information and statistics.

    Supports both new versioned format and legacy flat format.
    """
    info = project_data.get("info", {})
    sl = resolve_project_slice(project_data)
    version = sl["version"]
    ontology = sl["ontology"]
    assignment = sl["assignment"]

    return {
        "name": info.get("name", "Untitled"),
        "description": info.get("description", ""),
        "uri": info.get("uri", ""),
        "version": version,
        "author": info.get("author", ""),
        "statistics": {
            "classes": len(ontology.get("classes", [])),
            "properties": len(ontology.get("properties", [])),
            "entities": len(
                assignment.get("entities", assignment.get("data_source_mappings", []))
            ),
            "relationships": len(
                assignment.get(
                    "relationships", assignment.get("relationship_mappings", [])
                )
            ),
            "has_r2rml": bool(assignment.get("r2rml_output")),
        },
    }
