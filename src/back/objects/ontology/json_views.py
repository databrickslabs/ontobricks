"""Read-model helpers for ontology data inside project JSON dicts."""
from __future__ import annotations

from typing import Any, Dict, List

from back.objects.project.payload import resolve_project_slice


def get_ontology_info(project_data: Dict[str, Any]) -> Dict[str, Any]:
    """Get ontology details including classes and properties."""
    sl = resolve_project_slice(project_data)
    ontology = sl["ontology"]

    return {
        "classes": ontology.get("classes", []),
        "properties": ontology.get("properties", []),
        "constraints": sl["constraints"],
        "swrl_rules": sl["swrl_rules"],
        "statistics": {
            "class_count": len(ontology.get("classes", [])),
            "property_count": len(ontology.get("properties", [])),
        },
    }


def get_ontology_classes(project_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Get list of ontology classes with URIs."""
    sl = resolve_project_slice(project_data)
    ontology = sl["ontology"]
    classes = ontology.get("classes", [])

    return [
        {
            "uri": cls.get("uri", ""),
            "name": cls.get("name", cls.get("localName", "")),
            "label": cls.get("label", cls.get("name", "")),
            "attributes": cls.get("attributes", []),
        }
        for cls in classes
    ]


def get_ontology_properties(project_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Get list of ontology properties (relationships)."""
    sl = resolve_project_slice(project_data)
    ontology = sl["ontology"]
    properties = ontology.get("properties", [])

    return [
        {
            "uri": prop.get("uri", ""),
            "name": prop.get("name", prop.get("localName", "")),
            "label": prop.get("label", prop.get("name", "")),
            "domain": prop.get("domain", ""),
            "range": prop.get("range", ""),
        }
        for prop in properties
    ]
