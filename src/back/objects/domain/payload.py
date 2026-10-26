"""Pure functions over on-disk / API domain JSON (versioned or legacy)."""

from __future__ import annotations

from typing import Any, Dict, Mapping

from back.core.graphdb.GraphDBFactory import normalize_graph_backend
from back.core.logging import get_logger

logger = get_logger(__name__)


def coerce_version_key(value: Any) -> str:
    """Return a canonical version string, or ``""`` when *value* is unset."""
    if value is None:
        return ""
    return str(value).strip()


def _numeric_version_key(value: str) -> list[int]:
    try:
        return [int(part) for part in str(value).split(".")]
    except (TypeError, ValueError):
        return [0]


def select_loaded_version(
    versions: Mapping[Any, Any],
    requested: Any = None,
) -> str:
    """Pick the version key a load/session must use.

    JSON load payloads send ``version`` as an int; document keys are
    strings. A raw ``1 in versions`` miss used to fall through to
    lexicographic latest (``"2"``), so a V1 load queried ``…_V2_graph``.
    A specified version always wins, even if that slice is missing from
    the document — graph FQNs stay pinned to the loaded version.
    """
    keys = [coerce_version_key(key) for key in versions]
    keys = [key for key in keys if key]
    requested_key = coerce_version_key(requested)
    if requested_key:
        return requested_key
    if not keys:
        return "1"
    return sorted(keys, key=_numeric_version_key, reverse=True)[0]


def version_document_slice(
    versions: Mapping[Any, Any],
    version_key: str,
) -> Dict[str, Any]:
    """Return the version payload for *version_key*, or ``{}`` if absent."""
    if version_key in versions:
        found = versions[version_key]
        return found if isinstance(found, dict) else {}
    for key, payload in versions.items():
        if coerce_version_key(key) == version_key:
            return payload if isinstance(payload, dict) else {}
    return {}


def loaded_version_document(
    versions: Mapping[Any, Any],
    requested: Any = None,
) -> tuple[str, Dict[str, Any]]:
    """Return ``(version_key, slice)`` for a load, pinned to *requested*."""
    selected = select_loaded_version(versions, requested=requested)
    return selected, version_document_slice(versions, selected)


def resolve_domain_slice(domain_data: Dict[str, Any]) -> Dict[str, Any]:
    """Return the active ontology, assignment, and related fields for API/query use.

    For versioned documents, uses the highest numeric version key (same rule as
    :func:`get_domain_info`). For legacy documents, reads top-level keys.

    Returns:
        Dict with keys: ``version``, ``ontology``, ``assignment``, ``constraints``, ``swrl_rules``.
    """
    info = domain_data.get("info", {})
    version_key: str
    ontology: Dict[str, Any]
    assignment: Dict[str, Any]

    if "versions" in domain_data:
        versions = domain_data["versions"]
        if versions:
            version_key = select_loaded_version(versions)
            vd = version_document_slice(versions, version_key)
            ontology = vd.get("ontology", {})
            assignment = vd.get("assignment", vd.get("mapping", {}))
            constraints = vd.get("constraints", domain_data.get("constraints", []))
            swrl_rules = vd.get("swrl_rules", domain_data.get("swrl_rules", []))
        else:
            version_key = "1"
            ontology = {}
            assignment = {}
            constraints = domain_data.get("constraints", [])
            swrl_rules = domain_data.get("swrl_rules", [])
    else:
        version_key = str(info.get("version", "1"))
        ontology = domain_data.get("ontology", {})
        assignment = domain_data.get("assignment", domain_data.get("mapping", {}))
        constraints = domain_data.get("constraints", [])
        swrl_rules = domain_data.get("swrl_rules", [])

    return {
        "version": version_key,
        "ontology": ontology,
        "assignment": assignment,
        "constraints": constraints,
        "swrl_rules": swrl_rules,
    }


def get_domain_info(domain_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract domain information and statistics.

    Supports both new versioned format and legacy flat format.
    """
    info = domain_data.get("info", {})
    sl = resolve_domain_slice(domain_data)
    version = sl["version"]
    ontology = sl["ontology"]
    assignment = sl["assignment"]

    return {
        "name": info.get("name", "Untitled"),
        "description": info.get("description", ""),
        "uri": info.get("uri", ""),
        "version": version,
        "status": (info.get("status") or "DRAFT"),
        "author": info.get("author", ""),
        "graph_backend": normalize_graph_backend(info.get("graph_backend")),
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
