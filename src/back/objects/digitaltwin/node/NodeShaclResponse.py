"""Response contract of the entity SHACL facet check (``/nodes/shacl``)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class NodeShaclResponse(BaseModel):
    """SHACL facets of one entity, as produced by :class:`NodeShaclService`."""

    success: bool
    entity_uri: str = ""
    entity_local_id: str = ""
    entity_label: Optional[str] = None
    classes: List[Dict[str, str]] = Field(
        default_factory=list, description="The entity's classes: {name, uri}."
    )
    lineage: List[str] = Field(
        default_factory=list, description="Class names, own class first, then ancestors."
    )
    summary: Dict[str, Any] = Field(
        default_factory=dict,
        description="Count per status plus total and fail_by_severity.",
    )
    facets: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="One entry per applicable shape: shape_id, label, category, "
        "severity, target_class, inherited_from, property_path, property_uri, "
        "shacl_type, parameters, constraint, status (pass | fail | "
        "not_applicable | not_evaluated | error), values, values_truncated, "
        "details and message.",
    )
    message: Optional[str] = None
