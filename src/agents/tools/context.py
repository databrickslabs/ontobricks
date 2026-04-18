"""
Unified ToolContext shared by all agents.

Each agent populates the fields it needs; unused fields stay at their
default values.
"""

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class ToolContext:
    """Shared runtime context injected into every tool call."""

    # Common fields (used by all agents)
    host: str
    token: str
    metadata: dict = field(default_factory=dict)

    # OWL generator fields — registry-based document path
    registry: Optional[dict] = None
    domain_name: Optional[str] = None
    domain_folder: Optional[str] = None
    domain_version: Optional[str] = None

    # Auto-mapping fields
    client: Any = None
    ontology: Optional[dict] = None
    entity_mappings: list = field(default_factory=list)
    relationships: list = field(default_factory=list)
    documents: list = field(
        default_factory=list
    )  # Pre-loaded doc content: [{"name": str, "content": str}]

    # Auto-icon-assign fields
    icon_results: dict = field(default_factory=dict)

    # Ontology assistant fields – mutable references to the live ontology lists
    ontology_classes: list = field(default_factory=list)
    ontology_properties: list = field(default_factory=list)
    ontology_base_uri: str = ""
    ontology_dirty: bool = False
