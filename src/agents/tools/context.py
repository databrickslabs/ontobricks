"""
Unified ToolContext shared by all agents.

Each agent populates the fields it needs; unused fields stay at their
default values.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from agents.agent_mapping_pge.contracts import EvalReport, SourceModel


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

    # SQL Warehouse id — enables binary document parsing (ai_parse_document).
    # Empty string means no warehouse configured; binary docs are then skipped.
    warehouse_id: str = ""

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

    # Graph Chat agent fields (``agent_dtwin_chat``) – loopback REST caller
    dtwin_base_url: str = ""
    dtwin_session_cookies: dict = field(default_factory=dict)
    dtwin_session_headers: dict = field(default_factory=dict)
    dtwin_registry_params: dict = field(default_factory=dict)
    dtwin_domain_name: str = ""
    dtwin_ontology_labels: dict = field(default_factory=dict)  # uri/name → display label
    dtwin_class_actions: dict = field(default_factory=dict)
    pending_action: dict | None = None

    # Mapping PGE planner output (``agent_mapping_pge``) – populated by the
    # ``submit_source_model`` terminal tool. Forward-ref string typing avoids a
    # circular import between ``agents.tools`` and ``agents.agent_mapping_pge``.
    source_model: Optional["SourceModel"] = None

    # Mapping PGE semantic critic output (``agent_mapping_pge``) – populated by
    # the ``submit_evaluation`` terminal tool of the Sprint 6 Critic agent.
    # Same forward-ref pattern as ``source_model`` to avoid a circular import.
    semantic_eval_report: Optional["EvalReport"] = None
