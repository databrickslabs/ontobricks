"""Generator agents for the mapping-PGE pipeline.

Each Generator is a narrow tool-calling agent that maps ONE ontology item
(class or relationship) at a time. The orchestrator (Sprint 7) calls them
per-item with a filtered slice of the Planner's :class:`SourceModel` — the
Generators never see the full ontology or full metadata, keeping each
decision cheap and local.

* Sprint 4 — :mod:`agents.agent_mapping_pge.generators.entity`.
* Sprint 5 — :mod:`agents.agent_mapping_pge.generators.relationship`.
"""

from agents.agent_mapping_pge.generators.entity import (
    EntityGenResult,
    EntityGenStep,
    run_entity_generator,
)
from agents.agent_mapping_pge.generators.relationship import (
    RelationshipGenResult,
    RelationshipGenStep,
    run_relationship_generator,
)

__all__ = [
    "EntityGenResult",
    "EntityGenStep",
    "run_entity_generator",
    "RelationshipGenResult",
    "RelationshipGenStep",
    "run_relationship_generator",
]
