"""Generator agents for the mapping-PGE pipeline.

Each Generator is a narrow tool-calling agent that maps ONE ontology item
(class or relationship) at a time. The orchestrator (Sprint 7) calls them
per-item with a filtered slice of the Planner's :class:`SourceModel` — the
Generators never see the full ontology or full metadata, keeping each
decision cheap and local.

* Sprint 4 — :mod:`agents.agent_mapping_pge.generators.entity` (this sprint).
* Sprint 5 — relationship generator (not yet implemented).
"""
