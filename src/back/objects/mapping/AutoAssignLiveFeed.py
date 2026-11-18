"""Per-item live results for a running batch Auto-Map task."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Set


def _entity_uri(mapping: Dict[str, Any]) -> str:
    return mapping.get("ontology_class") or mapping.get("class_uri", "")


def _relationship_uri(mapping: Dict[str, Any]) -> str:
    return mapping.get("property", "")


class AutoAssignLiveFeed:
    """Accumulate one batch run's agent mappings and republish them on ``task.result``.

    ``Task.to_dict`` serializes ``result`` while the task is still running, so
    the Mapping page poll can paint each entity / relationship as soon as the
    agent submits it instead of only when the whole run completes.

    ``entities`` / ``relationships`` hold the mappings of finished chunks (what
    gets persisted); the live view additionally carries the in-flight chunk's
    submissions, and is rolled back to the committed state when a chunk fails.
    """

    def __init__(self, task: Any, *, entities_total: int, relationships_total: int):
        self._task = task
        self._entities_total = entities_total
        self._relationships_total = relationships_total
        self.entities: Dict[str, Dict[str, Any]] = {}
        self.relationships: Dict[str, Dict[str, Any]] = {}
        self._live_entities: Dict[str, Dict[str, Any]] = {}
        self._live_relationships: Dict[str, Dict[str, Any]] = {}
        self._agent_steps: List[Any] = []

    def item_callback(
        self, entity_uris: Set[str], relationship_uris: Set[str]
    ) -> Callable[[str, Dict[str, Any]], None]:
        """Return the agent ``on_mapping`` hook scoped to one chunk's URIs."""

        def on_mapping(kind: str, mapping: Dict[str, Any]) -> None:
            if kind == "entity":
                uri = _entity_uri(mapping)
                if uri and uri in entity_uris:
                    self._live_entities[uri] = mapping
                    self.publish()
            elif kind == "relationship":
                uri = _relationship_uri(mapping)
                if uri and uri in relationship_uris:
                    self._live_relationships[uri] = mapping
                    self.publish()

        return on_mapping

    def commit_chunk(
        self, agent_result: Any, entity_uris: Set[str], relationship_uris: Set[str]
    ) -> None:
        """Keep the chunk's final mappings (only for URIs the chunk owned)."""
        for em in agent_result.entity_mappings:
            uri = _entity_uri(em)
            if uri and uri in entity_uris:
                self.entities[uri] = em
        for rm in agent_result.relationship_mappings:
            uri = _relationship_uri(rm)
            if uri and uri in relationship_uris:
                self.relationships[uri] = rm
        self._resync_live()

    def rollback_chunk(self) -> None:
        """Drop in-flight submissions of a chunk that failed (never persisted)."""
        self._resync_live()
        self.publish()

    def _resync_live(self) -> None:
        self._live_entities = dict(self.entities)
        self._live_relationships = dict(self.relationships)

    def publish(self, agent_steps: List[Any] | None = None) -> None:
        """Expose live counts and mappings to the task poll."""
        if agent_steps is not None:
            self._agent_steps = agent_steps
        self._task.result = {
            "live_stats": True,
            "entities_assigned": len(self._live_entities),
            "entities_total": self._entities_total,
            "relationships_assigned": len(self._live_relationships),
            "relationships_total": self._relationships_total,
            "agent_steps": self._agent_steps,
            "live_entity_mappings": list(self._live_entities.values()),
            "live_relationship_mappings": list(self._live_relationships.values()),
        }
