"""Neo4j implementation of the :class:`GraphBuilder` capability.

Thin facade over the pure RDF-triple ↔ property-graph naming helpers
(``graph_model`` + ``Neo4jWriteOps.sanitise_label``) so callers outside
``back/core/graphdb/`` derive Neo4j-safe labels / identifiers without importing
``neo4j.*`` directly. No driver is touched here — these are deterministic,
side-effect-free string transforms.
"""

from __future__ import annotations

from back.core.graphdb.GraphBuilder import GraphBuilder


class Neo4jGraphBuilder(GraphBuilder):
    """Property-graph naming helpers for a Neo4j domain."""

    def sanitise_label(self, name: str) -> str:
        """Neo4j marker/node label for a graph (table) name."""
        from back.core.graphdb.neo4j.Neo4jWriteOps import sanitise_label

        return sanitise_label(name)

    def sanitise_ident(self, name: str) -> str:
        """Neo4j-safe identifier ``[A-Za-z0-9_]`` (backtick-free)."""
        from back.core.graphdb.neo4j.graph_model import _sanitise_ident

        return _sanitise_ident(name)

    def label_from_class_uri(self, class_uri: str) -> str:
        """Neo4j node label for an ``rdf:type`` object (class URI)."""
        from back.core.graphdb.neo4j.graph_model import label_from_class_uri

        return label_from_class_uri(class_uri)

    def reltype_from_predicate(self, predicate_uri: str) -> str:
        """Neo4j relationship type for an object-property predicate URI."""
        from back.core.graphdb.neo4j.graph_model import reltype_from_predicate

        return reltype_from_predicate(predicate_uri)
