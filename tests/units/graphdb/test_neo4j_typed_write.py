"""Tests for the typed property-graph write path (Neo4jWriteOps).

The neo4j driver is mocked — we assert on the Cypher emitted through the
connection. Verifies that inserts MERGE nodes on uri, set labels/properties,
create relationships, and persist the reverse schema map; and that drops /
deletes target the right graph.
"""

from typing import Any, Dict, List
from unittest.mock import MagicMock, call

import pytest

pytestmark = pytest.mark.unit

try:
    import neo4j  # noqa: F401
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not NEO4J_AVAILABLE, reason="neo4j driver not installed (optional dep)"
)

NS = "https://databricks-ontology.com/InsurBricks/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"


def _writeops():
    """Neo4jWriteOps wired to a mocked connection that records every call."""
    from back.core.graphdb.neo4j.Neo4jWriteOps import Neo4jWriteOps

    conn = MagicMock()
    conn.run = MagicMock(return_value=[])
    w = Neo4jWriteOps(conn)
    return w, conn


def _all_cypher(conn) -> List[str]:
    return [c.args[0] for c in conn.run.call_args_list if c.args]


def _marie_triples() -> List[Dict[str, str]]:
    c = f"{NS}Customer/CUST-1007"
    return [
        {"subject": c, "predicate": RDF_TYPE, "object": f"{NS}Customer"},
        {"subject": c, "predicate": RDFS_LABEL, "object": "Marie Lefebvre"},
        {"subject": c, "predicate": f"{NS}city", "object": "Paris"},
        {"subject": c, "predicate": f"{NS}holds", "object": f"{NS}Policy/POL-20008"},
    ]


class TestCreateDrop:
    def test_create_table_makes_uri_constraint(self):
        w, conn = _writeops()
        w.create_table("InsurBricks_V1")
        cy = " ".join(_all_cypher(conn))
        assert "CREATE CONSTRAINT" in cy and "n.uri IS UNIQUE" in cy
        assert "`InsurBricks_V1`" in cy

    def test_drop_table_detach_deletes_marker_and_schema(self):
        w, conn = _writeops()
        w.drop_table("InsurBricks_V1")
        cy = " ".join(_all_cypher(conn))
        assert "DROP CONSTRAINT" in cy
        assert "MATCH (n:`InsurBricks_V1`) DETACH DELETE n" in cy
        # schema map drop also fires (via Neo4jSchemaMap)
        assert "__GraphSchema" in cy


class TestInsertTriples:
    def test_returns_triple_count(self):
        w, _ = _writeops()
        assert w.insert_triples("G", _marie_triples()) == 4

    def test_merges_nodes_on_uri(self):
        w, conn = _writeops()
        w.insert_triples("G", _marie_triples())
        cy = " ".join(_all_cypher(conn))
        assert "MERGE (n:`G` {uri: r.uri})" in cy

    def test_sets_class_label_on_node(self):
        w, conn = _writeops()
        w.insert_triples("G", _marie_triples())
        cy = " ".join(_all_cypher(conn))
        # Customer label applied via SET n:`Customer`
        assert "`Customer`" in cy

    def test_creates_relationship_for_uri_object(self):
        w, conn = _writeops()
        w.insert_triples("G", _marie_triples())
        cy = " ".join(_all_cypher(conn))
        assert "MERGE (s)-[:`holds`]->(o)" in cy

    def test_relationships_are_batched_by_reltype(self):
        # Phase 2 must be one UNWIND per reltype, not one round-trip per edge
        # (R1 perf: O(distinct reltypes), not O(edges)).
        w, conn = _writeops()
        c = f"{NS}Customer/CUST-1007"
        triples = [
            {"subject": c, "predicate": RDF_TYPE, "object": f"{NS}Customer"},
            {"subject": c, "predicate": f"{NS}holds", "object": f"{NS}Policy/POL-1"},
            {"subject": c, "predicate": f"{NS}holds", "object": f"{NS}Policy/POL-2"},
            {"subject": c, "predicate": f"{NS}holds", "object": f"{NS}Policy/POL-3"},
        ]
        w.insert_triples("G", triples)
        edge_calls = [
            call
            for call in conn.run.call_args_list
            if call.args and "MERGE (s)-[:`holds`]->(o)" in call.args[0]
        ]
        # Three `holds` edges collapse into a single UNWIND statement.
        assert len(edge_calls) == 1
        assert "UNWIND $rows AS r" in edge_calls[0].args[0]
        assert len(edge_calls[0].kwargs["rows"]) == 3

    def test_edge_object_only_node_is_created(self):
        # An object that never appears as a subject must still get its node
        # MERGE-created in phase 1 so the phase-2 edge MATCH resolves (R3).
        w, conn = _writeops()
        c = f"{NS}Customer/CUST-1007"
        pol = f"{NS}Policy/POL-20008"
        # `pol` only appears as an object here — no triple has it as subject.
        w.insert_triples("G", [{"subject": c, "predicate": f"{NS}holds", "object": pol}])
        node_rows = [
            call.kwargs["rows"]
            for call in conn.run.call_args_list
            if call.args and "MERGE (n:`G` {uri: r.uri})" in call.args[0]
        ]
        merged_uris = {row["uri"] for rows in node_rows for row in rows}
        assert c in merged_uris and pol in merged_uris

    def test_persists_reverse_schema_map(self):
        w, conn = _writeops()
        w.insert_triples("G", _marie_triples())
        cy = " ".join(_all_cypher(conn))
        assert "__GraphSchema" in cy  # merge_and_save fired

    def test_empty_triples_noop(self):
        w, conn = _writeops()
        assert w.insert_triples("G", []) == 0
        assert conn.run.call_count == 0

    def test_idempotent_uses_merge_not_create(self):
        # Re-running the same batch must not CREATE duplicate nodes/edges.
        w, conn = _writeops()
        w.insert_triples("G", _marie_triples())
        cy = " ".join(_all_cypher(conn))
        assert "CREATE (" not in cy  # only MERGE for nodes/edges
        assert "MERGE" in cy


class TestDeleteTriples:
    def test_removes_label_for_rdf_type(self):
        w, conn = _writeops()
        c = f"{NS}Customer/CUST-1007"
        w.delete_triples("G", [{"subject": c, "predicate": RDF_TYPE, "object": f"{NS}Customer"}])
        cy = " ".join(_all_cypher(conn))
        assert "REMOVE n:`Customer`" in cy

    def test_deletes_relationship_for_uri_object(self):
        w, conn = _writeops()
        c = f"{NS}Customer/CUST-1007"
        w.delete_triples("G", [{"subject": c, "predicate": f"{NS}holds", "object": f"{NS}Policy/POL-20008"}])
        cy = " ".join(_all_cypher(conn))
        assert "[r:`holds`]" in cy and "DELETE r" in cy

    def test_unsets_property_for_literal(self):
        w, conn = _writeops()
        c = f"{NS}Customer/CUST-1007"
        w.delete_triples("G", [{"subject": c, "predicate": f"{NS}city", "object": "Paris"}])
        cy = " ".join(_all_cypher(conn))
        assert "REMOVE n.`city`" in cy


class TestCohortDelete:
    def test_detach_deletes_by_uri_prefix(self):
        w, conn = _writeops()
        w.delete_cohort_triples("G", f"{NS}cohort/", f"{NS}inCohort")
        cy = " ".join(_all_cypher(conn))
        assert "n.uri STARTS WITH $prefix" in cy and "DETACH DELETE n" in cy
