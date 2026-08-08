"""Tests for the Neo4j admin inventory methods (P5 Settings → Neo4j tabs).

``Neo4jReadOps.list_labels`` (Objects tab: graphs + node/edge counts) and
``Neo4jReadOps.list_databases`` (P4 DB selector) are exercised against a mocked
connection whose ``run`` is routed per-Cypher, so no live Neo4j is needed.
"""

from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.unit


def _readops(router):
    """Neo4jReadOps whose connection.run dispatches through *router(cypher, params)*."""
    from back.core.graphdb.neo4j.Neo4jReadOps import Neo4jReadOps

    conn = MagicMock()
    conn.run = MagicMock(side_effect=lambda cypher, **params: router(cypher, params))
    return Neo4jReadOps(conn), conn


class TestListLabels:
    def test_parses_constraints_into_graphs_with_counts(self):
        def router(cypher: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
            if cypher.startswith("SHOW CONSTRAINTS"):
                return [
                    {"labels": ["InsurBricks_V1"]},
                    {"labels": ["ContactCenter_V2"]},
                ]
            if "count(n) AS nodes" in cypher:
                return [{"nodes": 60}] if "InsurBricks_V1" in cypher else [{"nodes": 15769}]
            if "count(r) AS edges" in cypher:
                return [{"edges": 102}] if "InsurBricks_V1" in cypher else [{"edges": 40000}]
            return []

        r, _ = _readops(router)
        graphs = r.list_labels()
        by_label = {g["label"]: g for g in graphs}
        assert set(by_label) == {"InsurBricks_V1", "ContactCenter_V2"}
        assert by_label["InsurBricks_V1"] == {
            "label": "InsurBricks_V1", "nodes": 60, "edges": 102
        }
        assert by_label["ContactCenter_V2"]["nodes"] == 15769

    def test_excludes_schema_label(self):
        def router(cypher: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
            if cypher.startswith("SHOW CONSTRAINTS"):
                return [{"labels": ["__GraphSchema"]}, {"labels": ["G1"]}]
            if "count(n) AS nodes" in cypher:
                return [{"nodes": 3}]
            if "count(r) AS edges" in cypher:
                return [{"edges": 1}]
            return []

        r, _ = _readops(router)
        labels = {g["label"] for g in r.list_labels()}
        assert "__GraphSchema" not in labels
        assert labels == {"G1"}

    def test_empty_when_no_constraints(self):
        r, _ = _readops(lambda cypher, params: [])
        assert r.list_labels() == []


class TestListDatabases:
    def test_filters_system_and_sorts(self):
        def router(cypher: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
            return [{"name": "neo4j"}, {"name": "system"}, {"name": "insurbricks"}]

        r, _ = _readops(router)
        assert r.list_databases() == ["insurbricks", "neo4j"]

    def test_returns_empty_on_error(self):
        # Aura free / Community may reject SHOW DATABASES → graceful fallback.
        def router(cypher: str, params: Dict[str, Any]):
            raise RuntimeError("SHOW DATABASES not permitted on this tier")

        r, _ = _readops(router)
        assert r.list_databases() == []
