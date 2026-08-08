"""Tests for per-backend nested graph_engine_config (lakebase / neo4j / lakehouse)."""

from __future__ import annotations

from back.core.graphdb.engine_config import (
    is_nested_graph_engine_config,
    lakebase_section,
    lakehouse_section,
    list_neo4j_connections,
    neo4j_section,
    normalize_graph_engine_config,
    resolve_lakehouse_warehouse_id,
    resolve_neo4j_connection,
)
from back.core.graphdb.lakebase.LakebaseBase import resolve_postgres_database_override
from back.core.graphdb.neo4j.Neo4jConnection import resolve_neo4j_database


class TestNormalizeGraphEngineConfig:
    def test_empty(self):
        assert normalize_graph_engine_config(None) == {
            "lakebase": {},
            "neo4j": {},
            "lakehouse": {},
        }
        assert normalize_graph_engine_config({}) == {
            "lakebase": {},
            "neo4j": {},
            "lakehouse": {},
        }

    def test_already_nested(self):
        raw = {
            "lakebase": {"database": "analytics", "schema": "ontobricks_graph"},
            "neo4j": {"uri": "bolt://x", "database": "neo4j"},
            "lakehouse": {"warehouse_id": "wh-123"},
        }
        out = normalize_graph_engine_config(raw)
        assert out["lakebase"]["database"] == "analytics"
        assert out["neo4j"]["uri"] == "bolt://x"
        assert out["neo4j"]["database"] == "neo4j"
        assert out["lakehouse"]["warehouse_id"] == "wh-123"
        assert "uri" not in out["lakebase"]
        assert "schema" not in out["neo4j"]
        assert "warehouse_id" not in out["lakebase"]

    def test_flat_split_no_collision(self):
        raw = {
            "database": "analytics",
            "schema": "ontobricks_graph",
            "sync_mode": "app_managed",
            "uri": "neo4j+s://aura.example",
            "neo4j_database": "neo4j",
            "username": "neo4j",
            "password": "secret",
            "warehouse_id": "wh-abc",
        }
        out = normalize_graph_engine_config(raw)
        assert out["lakebase"]["database"] == "analytics"
        assert out["lakebase"]["schema"] == "ontobricks_graph"
        assert out["neo4j"]["uri"] == "neo4j+s://aura.example"
        assert out["neo4j"]["database"] == "neo4j"
        assert out["neo4j"]["username"] == "neo4j"
        assert "password" in out["neo4j"]
        assert out["lakehouse"]["warehouse_id"] == "wh-abc"
        assert "uri" not in out["lakebase"]
        assert "neo4j_database" not in out["neo4j"]

    def test_flat_polluted_database_neo4j_goes_to_neo4j_only(self):
        raw = {
            "uri": "bolt://x",
            "database": "neo4j",
            "schema": "ontobricks_graph",
        }
        out = normalize_graph_engine_config(raw)
        assert "database" not in out["lakebase"]
        assert out["neo4j"]["database"] == "neo4j"
        assert out["lakebase"]["schema"] == "ontobricks_graph"
        assert out["lakehouse"] == {}

    def test_sections_helpers(self):
        raw = {
            "lakebase": {"database": "lb"},
            "neo4j": {"uri": "bolt://x"},
            "lakehouse": {"warehouse_id": "wh-1"},
        }
        assert lakebase_section(raw)["database"] == "lb"
        assert neo4j_section(raw)["uri"] == "bolt://x"
        assert lakehouse_section(raw)["warehouse_id"] == "wh-1"
        assert resolve_lakehouse_warehouse_id(raw) == "wh-1"
        assert is_nested_graph_engine_config(raw)


class TestResolveWithNested:
    def test_postgres_from_nested(self):
        cfg = {
            "lakebase": {"database": "analytics"},
            "neo4j": {"uri": "bolt://x", "database": "neo4j"},
            "lakehouse": {"warehouse_id": "wh-x"},
        }
        assert resolve_postgres_database_override(cfg) == "analytics"

    def test_neo4j_from_nested_uses_database(self):
        cfg = {
            "lakebase": {"database": "analytics"},
            "neo4j": {"uri": "bolt://x", "database": "mydb"},
            "lakehouse": {},
        }
        assert resolve_neo4j_database(cfg) == "mydb"

    def test_neo4j_section_alone(self):
        assert resolve_neo4j_database({"database": "mydb", "uri": "bolt://x"}) == "mydb"

    def test_flat_legacy_still_safe(self):
        flat = {"database": "neo4j", "uri": "bolt://x", "schema": "g"}
        assert resolve_postgres_database_override(flat) == ""
        assert resolve_neo4j_database(flat) == "neo4j"


class TestNeo4jNamedConnections:
    def test_list_empty_when_only_flat_keys(self):
        """Flat legacy uri/username must NOT become a synthetic connection."""
        raw = {
            "neo4j": {
                "uri": "bolt://x",
                "database": "neo4j",
                "username": "neo4j",
                "secret_scope": "s",
                "secret_key": "k",
            }
        }
        assert list_neo4j_connections(raw) == []
        assert resolve_neo4j_connection(raw, "default") == {}

    def test_list_and_resolve_by_name(self):
        raw = {
            "neo4j": {
                "connections": [
                    {
                        "name": "Aura Prod",
                        "uri": "neo4j+s://a.example",
                        "database": "neo4j",
                        "username": "neo4j",
                        "secret_scope": "ontobricks",
                        "secret_key": "pw",
                        "encrypted": True,
                        "auth_method": "databricks_secret",
                    },
                    {
                        "name": "Lab",
                        "uri": "bolt://lab:7687",
                        "neo4j_database": "graph",
                        "username": "neo4j",
                    },
                ]
            }
        }
        conns = list_neo4j_connections(raw)
        assert [c["name"] for c in conns] == ["Aura Prod", "Lab"]
        assert conns[1]["database"] == "graph"
        assert "neo4j_database" not in conns[1]
        aura = resolve_neo4j_connection(raw, "Aura Prod")
        assert aura["uri"] == "neo4j+s://a.example"
        assert aura["secret_scope"] == "ontobricks"
        assert resolve_neo4j_connection(raw, "missing") == {}

    def test_duplicate_names_keep_first(self):
        raw = {
            "neo4j": {
                "connections": [
                    {"name": "A", "uri": "bolt://1"},
                    {"name": "A", "uri": "bolt://2"},
                ]
            }
        }
        conns = list_neo4j_connections(raw)
        assert len(conns) == 1
        assert conns[0]["uri"] == "bolt://1"

    def test_normalize_keeps_connections(self):
        raw = {
            "neo4j": {
                "connections": [{"name": "X", "uri": "bolt://x", "database": "db1"}]
            }
        }
        out = normalize_graph_engine_config(raw)
        assert out["neo4j"]["connections"][0]["name"] == "X"
        assert out["neo4j"]["connections"][0]["database"] == "db1"
