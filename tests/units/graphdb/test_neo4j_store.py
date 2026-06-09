"""Tests for the Neo4j graph DB backend (Neo4jStore).

The neo4j driver is mocked — no live connection required. We exercise:
- Capability flags
- Construction (valid config, missing URI, bad auth method)
- Cypher emission for CRUD methods (assert against the captured query)
- Factory dispatch via GraphDBFactory
"""

import sys
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest


pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
#  Skip the whole module when the neo4j driver isn't installed in the test
#  environment — Neo4jStore.__init__ raises ImportError in that case and the
#  module-under-test imports `neo4j` at import time.
# ---------------------------------------------------------------------------

try:
    import neo4j  # noqa: F401
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not NEO4J_AVAILABLE, reason="neo4j driver not installed (optional dep)"
)


# ---------------------------------------------------------------------------
#  Helpers
# ---------------------------------------------------------------------------

def _basic_config(**overrides: Any) -> Dict[str, Any]:
    cfg = {
        "uri": "neo4j+s://b4810af7.databases.neo4j.io",
        "database": "neo4j",
        "auth_method": "basic",
        "username": "neo4j",
        "password": "test-password-123",
    }
    cfg.update(overrides)
    return cfg


def _store(**overrides: Any):
    """Construct a Neo4jStore with mocked driver + capture _run calls."""
    from back.core.graphdb.neo4j.Neo4jStore import Neo4jStore

    s = Neo4jStore(db_name="testset", engine_config=_basic_config(**overrides))
    s._run = MagicMock(return_value=[])  # type: ignore[assignment]
    return s


# ---------------------------------------------------------------------------
#  Capability flags
# ---------------------------------------------------------------------------

class TestCapabilityFlags:
    def test_supports_cypher(self):
        s = _store()
        assert s.supports_cypher is True

    def test_supports_graph_model(self):
        s = _store()
        assert s.supports_graph_model is False  # flat-triple model in v1

    def test_query_dialect(self):
        s = _store()
        assert s.query_dialect == "cypher"


# ---------------------------------------------------------------------------
#  Construction & validation
# ---------------------------------------------------------------------------

class TestConstruction:
    def test_valid_config(self):
        from back.core.graphdb.neo4j.Neo4jStore import Neo4jStore

        store = Neo4jStore(db_name="dom_V1", engine_config=_basic_config())
        assert store.db_name == "dom_V1"
        assert store._uri == "neo4j+s://b4810af7.databases.neo4j.io"
        assert store._database == "neo4j"
        assert store._auth_method == "basic"

    def test_missing_uri_raises(self):
        from back.core.graphdb.neo4j.Neo4jStore import Neo4jStore

        cfg = _basic_config()
        cfg["uri"] = ""
        with pytest.raises(ValueError, match="uri"):
            Neo4jStore(engine_config=cfg)

    def test_unsupported_auth_method_raises(self):
        from back.core.graphdb.neo4j.Neo4jStore import Neo4jStore

        with pytest.raises(ValueError, match="auth_method"):
            Neo4jStore(engine_config=_basic_config(auth_method="kerberos"))

    def test_database_defaults_to_neo4j(self):
        from back.core.graphdb.neo4j.Neo4jStore import Neo4jStore

        cfg = _basic_config()
        cfg["database"] = ""
        store = Neo4jStore(engine_config=cfg)
        assert store._database == "neo4j"


# ---------------------------------------------------------------------------
#  Schema sanitisation
# ---------------------------------------------------------------------------

class TestGetNodeTable:
    def test_alphanumeric_passthrough(self):
        assert _store().get_node_table("MyDomain_V1") == "MyDomain_V1"

    def test_non_alphanumeric_replaced(self):
        assert _store().get_node_table("foo-bar.baz") == "foo_bar_baz"


# ---------------------------------------------------------------------------
#  CRUD — assert Cypher emission shape
# ---------------------------------------------------------------------------

class TestCRUDCypher:
    def test_create_table_creates_constraint(self):
        s = _store()
        s.create_table("dom_V1")
        cypher = s._run.call_args.args[0]
        assert "CREATE CONSTRAINT" in cypher
        assert "triple_dom_V1_spo" in cypher
        assert "(:Triple:dom_V1)" in cypher.replace(" ", "")  # whitespace-tolerant
        assert "(t.subject, t.predicate, t.object) IS UNIQUE" in cypher

    def test_drop_table_drops_constraint_and_nodes(self):
        s = _store()
        s.drop_table("dom_V1")
        cyphers = [c.args[0] for c in s._run.call_args_list]
        assert any("DROP CONSTRAINT" in c for c in cyphers)
        assert any("DETACH DELETE" in c for c in cyphers)

    def test_insert_triples_uses_unwind_merge(self):
        s = _store()
        triples = [
            {"subject": "ex:a", "predicate": "ex:p", "object": "ex:b"},
            {"subject": "ex:b", "predicate": "ex:q", "object": "ex:c"},
        ]
        count = s.insert_triples("dom_V1", triples)
        assert count == 2
        cypher = s._run.call_args.args[0]
        assert "UNWIND $rows" in cypher
        assert "MERGE" in cypher
        # Verify the rows parameter shape
        rows = s._run.call_args.kwargs.get("rows", [])
        assert len(rows) == 2
        assert rows[0]["subject"] == "ex:a"

    def test_insert_triples_empty_returns_zero(self):
        s = _store()
        assert s.insert_triples("dom_V1", []) == 0
        s._run.assert_not_called()

    def test_count_triples_emits_count_cypher(self):
        s = _store()
        s._run.return_value = [{"cnt": 42}]
        assert s.count_triples("dom_V1") == 42
        cypher = s._run.call_args.args[0]
        assert "count(t) AS cnt" in cypher

    def test_table_exists_via_show_constraints(self):
        s = _store()
        s._run.return_value = [{"name": "triple_dom_V1_spo"}]
        assert s.table_exists("dom_V1") is True
        cypher = s._run.call_args.args[0]
        assert "SHOW CONSTRAINTS" in cypher

    def test_get_status_reports_format(self):
        s = _store()
        s._run.return_value = [{"cnt": 7}]
        status = s.get_status("dom_V1")
        assert status["format"] == "neo4j"
        assert status["count"] == 7
        assert status["path"] is None

    def test_execute_query_raises(self):
        s = _store()
        with pytest.raises(NotImplementedError):
            s.execute_query("MATCH (n) RETURN n")


# ---------------------------------------------------------------------------
#  Named queries — sanity that Cypher is emitted, not stubs
# ---------------------------------------------------------------------------

class TestNamedQueriesEmitCypher:
    def test_get_aggregate_stats_runs_cypher(self):
        s = _store()
        s._run.return_value = [
            {
                "total": 100,
                "distinct_subjects": 30,
                "distinct_predicates": 8,
                "type_assertion_count": 25,
                "label_count": 12,
            }
        ]
        stats = s.get_aggregate_stats("dom_V1")
        assert stats["total"] == 100
        assert stats["distinct_subjects"] == 30
        cypher = s._run.call_args.args[0]
        assert "MATCH (t:Triple:dom_V1)" in cypher
        assert "count(t) AS total" in cypher

    def test_find_subjects_by_type_paginates(self):
        s = _store()
        s._run.return_value = [{"subject": "ex:a"}, {"subject": "ex:b"}]
        result = s.find_subjects_by_type("dom_V1", "ex:Class", limit=10, offset=5)
        assert result == ["ex:a", "ex:b"]
        kwargs = s._run.call_args.kwargs
        assert kwargs["type_uri"] == "ex:Class"
        assert kwargs["limit"] == 10
        assert kwargs["offset"] == 5


# ---------------------------------------------------------------------------
#  Factory dispatch
# ---------------------------------------------------------------------------

class TestFactoryDispatch:
    def test_factory_routes_neo4j_engine(self):
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        factory = GraphDBFactory()
        domain = MagicMock()
        domain.info = {"name": "dom"}
        domain.current_version = "1"
        store = factory.create(
            domain, settings=None, engine="neo4j", engine_config=_basic_config()
        )
        assert store is not None
        assert store.__class__.__name__ == "Neo4jStore"
        assert store.db_name == "dom_V1"

    def test_factory_returns_none_on_missing_uri(self):
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        factory = GraphDBFactory()
        domain = MagicMock()
        domain.info = {"name": "dom"}
        domain.current_version = "1"
        bad_cfg = _basic_config()
        bad_cfg["uri"] = ""
        store = factory.create(
            domain, settings=None, engine="neo4j", engine_config=bad_cfg
        )
        assert store is None  # ValueError caught, logged, returns None

    def test_neo4j_available_flag(self):
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        assert GraphDBFactory.NEO4J_AVAILABLE is True


# ---------------------------------------------------------------------------
#  Reasoning translator wiring
# ---------------------------------------------------------------------------

class TestReasoningTranslator:
    def test_get_query_translator_returns_cypher_translator(self):
        from back.core.reasoning.SWRLFlatCypherTranslator import (
            SWRLFlatCypherTranslator,
        )

        s = _store()
        translator = s.get_query_translator("dom_V1")
        assert isinstance(translator, SWRLFlatCypherTranslator)
        assert translator.node_label == "dom_V1"

    def test_translator_methods_return_none_with_warning(self, caplog):
        from back.core.reasoning.SWRLFlatCypherTranslator import (
            SWRLFlatCypherTranslator,
        )

        t = SWRLFlatCypherTranslator(node_label="dom_V1")
        assert t.build_violation_sql("dom_V1", {}) is None
        assert t.build_antecedent_count_sql("dom_V1", {}) is None
        assert t.build_materialization_sql("dom_V1", {}) is None
        assert t.build_inference_sql("dom_V1", {}) is None
