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
    """Construct a Neo4jStore with the underlying connection's `run` mocked.

    Post-split (PR #47 Benoit review) the actual Cypher execution lives on
    :class:`Neo4jConnection`, not on the Store. Mocking ``s._connection.run``
    intercepts every Cypher call regardless of whether the read/write op
    helpers or the Store façade is the entry point.
    """
    from back.core.graphdb.neo4j.Neo4jStore import Neo4jStore

    s = Neo4jStore(db_name="testset", engine_config=_basic_config(**overrides))
    s._connection.run = MagicMock(return_value=[])  # type: ignore[assignment]
    # Legacy alias: tests that still reference ``s._run`` go through the
    # façade's delegator, which now points at the mocked connection.
    s._run = s._connection.run
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
        # Neo4j 5+ rejects multi-label CREATE CONSTRAINT; single backtick label.
        assert "(t:`dom_V1`)" in cypher
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
        assert "MATCH (t:`dom_V1`)" in cypher
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


# ---------------------------------------------------------------------------
#  Password sourcing — Databricks Apps secret vs local-dev fallback
# ---------------------------------------------------------------------------

class TestPasswordSourcing:
    """The Bolt password must come from the NEO4J_PASSWORD env var in prod
    (populated by a Databricks Apps secret resource bound in app.yaml) and
    fall back to engine_config['password'] only in local dev.
    """

    def test_helper_reports_secret_when_env_var_set(self, monkeypatch):
        from back.core.graphdb.neo4j.Neo4jStore import is_neo4j_password_from_secret

        monkeypatch.setenv("NEO4J_PASSWORD", "from-secret")
        assert is_neo4j_password_from_secret() is True

    def test_helper_reports_no_secret_when_env_var_blank(self, monkeypatch):
        from back.core.graphdb.neo4j.Neo4jStore import is_neo4j_password_from_secret

        monkeypatch.delenv("NEO4J_PASSWORD", raising=False)
        assert is_neo4j_password_from_secret() is False

    def test_resolve_auth_prefers_env_var(self, monkeypatch):
        monkeypatch.setenv("NEO4J_PASSWORD", "from-secret")
        s = _store(password="from-config")
        user, pwd = s._resolve_auth()
        assert user == "neo4j"
        assert pwd == "from-secret"

    def test_resolve_auth_falls_back_to_config_in_local_dev(self, monkeypatch):
        monkeypatch.delenv("NEO4J_PASSWORD", raising=False)
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        s = _store(password="from-config")
        user, pwd = s._resolve_auth()
        assert pwd == "from-config"

    def test_resolve_auth_raises_in_prod_without_env_var(self, monkeypatch):
        from back.core.errors import InfrastructureError

        monkeypatch.delenv("NEO4J_PASSWORD", raising=False)
        monkeypatch.setenv("DATABRICKS_APP_PORT", "8080")
        s = _store(password="from-config")  # config password ignored in prod
        with pytest.raises(InfrastructureError, match="NEO4J_PASSWORD"):
            s._resolve_auth()

    def test_resolve_auth_raises_when_no_credentials_anywhere(self, monkeypatch):
        from back.core.errors import ValidationError

        monkeypatch.delenv("NEO4J_PASSWORD", raising=False)
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        s = _store(password="")
        with pytest.raises(ValidationError, match="NEO4J_PASSWORD"):
            s._resolve_auth()

    def test_resolve_auth_raises_when_username_missing(self, monkeypatch):
        from back.core.errors import ValidationError

        monkeypatch.setenv("NEO4J_PASSWORD", "from-secret")
        s = _store(username="")
        with pytest.raises(ValidationError, match="username"):
            s._resolve_auth()


# ---------------------------------------------------------------------------
#  Cypher logging — every _run call emits an info-level summary
# ---------------------------------------------------------------------------

class TestCypherLogging:
    """Benoit's review (2026-06-18) asked that the Cypher emitted by every
    ``_run`` call be visible in the Databricks app logs at INFO level so
    operators can correlate UI actions with backend queries.
    """

    def test_normalise_collapses_whitespace(self):
        from back.core.graphdb.neo4j.Neo4jStore import _normalise_cypher_for_log

        flat = _normalise_cypher_for_log(
            "MATCH (t:`X`)\n  WHERE t.predicate = $rdf_type\n   RETURN t"
        )
        assert flat == "MATCH (t:`X`) WHERE t.predicate = $rdf_type RETURN t"

    def test_normalise_truncates_long_cypher(self):
        from back.core.graphdb.neo4j.Neo4jStore import _normalise_cypher_for_log

        long = "MATCH (t:`X`) RETURN t " + ("x" * 4000)
        out = _normalise_cypher_for_log(long)
        assert out.endswith("… (truncated)")
        assert len(out) < len(long)

    def test_run_emits_info_log_with_cypher_and_metrics(self, caplog):
        """The real `_run` (no MagicMock) emits a single INFO line per call."""
        from back.core.graphdb.neo4j.Neo4jStore import Neo4jStore

        # Build a store without the MagicMock helper so `_run` runs for real,
        # then stub `get_connection` to return a fake driver capturing the call.
        s = Neo4jStore(db_name="testset", engine_config=_basic_config())

        class _FakeResult:
            def __iter__(self):
                return iter([{"subject": "ex:a"}, {"subject": "ex:b"}])

        class _FakeSession:
            def __enter__(self):
                return self
            def __exit__(self, *a):
                return False
            def run(self, cypher, **params):
                return _FakeResult()

        class _FakeDriver:
            def session(self, **kw):
                return _FakeSession()

        s._driver = _FakeDriver()

        import logging
        # The Cypher INFO log is emitted from Neo4jConnection.run (post-split,
        # PR #47 Benoit review). The Store façade now delegates via _run.
        # NOTE: `get_logger` (back.core.logging.LogManager) rewrites the
        # ``back.`` prefix → ``ontobricks.`` so the real logger name is
        # ``ontobricks.core.graphdb.neo4j.Neo4jConnection``.
        # LogManager.setup() sets propagate=False on the ontobricks tree, so
        # attach caplog's handler directly (same pattern as TaskManager tests).
        target = logging.getLogger("ontobricks.core.graphdb.neo4j.Neo4jConnection")
        target.addHandler(caplog.handler)
        target.setLevel(logging.INFO)
        try:
            rows = s._run("MATCH (t:`X`) WHERE t.subject = $s RETURN t", s="ex:a")
        finally:
            target.removeHandler(caplog.handler)

        assert len(rows) == 2
        info_records = [r for r in caplog.records if r.levelname == "INFO"]
        cypher_logs = [r for r in info_records if r.getMessage().startswith("Cypher (")]
        assert len(cypher_logs) == 1, (
            "Expected exactly one INFO log line starting with 'Cypher (', "
            f"got {len(cypher_logs)}"
        )
        msg = cypher_logs[0].getMessage()
        assert "2 rows" in msg
        assert "MATCH (t:`X`) WHERE t.subject = $s RETURN t" in msg
        # Critical: the bound parameter value must not appear in the INFO log
        assert "ex:a" not in msg, "Bound params leaked into INFO log"
