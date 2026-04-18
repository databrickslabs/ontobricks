"""Tests for the Digital Twin API module (api.routers.digitaltwin).

Covers helper functions, Pydantic models, and endpoint behavior
with mocked dependencies.
"""

import pytest
from unittest.mock import MagicMock, patch
from back.core.helpers import (
    effective_view_table,
    effective_graph_name,
    sql_escape,
)
from api.routers.digitaltwin import (
    _resolve_registry,
    _extract_local_id,
    _expand_uri_aliases,
)
from back.core.triplestore import TripleStoreBackend


# ---------------------------------------------------------------------------
# _effective_view_table / _effective_graph_name
# ---------------------------------------------------------------------------


class TestEffectiveViewTable:
    def test_fully_qualified(self):
        domain = MagicMock()
        domain.delta = {"catalog": "cat", "schema": "sch", "table_name": "triples"}
        domain.info = {"name": ""}
        domain.current_version = "1"
        assert effective_view_table(domain) == "cat.sch.triples"

    def test_partial_with_fallback(self):
        domain = MagicMock()
        domain.delta = {"catalog": "", "schema": "", "table_name": ""}
        domain.info = {"name": ""}
        domain.current_version = "1"
        settings = MagicMock()
        settings.databricks_triplestore_table = "fallback.table"
        assert effective_view_table(domain, settings) == "fallback.table"

    def test_no_settings(self):
        domain = MagicMock()
        domain.delta = {"catalog": "c", "schema": "s", "table_name": "t"}
        domain.info = {"name": ""}
        domain.current_version = "1"
        assert effective_view_table(domain) == "c.s.t"


class TestEffectiveGraphName:
    def test_from_domain_name(self):
        domain = MagicMock()
        domain.info = {"name": "MyDomain"}
        domain.current_version = "1"
        assert effective_graph_name(domain) == "MyDomain_V1"

    def test_default(self):
        domain = MagicMock()
        domain.info = {}
        domain.current_version = "1"
        assert effective_graph_name(domain) == "ontobricks_V1"


# ---------------------------------------------------------------------------
# _resolve_registry
# ---------------------------------------------------------------------------


class TestResolveRegistry:
    def _make_cfg(self, catalog="", schema="", volume=""):
        from back.objects.registry import RegistryCfg

        return RegistryCfg(catalog=catalog, schema=schema, volume=volume)

    @patch("back.objects.registry.RegistryCfg.from_session")
    def test_explicit_params_override(self, mock_from_session):
        mock_from_session.return_value = self._make_cfg(
            "session_cat",
            "session_sch",
            "session_vol",
        )
        result = _resolve_registry(
            MagicMock(),
            MagicMock(),
            registry_catalog="override_cat",
            registry_schema="override_sch",
            registry_volume="override_vol",
        )
        assert result["catalog"] == "override_cat"
        assert result["schema"] == "override_sch"
        assert result["volume"] == "override_vol"

    @patch("back.objects.registry.RegistryCfg.from_session")
    def test_falls_back_to_session(self, mock_from_session):
        mock_from_session.return_value = self._make_cfg(
            "sess_cat",
            "sess_sch",
            "sess_vol",
        )
        result = _resolve_registry(MagicMock(), MagicMock())
        assert result["catalog"] == "sess_cat"
        assert result["schema"] == "sess_sch"
        assert result["volume"] == "sess_vol"

    @patch("back.objects.registry.RegistryCfg.from_session")
    def test_default_volume_when_key_missing(self, mock_from_session):
        mock_from_session.return_value = self._make_cfg("c", "s", "")
        result = _resolve_registry(MagicMock(), MagicMock())
        assert result["volume"] == ""

    @patch("back.objects.registry.RegistryCfg.from_session")
    def test_empty_volume_stays_empty(self, mock_from_session):
        mock_from_session.return_value = self._make_cfg("c", "s", "")
        result = _resolve_registry(MagicMock(), MagicMock())
        assert result["volume"] == ""

    @patch("back.objects.registry.RegistryCfg.from_session")
    def test_explicit_volume_overrides(self, mock_from_session):
        mock_from_session.return_value = self._make_cfg("c", "s", "v")
        result = _resolve_registry(MagicMock(), MagicMock(), registry_volume="custom")
        assert result["volume"] == "custom"


# ---------------------------------------------------------------------------
# _extract_local_id
# ---------------------------------------------------------------------------


class TestExtractLocalId:
    def test_hash_separator(self):
        assert _extract_local_id("http://example.org#Customer") == "Customer"

    def test_slash_separator(self):
        assert _extract_local_id("http://example.org/ontology/Customer") == "Customer"

    def test_nested_slash(self):
        assert (
            _extract_local_id("https://ontobricks.com/ontology/Customer/CUST001")
            == "CUST001"
        )

    def test_no_separator(self):
        assert _extract_local_id("Customer") == "Customer"

    def test_trailing_separator(self):
        uri = "http://example.org/Customer"
        assert _extract_local_id(uri) == "Customer"


# ---------------------------------------------------------------------------
# _expand_uri_aliases
# ---------------------------------------------------------------------------


class TestExpandUriAliases:
    def test_empty_set(self):
        store = MagicMock()
        result = _expand_uri_aliases(store, "table", set())
        assert result == set()

    def test_finds_aliases(self):
        store = MagicMock()
        store.find_subjects_by_patterns.return_value = {
            "http://ex.org/Customer/CUST001",
            "http://ex.org/CUST001",
        }
        uris = {"http://ex.org/Customer/CUST001"}
        result = _expand_uri_aliases(store, "triples", uris)
        assert "http://ex.org/CUST001" in result
        assert "http://ex.org/Customer/CUST001" in result

    def test_no_new_aliases(self):
        store = MagicMock()
        store.find_subjects_by_patterns.return_value = {
            "http://ex.org/Customer/CUST001",
        }
        uris = {"http://ex.org/Customer/CUST001"}
        result = _expand_uri_aliases(store, "triples", uris)
        assert result == uris


# ---------------------------------------------------------------------------
# _sql_escape
# ---------------------------------------------------------------------------


class TestSqlEscape:
    def test_single_quotes(self):
        assert sql_escape("O'Brien") == "O''Brien"

    def test_backslash(self):
        assert sql_escape("path\\to") == "path\\\\to"

    def test_clean_string(self):
        assert sql_escape("hello") == "hello"

    def test_both(self):
        assert sql_escape("it's a\\b") == "it''s a\\\\b"


# ---------------------------------------------------------------------------
# TripleStoreBackend.bfs_traversal (SQL generation moved from _build_bfs_sql)
# ---------------------------------------------------------------------------


class TestBfsTraversalSql:
    """Verify the SQL generated by TripleStoreBackend.bfs_traversal.

    Uses a thin stub that captures the SQL instead of executing it.
    """

    RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"

    def _capture_sql(self, table, seed_where, depth):
        captured = {}

        def fake_execute(sql):
            captured["sql"] = sql
            return []

        store = MagicMock(spec=TripleStoreBackend)
        store.execute_query = fake_execute
        store.bfs_traversal = lambda *a, **kw: TripleStoreBackend.bfs_traversal(
            store, *a, **kw
        )
        store.bfs_traversal(table, seed_where, depth)
        return captured["sql"]

    def test_contains_recursive_cte(self):
        sql = self._capture_sql("my_table", " WHERE subject = 'x'", 2)
        assert "WITH RECURSIVE" in sql
        assert "bfs" in sql

    def test_seed_where_embedded(self):
        seed_where = " WHERE predicate = 'http://test/pred'"
        sql = self._capture_sql("tbl", seed_where, 1)
        assert seed_where.strip() in sql

    def test_depth_limit_in_query(self):
        sql = self._capture_sql("tbl", " WHERE 1=1", 3)
        assert "b.lvl < 3" in sql

    def test_excludes_type_and_label_predicates(self):
        sql = self._capture_sql("tbl", " WHERE 1=1", 1)
        assert self.RDF_TYPE in sql
        assert self.RDFS_LABEL in sql
        assert "NOT LIKE '%#label'" in sql
        assert "NOT LIKE '%/label'" in sql

    def test_returns_entity_and_min_lvl(self):
        sql = self._capture_sql("tbl", " WHERE 1=1", 1)
        assert "entity" in sql
        assert "min_lvl" in sql
        assert "GROUP BY entity" in sql


# ---------------------------------------------------------------------------
# Pydantic response models
# ---------------------------------------------------------------------------


class TestPydanticModels:
    def test_status_response_defaults(self):
        from api.routers.digitaltwin import StatusResponse

        r = StatusResponse(success=True)
        assert r.has_data is False
        assert r.count == 0

    def test_stats_response_defaults(self):
        from api.routers.digitaltwin import StatsResponse

        r = StatsResponse(success=True)
        assert r.total_triples == 0
        assert r.entity_types == []

    def test_domains_response(self):
        from api.routers.domains import DomainInfo, DomainsResponse

        r = DomainsResponse(
            success=True,
            domains=[
                DomainInfo(name="d1", description="desc1"),
            ],
        )
        assert len(r.domains) == 1
        assert r.domains[0].name == "d1"

    def test_find_response_defaults(self):
        from api.routers.digitaltwin import FindResponse

        r = FindResponse(success=True)
        assert r.seed_count == 0
        assert r.triples == []

    def test_triples_response(self):
        from api.routers.digitaltwin import TriplesResponse, TripleRow

        r = TriplesResponse(
            success=True,
            triples=[TripleRow(subject="s", predicate="p", object="o")],
            count=1,
        )
        assert r.triples[0].subject == "s"

    def test_build_request_defaults(self):
        from api.routers.digitaltwin import BuildRequest

        r = BuildRequest()
        assert r.build_mode == "incremental"
        assert r.drop_existing is False

    def test_build_progress_response(self):
        from api.routers.digitaltwin import BuildProgressResponse

        r = BuildProgressResponse(success=True, task_id="abc", status="running")
        assert r.progress == 0
