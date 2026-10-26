"""Unit tests for :class:`GraphEngineNeo4jSettings` (extracted in 0.9.0).

Calls go through the public ``SettingsService`` façade so the delegator chain
(``SettingsService`` → ``GraphEngineSettings`` → ``GraphEngineNeo4jSettings``)
is exercised end to end. Neo4j, Databricks and the registry are mocked.
"""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import InfrastructureError, ValidationError
from back.objects.domain.settings.graphengine.GraphEngineNeo4jSettings import GraphEngineNeo4jSettings
from back.objects.domain.settings.SettingsService import SettingsService

pytestmark = pytest.mark.unit

_ss = importlib.import_module("back.objects.domain.settings.SettingsService")
_neo_conn_mod = importlib.import_module("back.core.graphdb.neo4j.Neo4jConnection")
_read_ops_mod = importlib.import_module("back.core.graphdb.neo4j.Neo4jReadOps")
_write_ops_mod = importlib.import_module("back.core.graphdb.neo4j.Neo4jWriteOps")
_client_mod = importlib.import_module("back.core.databricks.DatabricksClient")

REG = {"catalog": "cat", "schema": "sch", "volume": "vol"}


def _conn_profile(name="Prod", **overrides):
    profile = {
        "name": name,
        "uri": f"bolt://{name.lower()}:7687",
        "username": "neo4j",
        "secret_scope": "scope",
        "secret_key": "key",
        "auth_method": "databricks_secret",
        "database": "graphdb",
    }
    profile.update(overrides)
    return profile


def _gcfg(*profiles):
    return {"neo4j": {"connections": list(profiles)}}


@pytest.fixture
def ctx():
    """Patch the settings context resolution and the global config service."""
    gcs = MagicMock()
    gcs.get_graph_engine_config.return_value = {}
    with patch.object(
        SettingsService, "_resolve_context", return_value=(MagicMock(), "h", "t", REG)
    ), patch.object(_ss, "global_config_service", gcs):
        yield gcs


def _fake_neo4j_connection(rows=None, verify_exc=None, run_exc=None, close_exc=None):
    """Build a Neo4jConnection class double that records its constructor kwargs."""
    created = []

    def _factory(**kwargs):
        conn = MagicMock()
        conn.uri = kwargs["uri"]
        conn.database = kwargs["database"]
        driver = MagicMock()
        if verify_exc is not None:
            driver.verify_connectivity.side_effect = verify_exc
        conn.get_driver.return_value = driver
        if run_exc is not None:
            conn.run.side_effect = run_exc
        else:
            conn.run.return_value = rows if rows is not None else [{"probe": 1}]
        if close_exc is not None:
            conn.close.side_effect = close_exc
        conn.init_kwargs = kwargs
        created.append(conn)
        return conn

    cls = MagicMock(side_effect=_factory)
    cls.created = created
    return cls


# ---------------------------------------------------------------------------
# Reference safety guard
# ---------------------------------------------------------------------------


class TestNeo4jConnectionRefsGuard:
    def test_no_removed_connection_skips_domain_scan(self):
        with patch.object(
            SettingsService, "_domains_referencing_neo4j_connections"
        ) as scan:
            SettingsService._assert_neo4j_connection_refs_safe(
                _gcfg(_conn_profile("A")),
                _gcfg(_conn_profile("A"), _conn_profile("B")),
                MagicMock(),
                MagicMock(),
            )
        scan.assert_not_called()

    def test_removed_but_unreferenced_connection_is_allowed(self):
        with patch.object(
            SettingsService, "_domains_referencing_neo4j_connections", return_value={}
        ) as scan:
            SettingsService._assert_neo4j_connection_refs_safe(
                _gcfg(_conn_profile("A"), _conn_profile("B")),
                _gcfg(_conn_profile("A")),
                MagicMock(),
                MagicMock(),
            )
        assert scan.call_args[0][2] == ["B"]

    def test_removed_connection_still_referenced_by_domains_is_rejected(self):
        refs = {"B": ["sales", "hr"], "C": ["ops"]}
        with patch.object(
            SettingsService, "_domains_referencing_neo4j_connections", return_value=refs
        ):
            with pytest.raises(ValidationError) as exc_info:
                SettingsService._assert_neo4j_connection_refs_safe(
                    _gcfg(_conn_profile("A"), _conn_profile("B"), _conn_profile("C")),
                    _gcfg(_conn_profile("A")),
                    MagicMock(),
                    MagicMock(),
                )
        msg = str(exc_info.value)
        assert "re-point those domains first" in msg
        assert "'B' used by: sales, hr" in msg
        assert "'C' used by: ops" in msg


class TestDomainsReferencingNeo4jConnections:
    def _patch_registry(self, ok=True, details=None, from_context_exc=None):
        svc = MagicMock()
        svc.list_domain_details.return_value = (ok, details or [], "msg")
        target = "back.objects.registry.RegistryService.RegistryService.from_context"
        if from_context_exc is not None:
            return patch(target, side_effect=from_context_exc)
        return patch(target, return_value=svc)

    def test_blank_names_short_circuit(self):
        with patch.object(SettingsService, "_resolve_context") as resolve:
            assert (
                SettingsService._domains_referencing_neo4j_connections(
                    MagicMock(), MagicMock(), ["", "  "]
                )
                == {}
            )
        resolve.assert_not_called()

    def test_maps_connection_to_referencing_domain_folders(self, ctx):
        details = [
            {"name": "sales", "neo4j_connection": "Prod"},
            {"name": "hr", "neo4j_connection": " Prod "},
            {"name": "ops", "neo4j_connection": "Dev"},
            {"name": "misc", "neo4j_connection": "Other"},
            {"name": "", "neo4j_connection": "Prod"},
            "not-a-dict",
        ]
        with self._patch_registry(details=details):
            refs = SettingsService._domains_referencing_neo4j_connections(
                MagicMock(), MagicMock(), ["Prod", "Dev"]
            )
        assert refs == {"Prod": ["sales", "hr"], "Dev": ["ops"]}

    def test_registry_listing_failure_returns_empty(self, ctx):
        with self._patch_registry(ok=False):
            assert (
                SettingsService._domains_referencing_neo4j_connections(
                    MagicMock(), MagicMock(), ["Prod"]
                )
                == {}
            )

    def test_registry_exception_is_swallowed(self, ctx):
        with self._patch_registry(from_context_exc=RuntimeError("boom")):
            assert (
                SettingsService._domains_referencing_neo4j_connections(
                    MagicMock(), MagicMock(), ["Prod"]
                )
                == {}
            )


# ---------------------------------------------------------------------------
# Connection listing
# ---------------------------------------------------------------------------


class TestNeo4jConnectionsListing:
    def test_lists_connections_without_passwords(self, ctx):
        ctx.get_graph_engine_config.return_value = _gcfg(
            _conn_profile("A", password="s3cret"), _conn_profile("B")
        )
        out = SettingsService.graph_engine_neo4j_connections_result(
            MagicMock(), MagicMock()
        )
        assert out["success"] is True
        assert [c["name"] for c in out["connections"]] == ["A", "B"]
        assert all("password" not in c for c in out["connections"])
        ctx.load.assert_called_once_with("h", "t", REG, force=True)

    def test_config_load_failure_maps_to_infrastructure_error(self, ctx):
        ctx.load.side_effect = RuntimeError("store down")
        with pytest.raises(InfrastructureError, match="Could not load graph engine config"):
            SettingsService.graph_engine_neo4j_connections_result(
                MagicMock(), MagicMock()
            )


# ---------------------------------------------------------------------------
# Connectivity test
# ---------------------------------------------------------------------------


class TestNeo4jTestResult:
    def test_draft_with_uri_is_probed_without_loading_settings(self):
        neo = _fake_neo4j_connection(rows=[{"probe": 1}])
        draft = _conn_profile("Draft", uri="bolt://draft:7687")
        with patch.object(SettingsService, "_resolve_context") as resolve, patch.object(
            _neo_conn_mod, "Neo4jConnection", neo
        ):
            out = SettingsService.graph_engine_neo4j_test_result(
                MagicMock(), MagicMock(), draft=draft
            )
        resolve.assert_not_called()
        assert out["ok"] is True
        assert out["uri"] == "bolt://draft:7687"
        assert out["database"] == "graphdb"
        assert out["connection_name"] == "Draft"
        assert out["cypher_probe"] == {"rows": 1, "echo": {"probe": 1}}
        assert out["credentials_source"] == "Databricks secret (scope/key)"
        assert isinstance(out["latency_ms"], float)
        conn = neo.created[0]
        conn.get_driver.return_value.verify_connectivity.assert_called_once()
        conn.run.assert_called_once_with("RETURN 1 AS probe")
        conn.close.assert_called_once()

    def test_named_connection_resolved_from_settings(self, ctx):
        ctx.get_graph_engine_config.return_value = _gcfg(
            _conn_profile("A"), _conn_profile("B")
        )
        neo = _fake_neo4j_connection(rows=[])
        with patch.object(_neo_conn_mod, "Neo4jConnection", neo):
            out = SettingsService.graph_engine_neo4j_test_result(
                MagicMock(), MagicMock(), connection_name="B"
            )
        assert out["ok"] is True
        assert out["uri"] == "bolt://b:7687"
        assert out["cypher_probe"] == {"rows": 0, "echo": None}
        assert neo.created[0].init_kwargs["auth_method"] == "databricks_secret"
        assert neo.created[0].init_kwargs["encrypted"] is True

    def test_missing_selection_with_existing_connections_asks_to_select(self, ctx):
        ctx.get_graph_engine_config.return_value = _gcfg(_conn_profile("A"))
        out = SettingsService.graph_engine_neo4j_test_result(MagicMock(), MagicMock())
        assert out == {
            "success": True,
            "ok": False,
            "error": "Select a Neo4j connection to test.",
            "category": "config",
        }

    def test_unknown_connection_name_is_a_config_error(self, ctx):
        ctx.get_graph_engine_config.return_value = _gcfg(_conn_profile("A"))
        out = SettingsService.graph_engine_neo4j_test_result(
            MagicMock(), MagicMock(), connection_name="Ghost"
        )
        assert out["ok"] is False
        assert out["category"] == "config"
        assert "'Ghost' not found" in out["error"]

    def test_no_connections_configured(self, ctx):
        out = SettingsService.graph_engine_neo4j_test_result(MagicMock(), MagicMock())
        assert out["ok"] is False
        assert out["category"] == "config"
        assert "No Neo4j connection configured" in out["error"]

    def test_named_connection_without_uri(self, ctx):
        ctx.get_graph_engine_config.return_value = _gcfg(_conn_profile("A", uri=""))
        out = SettingsService.graph_engine_neo4j_test_result(
            MagicMock(), MagicMock(), connection_name="A"
        )
        assert out["ok"] is False
        assert out["error"] == "Bolt URI is missing on this connection."

    def test_settings_load_failure_raises_infrastructure_error(self, ctx):
        ctx.get_graph_engine_config.side_effect = RuntimeError("down")
        with pytest.raises(InfrastructureError):
            SettingsService.graph_engine_neo4j_test_result(
                MagicMock(), MagicMock(), connection_name="A"
            )

    @pytest.mark.parametrize(
        "ctor_exc, category",
        [
            (ValidationError("bad secret ref"), "config"),
            (ImportError("neo4j driver missing"), "driver-missing"),
        ],
    )
    def test_constructor_errors_are_categorised(self, ctor_exc, category):
        with patch.object(
            _neo_conn_mod, "Neo4jConnection", MagicMock(side_effect=ctor_exc)
        ):
            out = SettingsService.graph_engine_neo4j_test_result(
                MagicMock(), MagicMock(), draft=_conn_profile("D")
            )
        assert out["ok"] is False
        assert out["category"] == category
        assert str(ctor_exc) in out["error"]

    @pytest.mark.parametrize(
        "probe_exc, category, expected",
        [
            (InfrastructureError("auth failed"), "auth", "auth failed"),
            (ValidationError("bad db"), "config", "bad db"),
            (OSError("refused"), "connectivity", "OSError: refused"),
        ],
    )
    def test_probe_errors_are_categorised_and_connection_closed(
        self, probe_exc, category, expected
    ):
        neo = _fake_neo4j_connection(verify_exc=probe_exc, close_exc=RuntimeError("x"))
        with patch.object(_neo_conn_mod, "Neo4jConnection", neo):
            out = SettingsService.graph_engine_neo4j_test_result(
                MagicMock(), MagicMock(), draft=_conn_profile("D")
            )
        assert out["ok"] is False
        assert out["category"] == category
        assert expected in out["error"]
        neo.created[0].close.assert_called_once()


# ---------------------------------------------------------------------------
# Credentials source label
# ---------------------------------------------------------------------------


class TestNeo4jCredentialsSource:
    def test_databricks_secret_reports_scope_and_key(self):
        assert (
            SettingsService._neo4j_credentials_source(
                {"auth_method": "databricks_secret", "secret_scope": "s", "secret_key": "k"}
            )
            == "Databricks secret (s/k)"
        )

    def test_basic_auth_with_env_password_reports_apps_secret(self, monkeypatch):
        monkeypatch.setenv("NEO4J_PASSWORD", "pw")
        assert GraphEngineNeo4jSettings._neo4j_credentials_source(
            {"auth_method": "basic"}
        ).startswith("env var (NEO4J_PASSWORD")

    def test_basic_auth_without_env_password_reports_local_fallback(self, monkeypatch):
        monkeypatch.delenv("NEO4J_PASSWORD", raising=False)
        assert (
            GraphEngineNeo4jSettings._neo4j_credentials_source({})
            == "engine_config (local-dev fallback)"
        )

    def test_basic_auth_label_when_env_helper_available(self, monkeypatch):
        """Contract of the non-secret branches once the helper resolves."""
        with patch.object(
            _ss, "is_neo4j_password_from_secret", return_value=True
        ):
            assert "Databricks Apps secret" in (
                GraphEngineNeo4jSettings._neo4j_credentials_source({"auth_method": "basic"})
            )
        with patch.object(
            _ss, "is_neo4j_password_from_secret", return_value=False
        ):
            assert (
                GraphEngineNeo4jSettings._neo4j_credentials_source({})
                == "engine_config (local-dev fallback)"
            )


# ---------------------------------------------------------------------------
# Secret scope / key dropdowns
# ---------------------------------------------------------------------------


class TestNeo4jSecretDropdowns:
    def test_secret_scopes_listed_with_app_identity(self, ctx):
        client = MagicMock()
        client.list_secret_scopes.return_value = ["a", "b"]
        with patch.object(_client_mod, "DatabricksClient", return_value=client) as ctor:
            out = SettingsService.graph_engine_neo4j_secret_scopes_result(
                MagicMock(), MagicMock()
            )
        ctor.assert_called_once_with(host="h", token="t")
        assert out == {"success": True, "scopes": ["a", "b"]}

    def test_secret_keys_listed_for_scope(self, ctx):
        client = MagicMock()
        client.list_secret_keys.return_value = ["k1"]
        with patch.object(_client_mod, "DatabricksClient", return_value=client):
            out = SettingsService.graph_engine_neo4j_secret_keys_result(
                "  scope ", MagicMock(), MagicMock()
            )
        client.list_secret_keys.assert_called_once_with("scope")
        assert out == {"success": True, "keys": ["k1"]}

    def test_blank_scope_returns_empty_without_client(self):
        with patch.object(_client_mod, "DatabricksClient") as ctor:
            out = SettingsService.graph_engine_neo4j_secret_keys_result(
                "  ", MagicMock(), MagicMock()
            )
        ctor.assert_not_called()
        assert out == {"success": True, "keys": []}


# ---------------------------------------------------------------------------
# Admin endpoints: connection builder, databases, labels, health, drop
# ---------------------------------------------------------------------------


class TestNeo4jConnectionFromConfig:
    def test_single_connection_is_auto_selected(self, ctx):
        ctx.get_graph_engine_config.return_value = _gcfg(
            _conn_profile("Only", encrypted=False, auth_method="")
        )
        neo = _fake_neo4j_connection()
        with patch.object(_neo_conn_mod, "Neo4jConnection", neo):
            conn, gcfg = SettingsService._neo4j_connection_from_config(
                MagicMock(), MagicMock()
            )
        assert gcfg["name"] == "Only"
        assert conn.init_kwargs["uri"] == "bolt://only:7687"
        assert conn.init_kwargs["database"] == "graphdb"
        assert conn.init_kwargs["auth_method"] == "databricks_secret"
        assert conn.init_kwargs["encrypted"] is False

    @pytest.mark.parametrize("profiles", [[], ["A", "B"]])
    def test_ambiguous_or_missing_selection_rejected(self, ctx, profiles):
        ctx.get_graph_engine_config.return_value = _gcfg(
            *[_conn_profile(p) for p in profiles]
        )
        with pytest.raises(ValidationError, match="Select a Neo4j connection first"):
            SettingsService._neo4j_connection_from_config(MagicMock(), MagicMock())

    def test_named_connection_without_uri_rejected(self, ctx):
        ctx.get_graph_engine_config.return_value = _gcfg(_conn_profile("A", uri=""))
        with pytest.raises(ValidationError, match="no Bolt URI"):
            SettingsService._neo4j_connection_from_config(
                MagicMock(), MagicMock(), connection_name="A"
            )


def _patch_conn_builder(conn, gcfg=None):
    return patch.object(
        SettingsService,
        "_neo4j_connection_from_config",
        return_value=(conn, gcfg if gcfg is not None else {"name": "Prod"}),
    )


def _plain_conn(database="neo4j", uri="bolt://x", close_exc=None):
    conn = MagicMock()
    conn.database = database
    conn.uri = uri
    if close_exc is not None:
        conn.close.side_effect = close_exc
    return conn


class TestNeo4jDatabasesAndLabels:
    def test_configured_database_prepended_when_absent_from_server(self):
        conn = _plain_conn(database="custom", close_exc=RuntimeError("x"))
        with _patch_conn_builder(conn), patch.object(
            _read_ops_mod, "Neo4jReadOps"
        ) as ops:
            ops.return_value.list_databases.return_value = ["neo4j", "system"]
            out = SettingsService.graph_engine_neo4j_databases_result(
                MagicMock(), MagicMock(), connection_name="Prod"
            )
        assert out == {
            "success": True,
            "databases": ["custom", "neo4j", "system"],
            "configured": "custom",
            "connection_name": "Prod",
        }
        conn.close.assert_called_once()

    def test_configured_database_not_duplicated(self):
        conn = _plain_conn(database="neo4j")
        with _patch_conn_builder(conn, {}), patch.object(
            _read_ops_mod, "Neo4jReadOps"
        ) as ops:
            ops.return_value.list_databases.return_value = ["neo4j"]
            out = SettingsService.graph_engine_neo4j_databases_result(
                MagicMock(), MagicMock(), connection_name="Fallback"
            )
        assert out["databases"] == ["neo4j"]
        assert out["connection_name"] == "Fallback"

    def test_connection_closed_even_when_listing_fails(self):
        conn = _plain_conn()
        with _patch_conn_builder(conn), patch.object(
            _read_ops_mod, "Neo4jReadOps"
        ) as ops:
            ops.return_value.list_databases.side_effect = RuntimeError("down")
            with pytest.raises(RuntimeError):
                SettingsService.graph_engine_neo4j_databases_result(
                    MagicMock(), MagicMock()
                )
        conn.close.assert_called_once()

    def test_labels_listed_with_database(self):
        conn = _plain_conn(database="g", close_exc=RuntimeError("x"))
        graphs = [{"label": "Sales_v1", "nodes": 3}]
        with _patch_conn_builder(conn), patch.object(
            _read_ops_mod, "Neo4jReadOps"
        ) as ops:
            ops.return_value.list_labels.return_value = graphs
            out = SettingsService.graph_engine_neo4j_labels_result(
                MagicMock(), MagicMock(), connection_name="Prod"
            )
        assert out == {
            "success": True,
            "graphs": graphs,
            "database": "g",
            "connection_name": "Prod",
        }
        conn.close.assert_called_once()


class TestNeo4jHealth:
    def test_healthy_probe(self):
        conn = _plain_conn(database="g", uri="bolt://h", close_exc=RuntimeError("x"))
        with _patch_conn_builder(conn):
            out = SettingsService.graph_engine_neo4j_health_result(
                MagicMock(), MagicMock()
            )
        assert out["ok"] is True
        assert out["error"] is None
        assert out["uri"] == "bolt://h"
        assert out["database"] == "g"
        assert out["connection_name"] == "Prod"
        conn.run.assert_called_once_with("RETURN 1 AS probe")
        conn.close.assert_called_once()

    def test_failed_probe_reports_error_instead_of_raising(self):
        conn = _plain_conn()
        conn.get_driver.return_value.verify_connectivity.side_effect = OSError("refused")
        with _patch_conn_builder(conn):
            out = SettingsService.graph_engine_neo4j_health_result(
                MagicMock(), MagicMock()
            )
        assert out["success"] is True
        assert out["ok"] is False
        assert out["error"] == "OSError: refused"
        conn.close.assert_called_once()


class TestNeo4jDropLabel:
    def test_blank_label_rejected_before_connecting(self):
        with patch.object(SettingsService, "_neo4j_connection_from_config") as build:
            with pytest.raises(ValidationError, match="No graph label"):
                SettingsService.graph_engine_neo4j_drop_label_result(
                    "  ", MagicMock(), MagicMock()
                )
        build.assert_not_called()

    def test_drops_label_and_returns_sanitised_name(self):
        conn = _plain_conn(close_exc=RuntimeError("x"))
        with _patch_conn_builder(conn), patch.object(
            _write_ops_mod, "Neo4jWriteOps"
        ) as ops:
            out = SettingsService.graph_engine_neo4j_drop_label_result(
                " 1sales-v2 ", MagicMock(), MagicMock(), connection_name="Prod"
            )
        ops.assert_called_once_with(conn)
        ops.return_value.drop_table.assert_called_once_with("1sales-v2")
        assert out == {"success": True, "dropped": "_1sales_v2"}
        conn.close.assert_called_once()

    def test_connection_closed_when_drop_fails(self):
        conn = _plain_conn()
        with _patch_conn_builder(conn), patch.object(
            _write_ops_mod, "Neo4jWriteOps"
        ) as ops:
            ops.return_value.drop_table.side_effect = RuntimeError("locked")
            with pytest.raises(RuntimeError):
                SettingsService.graph_engine_neo4j_drop_label_result(
                    "g", MagicMock(), MagicMock()
                )
        conn.close.assert_called_once()
