"""Unit tests for :class:`GraphEngineSettings` (extracted from SettingsService in 0.9.0).

Covers the Connection-tab graph engine config get/set, domain-registry
mirroring, Lakehouse object scoping, UC pickers and the UC drop endpoint.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import (
    AuthorizationError,
    InfrastructureError,
    ValidationError,
)
from back.objects.domain.settings.graphengine.GraphEngineSettings import GraphEngineSettings
from back.objects.domain.settings.SettingsService import SettingsService

pytestmark = pytest.mark.unit

_ss = importlib.import_module("back.objects.domain.settings.SettingsService")
_db_auth_mod = importlib.import_module("back.core.databricks.DatabricksAuth")
_uc_pkg = importlib.import_module("back.core.databricks.uc")
_delta_objects_mod = importlib.import_module("back.core.graphdb.delta.objects")
_synced_mod = importlib.import_module("back.core.graphdb.lakebase.SyncedTableManager")

REG = {"catalog": "cat", "schema": "sch", "volume": "vol"}


@pytest.fixture
def domain():
    d = MagicMock()
    d.settings = {}
    d.databricks = {}
    return d


@pytest.fixture
def ctx(domain):
    gcs = MagicMock()
    gcs.get_graph_engine_config.return_value = {}
    gcs.get_warehouse_id.return_value = ""
    with patch.object(
        SettingsService, "_resolve_context", return_value=(domain, "h", "t", REG)
    ), patch.object(_ss, "global_config_service", gcs):
        yield gcs


def _neo_profile(name="Prod", **overrides):
    profile = {
        "name": name,
        "uri": "bolt://n:7687",
        "username": "neo4j",
        "secret_scope": "scope",
        "secret_key": "key",
    }
    profile.update(overrides)
    return profile


# ---------------------------------------------------------------------------
# Mirror to domain registry
# ---------------------------------------------------------------------------


class TestMirrorGraphEngineToDomainRegistry:
    def test_noop_when_nothing_to_mirror(self):
        with patch.object(_ss, "get_domain") as get_domain:
            SettingsService._mirror_graph_engine_to_domain_registry(MagicMock())
        get_domain.assert_not_called()

    def test_config_is_normalised_and_legacy_warehouse_key_dropped(self, domain):
        domain.settings = {"registry": {"delta_warehouse_id": "old"}}
        with patch.object(_ss, "get_domain", return_value=domain):
            SettingsService._mirror_graph_engine_to_domain_registry(
                MagicMock(), config={"lakebase": {"schema": "g"}}
            )
        reg = domain.settings["registry"]
        assert "delta_warehouse_id" not in reg
        assert reg["graph_engine_config"]["lakebase"]["schema"] == "g"
        assert set(reg["graph_engine_config"]) >= {"lakebase", "neo4j", "lakehouse"}
        domain.save.assert_called_once()

    def test_delta_warehouse_lands_under_lakehouse_section(self, domain):
        domain.settings = {
            "registry": {"graph_engine_config": {"lakehouse": {"use_sea": True}}}
        }
        with patch.object(_ss, "get_domain", return_value=domain):
            GraphEngineSettings._mirror_graph_engine_to_domain_registry(
                MagicMock(), delta_warehouse_id="  wh-42 "
            )
        lh = domain.settings["registry"]["graph_engine_config"]["lakehouse"]
        assert lh["warehouse_id"] == "wh-42"
        domain.save.assert_called_once()

    def test_failure_is_logged_not_raised(self, domain):
        domain.save.side_effect = RuntimeError("disk full")
        with patch.object(_ss, "get_domain", return_value=domain):
            GraphEngineSettings._mirror_graph_engine_to_domain_registry(
                MagicMock(), delta_warehouse_id="wh"
            )


# ---------------------------------------------------------------------------
# Lakehouse objects scoping
# ---------------------------------------------------------------------------


class TestLakehouseDomainVersionKeys:
    def _svc(self, ok=True, details=None, message=""):
        svc = MagicMock()
        svc.list_domain_details_cached.return_value = (ok, details or [], message)
        return svc

    def test_only_databricks_backend_versions_are_kept(self):
        details = [
            {
                "name": "Sales",
                "versions": [
                    {"version": "1", "graph_backend": "Databricks"},
                    {"version": "2", "graph_backend": "lakebase"},
                    {"version": "", "graph_backend": "databricks"},
                    "bad",
                ],
            },
            {"name": "hr", "versions": [{"version": "3", "graph_backend": "databricks"}]},
            "not-a-dict",
        ]
        with patch.object(
            _ss.RegistryService, "from_context", return_value=self._svc(details=details)
        ):
            keys = SettingsService._lakehouse_domain_version_keys(
                MagicMock(), MagicMock()
            )
        assert keys == {"sales_1", "hr_3"}

    def test_registry_failure_raises(self):
        with patch.object(
            _ss.RegistryService,
            "from_context",
            return_value=self._svc(ok=False, message="nope"),
        ):
            with pytest.raises(InfrastructureError, match="Failed to list registry domains"):
                SettingsService._lakehouse_domain_version_keys(MagicMock(), MagicMock())


class TestAnalyticsObjects:
    def test_non_catalog_schema_location_reported(self):
        settings = SimpleNamespace(analytics_job_output_schema="just_one_part")
        location, groups, message = SettingsService._analytics_objects(
            settings, "cat", "sch", []
        )
        assert location == "just_one_part"
        assert groups == []
        assert "not a catalog.schema pair" in message


class TestTripleStoreObjects:
    def test_listing_failure_maps_to_infrastructure_error(self, ctx):
        with patch.object(
            SettingsService, "_lakehouse_domain_version_keys", return_value=set()
        ), patch.object(
            _delta_objects_mod,
            "fetch_uc_schema_tables",
            side_effect=RuntimeError("uc down"),
        ):
            with pytest.raises(InfrastructureError, match="list Delta triple-store objects failed"):
                SettingsService.triple_store_databricks_objects_result(
                    MagicMock(), MagicMock()
                )


class TestTripleStoreDatabricksHealth:
    def test_health_reaches_permission_check_through_module_client(self, monkeypatch):
        with patch.object(
            _ss,
            "resolve_app_registry_context",
            return_value=("h", "t", {"catalog": "c", "schema": "s"}),
        ), patch.object(_ss, "DatabricksClient") as client_cls:
            client = client_cls.return_value
            client.auth.client_id = "sp"
            client.catalog.get_effective_schema_permissions.return_value = {
                "accessible": True,
                "assignments": [],
            }
            out = SettingsService.triple_store_databricks_health_result(
                MagicMock(), MagicMock()
            )
        assert out["principal"] == "sp"


# ---------------------------------------------------------------------------
# get / set graph engine config
# ---------------------------------------------------------------------------


class TestGetGraphEngineConfig:
    def test_empty_lakebase_fields_overlaid_with_env(self, ctx, monkeypatch):
        monkeypatch.setenv("LAKEBASE_PROJECT", "proj-env")
        monkeypatch.setenv("LAKEBASE_BRANCH", "br-env")
        monkeypatch.delenv("PGDATABASE", raising=False)
        monkeypatch.setenv("LAKEBASE_DATABASE", "db-env")
        out = SettingsService.get_graph_engine_config_result(MagicMock(), MagicMock())
        lb = out["graph_engine_config"]["lakebase"]
        assert lb["lakebase_project"] == "proj-env"
        assert lb["lakebase_branch"] == "br-env"
        assert lb["database"] == "db-env"

    def test_saved_values_win_over_env(self, ctx, monkeypatch):
        monkeypatch.setenv("LAKEBASE_PROJECT", "proj-env")
        monkeypatch.setenv("PGDATABASE", "pg-env")
        ctx.get_graph_engine_config.return_value = {
            "lakebase": {"lakebase_project": "saved", "database": "saved-db"}
        }
        out = SettingsService.get_graph_engine_config_result(MagicMock(), MagicMock())
        lb = out["graph_engine_config"]["lakebase"]
        assert lb["lakebase_project"] == "saved"
        assert lb["database"] == "saved-db"


class TestSetGraphEngineConfig:
    @pytest.fixture
    def save_ctx(self, ctx):
        ctx.set_graph_engine_config.return_value = (True, "ok")
        with patch.object(SettingsService, "require_admin_error"), patch.object(
            SettingsService, "_mirror_graph_engine_to_domain_registry"
        ) as mirror, patch.object(
            SettingsService, "_domains_referencing_neo4j_connections", return_value={}
        ):
            ctx.mirror = mirror
            yield ctx

    def _save(self, cfg):
        return SettingsService.set_graph_engine_config_result(
            cfg, "a@x.com", "tok", MagicMock(), MagicMock()
        )

    def test_non_admin_is_rejected_before_any_write(self, ctx):
        with patch.object(_ss, "is_databricks_app", return_value=True), patch.object(
            _ss.permission_service, "is_admin", return_value=False
        ):
            with pytest.raises(AuthorizationError):
                self._save({})
        ctx.set_graph_engine_config.assert_not_called()

    def test_duplicate_connection_names_rejected_end_to_end(self, save_ctx):
        cfg = {
            "neo4j": {
                "connections": [
                    _neo_profile("A", uri="bolt://one"),
                    _neo_profile("A", uri="bolt://two"),
                ]
            }
        }
        with pytest.raises(ValidationError, match="Duplicate Neo4j connection name 'A'"):
            self._save(cfg)
        save_ctx.set_graph_engine_config.assert_not_called()

    @pytest.mark.parametrize(
        "overrides, match",
        [
            ({"uri": ""}, "missing a Bolt URI"),
            ({"username": " "}, "missing a username"),
            ({"secret_key": ""}, "must set secret scope and secret name"),
        ],
    )
    def test_incomplete_connection_rejected(self, save_ctx, overrides, match):
        cfg = {"neo4j": {"connections": [_neo_profile("A", **overrides)]}}
        with pytest.raises(ValidationError, match=match):
            self._save(cfg)
        save_ctx.set_graph_engine_config.assert_not_called()

    def test_secret_backed_connection_password_is_stripped_and_mirrored(self, save_ctx):
        persisted = {"neo4j": {"connections": [{"name": "A"}]}}
        save_ctx.get_graph_engine_config.side_effect = [{}, persisted]
        cfg = {
            "neo4j": {
                "connections": [
                    {"name": ""},
                    _neo_profile(" A ", password="clear", auth_method=""),
                ]
            }
        }
        out = self._save(cfg)
        saved = save_ctx.set_graph_engine_config.call_args[0][3]
        conns = saved["neo4j"]["connections"]
        assert len(conns) == 1
        assert conns[0]["name"] == "A"
        assert conns[0]["auth_method"] == "databricks_secret"
        assert "password" not in conns[0]
        assert out == {"success": True, "graph_engine_config": persisted}
        save_ctx.mirror.assert_called_once()
        assert save_ctx.mirror.call_args.kwargs["config"] == persisted

    def test_store_rejection_surfaces_as_validation_error(self, save_ctx):
        save_ctx.set_graph_engine_config.return_value = (False, "invalid schema")
        with pytest.raises(ValidationError, match="invalid schema"):
            self._save({"lakebase": {"schema": "x"}})
        save_ctx.mirror.assert_not_called()

    def test_removing_referenced_connection_is_blocked(self, ctx):
        ctx.get_graph_engine_config.return_value = {
            "neo4j": {"connections": [_neo_profile("Old")]}
        }
        with patch.object(SettingsService, "require_admin_error"), patch.object(
            SettingsService,
            "_domains_referencing_neo4j_connections",
            return_value={"Old": ["sales"]},
        ):
            with pytest.raises(ValidationError, match="'Old' used by: sales"):
                self._save({"neo4j": {"connections": [_neo_profile("New")]}})
        ctx.set_graph_engine_config.assert_not_called()

    def test_basic_auth_password_kept_for_local_dev(self, save_ctx, monkeypatch):
        monkeypatch.delenv("NEO4J_PASSWORD", raising=False)
        save_ctx.get_graph_engine_config.return_value = {}
        self._save(
            {"neo4j": {"connections": [_neo_profile("A", auth_method="basic", password="pw")]}}
        )
        saved = save_ctx.set_graph_engine_config.call_args[0][3]
        assert saved["neo4j"]["connections"][0]["password"] == "pw"


# ---------------------------------------------------------------------------
# Unity Catalog pickers
# ---------------------------------------------------------------------------


class TestUcCatalogs:
    def test_warehouse_falls_back_to_domain_then_settings(self, ctx, domain):
        domain.databricks = {"warehouse_id": "wh-domain"}
        with patch.object(_db_auth_mod, "DatabricksAuth") as auth_cls, patch.object(
            _uc_pkg, "UnityCatalog"
        ) as uc_cls:
            uc_cls.return_value.get_catalogs.return_value = []
            out = SettingsService.graph_engine_uc_catalogs_result(
                MagicMock(), SimpleNamespace(sql_warehouse_id="wh-settings")
            )
        assert out == {"success": True, "catalogs": []}
        assert auth_cls.call_args.kwargs["warehouse_id"] == "wh-domain"

    def test_settings_warehouse_used_last(self, ctx):
        with patch.object(_db_auth_mod, "DatabricksAuth") as auth_cls, patch.object(
            _uc_pkg, "UnityCatalog"
        ) as uc_cls:
            uc_cls.return_value.get_catalogs.return_value = ["z", "a"]
            out = SettingsService.graph_engine_uc_catalogs_result(
                MagicMock(), SimpleNamespace(sql_warehouse_id="wh-settings")
            )
        assert out["catalogs"] == ["a", "z"]
        assert auth_cls.call_args.kwargs["warehouse_id"] == "wh-settings"

    def test_unexpected_error_maps_to_infrastructure_error(self, ctx):
        ctx.get_warehouse_id.return_value = "wh"
        with patch.object(_db_auth_mod, "DatabricksAuth"), patch.object(
            _uc_pkg, "UnityCatalog"
        ) as uc_cls:
            uc_cls.return_value.get_catalogs.side_effect = RuntimeError("boom")
            with pytest.raises(InfrastructureError, match="list Unity Catalog catalogs failed"):
                SettingsService.graph_engine_uc_catalogs_result(
                    MagicMock(), SimpleNamespace(sql_warehouse_id="")
                )


class TestUcSchemas:
    def test_catalog_required(self):
        with pytest.raises(ValidationError, match="catalog is required"):
            SettingsService.graph_engine_uc_schemas_result("", MagicMock(), MagicMock())

    def test_lists_sorted_schemas_with_global_warehouse(self, ctx):
        ctx.get_warehouse_id.return_value = "wh-global"
        with patch.object(_db_auth_mod, "DatabricksAuth") as auth_cls, patch.object(
            _uc_pkg, "UnityCatalog"
        ) as uc_cls:
            uc_cls.return_value.get_schemas.return_value = ["s2", "s1"]
            out = SettingsService.graph_engine_uc_schemas_result(
                "main", MagicMock(), SimpleNamespace(sql_warehouse_id="")
            )
        uc_cls.return_value.get_schemas.assert_called_once_with("main")
        assert auth_cls.call_args.kwargs == {
            "host": "h",
            "token": "t",
            "warehouse_id": "wh-global",
        }
        assert out == {"success": True, "schemas": ["s1", "s2"]}

    def test_empty_schema_list(self, ctx, domain):
        domain.databricks = {"warehouse_id": "wh-d"}
        with patch.object(_db_auth_mod, "DatabricksAuth"), patch.object(
            _uc_pkg, "UnityCatalog"
        ) as uc_cls:
            uc_cls.return_value.get_schemas.return_value = None
            out = SettingsService.graph_engine_uc_schemas_result(
                "main", MagicMock(), SimpleNamespace(sql_warehouse_id="")
            )
        assert out["schemas"] == []

    def test_no_warehouse_anywhere_is_a_validation_error(self, ctx):
        with pytest.raises(ValidationError, match="Configure a SQL warehouse"):
            SettingsService.graph_engine_uc_schemas_result(
                "main", MagicMock(), SimpleNamespace(sql_warehouse_id="")
            )

    def test_unexpected_error_maps_to_infrastructure_error(self, ctx):
        ctx.load.side_effect = RuntimeError("store down")
        with pytest.raises(InfrastructureError, match="list Unity Catalog schemas failed"):
            SettingsService.graph_engine_uc_schemas_result(
                "main", MagicMock(), SimpleNamespace(sql_warehouse_id="")
            )


# ---------------------------------------------------------------------------
# Drop UC object
# ---------------------------------------------------------------------------


class TestDropUcObject:
    @pytest.mark.parametrize("fqn", ["", "cat.table"])
    def test_requires_three_part_name(self, fqn):
        with patch("databricks.sdk.WorkspaceClient") as ws:
            with pytest.raises(ValidationError, match="3-part"):
                SettingsService.graph_engine_drop_uc_object_result(
                    fqn, False, MagicMock(), MagicMock()
                )
        ws.assert_not_called()

    def test_plain_table_deleted_via_uc_rest(self):
        ws = MagicMock()
        with patch("databricks.sdk.WorkspaceClient", return_value=ws), patch.object(
            _synced_mod, "SyncedTableManager"
        ) as mgr_cls:
            out = SettingsService.graph_engine_drop_uc_object_result(
                "c.s.t", False, MagicMock(), MagicMock()
            )
        ws.api_client.do.assert_called_once_with(
            "DELETE", "/api/2.1/unity-catalog/tables/c.s.t"
        )
        mgr_cls.assert_not_called()
        assert out == {"success": True, "message": "Dropped c.s.t"}

    def test_synced_table_deleted_via_manager_with_purge(self):
        ws = MagicMock()
        with patch("databricks.sdk.WorkspaceClient", return_value=ws), patch.object(
            _synced_mod, "SyncedTableManager"
        ) as mgr_cls:
            SettingsService.graph_engine_drop_uc_object_result(
                "c.s.t_sync", True, MagicMock(), MagicMock()
            )
        mgr_cls.return_value.delete.assert_called_once_with("c.s.t_sync", purge_data=True)
        ws.api_client.do.assert_not_called()

    def test_missing_api_client_is_infrastructure_error(self):
        with patch(
            "databricks.sdk.WorkspaceClient",
            return_value=SimpleNamespace(api_client=None),
        ):
            with pytest.raises(InfrastructureError, match="api_client unavailable"):
                SettingsService.graph_engine_drop_uc_object_result(
                    "c.s.t", False, MagicMock(), MagicMock()
                )

    def test_rest_failure_wrapped(self):
        ws = MagicMock()
        ws.api_client.do.side_effect = RuntimeError("403")
        with patch("databricks.sdk.WorkspaceClient", return_value=ws):
            with pytest.raises(InfrastructureError, match="Drop UC object failed"):
                SettingsService.graph_engine_drop_uc_object_result(
                    "c.s.t", False, MagicMock(), MagicMock()
                )
