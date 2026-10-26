"""RegistrySettings (0.9.0): UC browse probes, Lakebase registry status,
Initialize, and the *Repair permissions* grant flow."""

from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from back.core.errors import InfrastructureError, ValidationError
from back.objects.domain.settings.RegistrySettings import RegistrySettings
from back.objects.domain.settings.SettingsService import SettingsService

pytestmark = pytest.mark.unit

_rs = importlib.import_module("back.objects.domain.settings.RegistrySettings")
_ss = importlib.import_module("back.objects.domain.settings.SettingsService")


async def _inline_run_blocking(func, *args, **kwargs):
    return func(*args, **kwargs)


@pytest.fixture
def uc_client(monkeypatch):
    """Databricks client returned by ``get_databricks_client``; run_blocking inlined."""
    fake = MagicMock()
    monkeypatch.setattr(_rs, "get_domain", lambda sm: MagicMock())
    monkeypatch.setattr(_rs, "get_databricks_client", lambda d, s: fake)
    monkeypatch.setattr(_rs, "run_blocking", _inline_run_blocking)
    return fake


@pytest.fixture
def no_client(monkeypatch):
    monkeypatch.setattr(_rs, "get_domain", lambda sm: MagicMock())
    monkeypatch.setattr(_rs, "get_databricks_client", lambda d, s: None)
    monkeypatch.setattr(_rs, "run_blocking", _inline_run_blocking)


def _rcfg(configured=True, **kw):
    base = dict(
        is_configured=configured,
        catalog="cat",
        schema="sch",
        volume="vol",
        lakebase_schema="ontobricks_registry",
        lakebase_database="",
    )
    base.update(kw)
    cfg = SimpleNamespace(**base)
    cfg.as_dict = lambda: {"catalog": cfg.catalog, "schema": cfg.schema, "volume": cfg.volume}
    return cfg


@pytest.fixture
def rcfg(monkeypatch):
    """Patch ``RegistryCfg.from_session`` to return a configurable namespace."""
    holder = {"cfg": _rcfg()}
    cls = MagicMock()
    cls.from_session.side_effect = lambda sm, st: holder["cfg"]
    monkeypatch.setattr(_rs, "RegistryCfg", cls)
    return holder


@pytest.fixture
def factory(monkeypatch):
    """Patch ``RegistryFactory.lakebase`` used by the Lakebase probes."""
    import back.objects.registry.store as store_pkg

    fake_factory = MagicMock()
    monkeypatch.setattr(store_pkg, "RegistryFactory", fake_factory)
    return fake_factory


# ---------------------------------------------------------------------------
# UC browse probes
# ---------------------------------------------------------------------------

_BROWSE = [
    ("fetch_catalogs", (), "get_catalogs", "catalogs", None),
    ("fetch_schemas", ("main",), "get_schemas", "schemas", None),
    ("fetch_volumes", ("main", "s"), "get_volumes", "volumes", None),
    ("fetch_uc_assets", ("main", "s"), "list_tables_and_views", "assets", True),
    ("fetch_uc_functions", ("main", "s"), "list_functions", "functions", True),
]


@pytest.mark.parametrize("method,args,client_attr,key,success", _BROWSE)
async def test_browse_returns_client_listing(uc_client, method, args, client_attr, key, success):
    getattr(uc_client, client_attr).return_value = [{"name": "x"}]

    result = await getattr(RegistrySettings, method)(*args, MagicMock(), MagicMock())

    assert result[key] == [{"name": "x"}]
    getattr(uc_client, client_attr).assert_called_once_with(*args)
    if success:
        assert result["success"] is True


@pytest.mark.parametrize("method,args,client_attr,key,success", _BROWSE)
async def test_browse_without_client_raises_validation(no_client, method, args, client_attr, key, success):
    with pytest.raises(ValidationError, match="Databricks not configured"):
        await getattr(RegistrySettings, method)(*args, MagicMock(), MagicMock())


@pytest.mark.parametrize("method,args,client_attr,key,success", _BROWSE)
async def test_browse_wraps_sdk_errors_as_infrastructure(uc_client, method, args, client_attr, key, success):
    getattr(uc_client, client_attr).side_effect = RuntimeError("boom")

    with pytest.raises(InfrastructureError) as exc_info:
        await getattr(RegistrySettings, method)(*args, MagicMock(), MagicMock())
    assert "boom" in str(exc_info.value.detail)


async def test_browse_uses_custom_log_label_in_error_message(uc_client):
    uc_client.get_schemas.side_effect = RuntimeError("nope")
    with pytest.raises(InfrastructureError, match="Mapping schemas failed"):
        await RegistrySettings.fetch_schemas(
            "main", MagicMock(), MagicMock(), log_label="Mapping schemas"
        )


# ---------------------------------------------------------------------------
# check_lakebase_permissions
# ---------------------------------------------------------------------------


class TestCheckLakebasePermissions:
    async def test_unconfigured_registry_raises_validation(self, rcfg):
        rcfg["cfg"] = _rcfg(configured=False)
        with pytest.raises(ValidationError, match="Registry not configured"):
            await RegistrySettings.check_lakebase_permissions(MagicMock(), MagicMock())

    async def test_returns_store_report_built_for_registry_schema(self, rcfg, factory, monkeypatch):
        monkeypatch.setattr(_rs, "run_blocking", _inline_run_blocking)
        rcfg["cfg"] = _rcfg(lakebase_schema="reg", lakebase_database="db2")
        factory.lakebase.return_value.check_permissions.return_value = {"ok": True}

        result = await RegistrySettings.check_lakebase_permissions(MagicMock(), MagicMock())

        assert result == {"ok": True}
        factory.lakebase.assert_called_once_with(
            registry_cfg=rcfg["cfg"], schema="reg", database="db2"
        )

    async def test_missing_psycopg_raises_infrastructure(self, rcfg, factory, monkeypatch):
        monkeypatch.setattr(_rs, "run_blocking", _inline_run_blocking)
        factory.lakebase.side_effect = ImportError("psycopg")
        with pytest.raises(InfrastructureError, match="psycopg is not installed"):
            await RegistrySettings.check_lakebase_permissions(MagicMock(), MagicMock())

    async def test_store_error_surfaces_message(self, rcfg, factory, monkeypatch):
        monkeypatch.setattr(_rs, "run_blocking", _inline_run_blocking)
        factory.lakebase.return_value.check_permissions.side_effect = RuntimeError("denied")
        with pytest.raises(InfrastructureError, match="denied"):
            await RegistrySettings.check_lakebase_permissions(MagicMock(), MagicMock())


# ---------------------------------------------------------------------------
# check_registry_access
# ---------------------------------------------------------------------------


class TestCheckRegistryAccess:
    async def test_unconfigured_registry_raises_validation(self, rcfg):
        rcfg["cfg"] = _rcfg(configured=False)
        with pytest.raises(ValidationError):
            await RegistrySettings.check_registry_access(MagicMock(), MagicMock())

    async def test_missing_client_raises_infrastructure(self, rcfg, no_client):
        with pytest.raises(InfrastructureError, match="client not available"):
            await RegistrySettings.check_registry_access(MagicMock(), MagicMock())

    async def test_reports_schema_and_volume_paths(self, rcfg, uc_client):
        uc_client.catalog.check_schema_access.return_value = {"exists": True, "accessible": True}
        uc_client.catalog.check_volume_access.return_value = {"exists": True, "accessible": False}

        result = await RegistrySettings.check_registry_access(MagicMock(), MagicMock())

        assert result["success"] is True
        assert result["schema"]["path"] == "cat.sch"
        assert result["volume"] == {
            "exists": True,
            "accessible": False,
            "name": "vol",
            "path": "cat.sch.vol",
        }

    async def test_defaults_volume_name_when_unset(self, rcfg, uc_client):
        rcfg["cfg"] = _rcfg(volume="")
        uc_client.catalog.check_schema_access.return_value = {}
        uc_client.catalog.check_volume_access.return_value = {}

        result = await RegistrySettings.check_registry_access(MagicMock(), MagicMock())

        uc_client.catalog.check_volume_access.assert_called_once_with("cat", "sch", "OntoBricksRegistry")
        assert result["volume"]["path"] == "cat.sch.OntoBricksRegistry"


# ---------------------------------------------------------------------------
# build_registry_get_payload
# ---------------------------------------------------------------------------


class TestBuildRegistryGetPayload:
    @pytest.fixture(autouse=True)
    def _common(self, monkeypatch, rcfg):
        monkeypatch.setattr(_rs, "get_domain", lambda sm: MagicMock())
        monkeypatch.setattr(SettingsService, "is_registry_locked", staticmethod(lambda s: True))
        monkeypatch.setattr(
            SettingsService, "_lakebase_runtime_info", staticmethod(lambda r: {"bound": True})
        )
        monkeypatch.setattr(
            SettingsService,
            "_resolve_context",
            staticmethod(lambda sm, st: (MagicMock(), "h", "t", {"catalog": "cat"})),
        )

    def test_includes_graph_engine_config_and_delta_warehouse(self, monkeypatch):
        rs_cls = MagicMock()
        rs_cls.from_context.return_value.is_initialized.return_value = True
        monkeypatch.setattr(_rs, "RegistryService", rs_cls)
        gcs = MagicMock()
        gcs.get_graph_engine_config.return_value = {"engine": "lakebase"}
        gcs.get_delta_warehouse_id.return_value = "wh-delta"
        monkeypatch.setattr(_ss, "global_config_service", gcs)

        payload = RegistrySettings.build_registry_get_payload(MagicMock(), MagicMock())

        assert payload["success"] is True
        assert payload["configured"] is True
        assert payload["catalog"] == "cat"
        assert payload["registry_locked"] is True
        assert payload["lakebase"] == {"bound": True}
        assert payload["graph_engine_config"] == {"engine": "lakebase"}
        assert payload["delta_warehouse_id"] == "wh-delta"

    def test_registry_and_global_config_failures_degrade_to_defaults(self, monkeypatch):
        rs_cls = MagicMock()
        rs_cls.from_context.side_effect = RuntimeError("down")
        monkeypatch.setattr(_rs, "RegistryService", rs_cls)
        gcs = MagicMock()
        gcs.load.side_effect = RuntimeError("down")
        monkeypatch.setattr(_ss, "global_config_service", gcs)

        payload = RegistrySettings.build_registry_get_payload(MagicMock(), MagicMock())

        assert payload["configured"] is False
        assert payload["graph_engine_config"] == {}
        assert payload["delta_warehouse_id"] == ""


# ---------------------------------------------------------------------------
# Lakebase runtime info / schema status
# ---------------------------------------------------------------------------


class TestLakebaseRuntimeInfo:
    def _auth(self, monkeypatch, available):
        import back.core.databricks as dbx

        monkeypatch.setattr(
            dbx, "get_lakebase_auth", lambda: SimpleNamespace(is_available=available)
        )

    def test_unbound_returns_empty_block_with_override(self, monkeypatch):
        self._auth(monkeypatch, False)
        info = RegistrySettings._lakebase_runtime_info(_rcfg(lakebase_database="override_db"))

        assert info["bound"] is False
        assert info["host"] == ""
        assert info["database_override"] == "override_db"
        assert info["effective_database"] == "override_db"
        assert info["schema"] == "ontobricks_registry"

    def test_bound_uses_env_and_override_wins(self, monkeypatch):
        self._auth(monkeypatch, True)
        monkeypatch.setenv("PGHOST", "pg.host")
        monkeypatch.setenv("PGDATABASE", "bound_db")
        monkeypatch.setenv("PGPORT", "6543")
        monkeypatch.setenv("PGUSER", "sp-uuid")
        monkeypatch.setenv("LAKEBASE_BRANCH", "main")
        monkeypatch.setenv("LAKEBASE_PROJECT", "proj")
        monkeypatch.setattr(
            SettingsService,
            "_lakebase_schema_status",
            staticmethod(lambda r: {"initialized": True, "populated": False}),
        )

        info = RegistrySettings._lakebase_runtime_info(_rcfg(lakebase_database="ovr"))

        assert info["bound"] is True
        assert info["host"] == "pg.host"
        assert info["port"] == "6543"
        assert info["database"] == "bound_db"
        assert info["effective_database"] == "ovr"
        assert info["user"] == "sp-uuid"
        assert info["branch"] == "main"
        assert info["project"] == "proj"
        assert info["initialized"] is True
        assert info["populated"] is False

    def test_bound_falls_back_to_lakebase_database_env(self, monkeypatch):
        self._auth(monkeypatch, True)
        monkeypatch.delenv("PGDATABASE", raising=False)
        monkeypatch.setenv("LAKEBASE_DATABASE", "lb_db")
        monkeypatch.delenv("PGPORT", raising=False)
        monkeypatch.setattr(
            SettingsService,
            "_lakebase_schema_status",
            staticmethod(lambda r: {"initialized": False, "populated": False}),
        )

        info = RegistrySettings._lakebase_runtime_info(_rcfg())

        assert info["database"] == "lb_db"
        assert info["effective_database"] == "lb_db"
        assert info["port"] == "5432"


class TestLakebaseSchemaStatus:
    def test_missing_psycopg_returns_all_false(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "psycopg", None)
        assert RegistrySettings._lakebase_schema_status(_rcfg()) == {
            "initialized": False,
            "populated": False,
        }

    def test_store_error_returns_all_false(self, factory):
        factory.lakebase.side_effect = RuntimeError("no endpoint")
        assert RegistrySettings._lakebase_schema_status(_rcfg()) == {
            "initialized": False,
            "populated": False,
        }

    def test_uninitialized_skips_row_count_probe(self, factory):
        store = factory.lakebase.return_value
        store.is_initialized.return_value = False

        assert RegistrySettings._lakebase_schema_status(_rcfg())["initialized"] is False
        store.table_row_counts.assert_not_called()

    def test_populated_when_any_canonical_table_has_rows(self, factory):
        store = factory.lakebase.return_value
        store.is_initialized.return_value = True
        store.table_row_counts.return_value = {"domains": 0, "schedules": 3}

        assert RegistrySettings._lakebase_schema_status(_rcfg()) == {
            "initialized": True,
            "populated": True,
        }
        probed = store.table_row_counts.call_args.args[0]
        assert set(probed) == {
            "domains",
            "domain_versions",
            "domain_permissions",
            "schedules",
            "schedule_runs",
        }

    def test_empty_tables_are_not_populated(self, factory):
        store = factory.lakebase.return_value
        store.is_initialized.return_value = True
        store.table_row_counts.return_value = {"domains": 0, "schedules": None}
        assert RegistrySettings._lakebase_schema_status(_rcfg())["populated"] is False

    def test_row_count_failure_keeps_initialized(self, factory):
        store = factory.lakebase.return_value
        store.is_initialized.return_value = True
        store.table_row_counts.side_effect = RuntimeError("x")
        assert RegistrySettings._lakebase_schema_status(_rcfg()) == {
            "initialized": True,
            "populated": False,
        }

    def test_initialized_helper_reads_status(self, monkeypatch):
        monkeypatch.setattr(
            SettingsService,
            "_lakebase_schema_status",
            staticmethod(lambda r: {"initialized": True, "populated": False}),
        )
        assert RegistrySettings._lakebase_schema_initialized(_rcfg()) is True


# ---------------------------------------------------------------------------
# initialize_registry_result
# ---------------------------------------------------------------------------


class TestInitializeRegistry:
    @pytest.fixture
    def env(self, monkeypatch):
        svc = MagicMock()
        svc.cfg.is_configured = True
        svc.initialize.return_value = (True, "Registry initialized")
        rs_cls = MagicMock()
        rs_cls.from_context.return_value = svc
        monkeypatch.setattr(_rs, "RegistryService", rs_cls)
        monkeypatch.setattr(_rs, "get_domain", lambda sm: MagicMock())
        client = MagicMock()
        monkeypatch.setattr(_rs, "get_databricks_client", lambda d, s: client)
        monkeypatch.setattr(
            SettingsService,
            "_resolve_context",
            staticmethod(lambda sm, st: (MagicMock(), "h", "t", {"catalog": "cat"})),
        )
        gcs = MagicMock()
        gcs.load.return_value = {"graph_engine": "lakebase"}
        gcs._save.return_value = (True, "ok")
        monkeypatch.setattr(_ss, "global_config_service", gcs)
        grant = MagicMock(return_value={"granted": ["app"]})
        monkeypatch.setattr(SettingsService, "_grant_registry_permissions", grant)
        import back.objects.registry.store.lakebase.store as lb_store

        reset = MagicMock()
        monkeypatch.setattr(lb_store, "reset_lakebase_triplet_cache", reset)
        return SimpleNamespace(
            svc=svc, rs_cls=rs_cls, client=client, gcs=gcs, grant=grant, reset=reset
        )

    def test_success_pins_volume_binding_resets_cache_and_reports_grants(self, env):
        result = RegistrySettings.initialize_registry_result(MagicMock(), MagicMock())

        assert result == {
            "success": True,
            "message": "Registry initialized",
            "permissions": {"granted": ["app"]},
        }
        assert env.rs_cls.from_context.call_args.kwargs == {"prefer_volume_binding": True}
        env.svc.initialize.assert_called_once_with(env.client)
        env.reset.assert_called_once()
        env.gcs.load.assert_called_once_with("h", "t", {"catalog": "cat"}, force=True)
        env.gcs._save.assert_not_called()

    def test_seeds_lakebase_graph_engine_when_missing(self, env):
        env.gcs.load.return_value = {"graph_engine_config": {"schema": "g"}}

        RegistrySettings.initialize_registry_result(MagicMock(), MagicMock())

        env.gcs._save.assert_called_once_with(
            "h",
            "t",
            {"catalog": "cat"},
            {"graph_engine": "lakebase", "graph_engine_config": {"schema": "g"}},
        )

    def test_seed_ignores_non_dict_engine_config_and_failed_save(self, env):
        env.gcs.load.return_value = {"graph_engine_config": "garbage"}
        env.gcs._save.return_value = (False, "readonly")

        result = RegistrySettings.initialize_registry_result(MagicMock(), MagicMock())

        assert env.gcs._save.call_args.args[3]["graph_engine_config"] == {}
        assert result["success"] is True

    def test_post_init_failures_are_not_fatal(self, env):
        env.reset.side_effect = RuntimeError("no cache")
        env.gcs.load.side_effect = RuntimeError("no config")
        env.grant.side_effect = RuntimeError("no grants")

        result = RegistrySettings.initialize_registry_result(MagicMock(), MagicMock())

        assert result == {"success": True, "message": "Registry initialized"}

    def test_no_grant_summary_omits_permissions(self, env):
        env.grant.return_value = None
        result = RegistrySettings.initialize_registry_result(MagicMock(), MagicMock())
        assert "permissions" not in result

    def test_unconfigured_registry_raises_validation(self, env):
        env.svc.cfg.is_configured = False
        with pytest.raises(ValidationError, match="must be configured first"):
            RegistrySettings.initialize_registry_result(MagicMock(), MagicMock())

    def test_missing_client_raises_validation(self, env, monkeypatch):
        monkeypatch.setattr(_rs, "get_databricks_client", lambda d, s: None)
        with pytest.raises(ValidationError, match="Databricks not configured"):
            RegistrySettings.initialize_registry_result(MagicMock(), MagicMock())

    def test_failed_initialize_raises_infrastructure_with_detail(self, env):
        env.svc.initialize.return_value = (False, "CREATE SCHEMA denied")
        with pytest.raises(InfrastructureError) as exc_info:
            RegistrySettings.initialize_registry_result(MagicMock(), MagicMock())
        assert exc_info.value.detail == "CREATE SCHEMA denied"

    def test_unexpected_error_is_wrapped(self, env):
        env.rs_cls.from_context.side_effect = KeyError("boom")
        with pytest.raises(InfrastructureError, match="Initialize registry failed"):
            RegistrySettings.initialize_registry_result(MagicMock(), MagicMock())


# ---------------------------------------------------------------------------
# Grants
# ---------------------------------------------------------------------------


class TestRegistryGrantAppNames:
    def test_app_then_mcp_companion(self, monkeypatch):
        monkeypatch.delenv("MCP_APP_NAME", raising=False)
        names = RegistrySettings._registry_grant_app_names(
            SimpleNamespace(ontobricks_app_name=" ontobricks-07x ")
        )
        assert names == ["ontobricks-07x", "mcp-ontobricks-07x"]

    def test_deduplicates_names(self, monkeypatch):
        monkeypatch.setattr(_rs, "resolve_mcp_app_name", lambda name: name)
        assert RegistrySettings._registry_grant_app_names(
            SimpleNamespace(ontobricks_app_name="same")
        ) == ["same"]


class TestGrantRegistryPermissions:
    def test_unconfigured_registry_returns_none(self, rcfg):
        rcfg["cfg"] = _rcfg(configured=False)
        assert RegistrySettings._grant_registry_permissions(MagicMock(), MagicMock()) is None

    def test_no_app_name_raises_validation(self, rcfg, monkeypatch):
        monkeypatch.setattr(
            SettingsService, "_registry_grant_app_names", staticmethod(lambda s: [])
        )
        with pytest.raises(ValidationError, match="ONTOBRICKS_APP_NAME"):
            RegistrySettings._grant_registry_permissions(MagicMock(), MagicMock())

    def test_grants_to_app_names_on_trimmed_catalog(self, rcfg, factory, monkeypatch):
        rcfg["cfg"] = _rcfg(catalog=" main ")
        monkeypatch.setattr(
            SettingsService,
            "_registry_grant_app_names",
            staticmethod(lambda s: ["app", "mcp-app"]),
        )
        factory.lakebase.return_value.grant_app_permissions.return_value = {"ok": 1}

        result = RegistrySettings._grant_registry_permissions(MagicMock(), MagicMock())

        assert result == {"ok": 1}
        factory.lakebase.return_value.grant_app_permissions.assert_called_once_with(
            app_names=["app", "mcp-app"], uc_catalog="main"
        )

    def test_missing_psycopg_raises_infrastructure(self, rcfg, factory, monkeypatch):
        monkeypatch.setattr(
            SettingsService, "_registry_grant_app_names", staticmethod(lambda s: ["app"])
        )
        factory.lakebase.side_effect = ImportError("psycopg")
        with pytest.raises(InfrastructureError, match="psycopg"):
            RegistrySettings._grant_registry_permissions(MagicMock(), MagicMock())


class TestGrantRegistryPermissionsResult:
    @pytest.fixture(autouse=True)
    def _inline(self, monkeypatch):
        monkeypatch.setattr(_rs, "run_blocking", _inline_run_blocking)

    async def test_unconfigured_registry_raises_validation(self, rcfg):
        rcfg["cfg"] = _rcfg(configured=False)
        with pytest.raises(ValidationError):
            await RegistrySettings.grant_registry_permissions_result(MagicMock(), MagicMock())

    async def test_returns_summary(self, rcfg, monkeypatch):
        monkeypatch.setattr(
            SettingsService,
            "_grant_registry_permissions",
            staticmethod(lambda sm, st: {"granted": 2}),
        )
        assert await RegistrySettings.grant_registry_permissions_result(
            MagicMock(), MagicMock()
        ) == {"granted": 2}

    async def test_none_summary_means_backend_unavailable(self, rcfg, monkeypatch):
        monkeypatch.setattr(
            SettingsService, "_grant_registry_permissions", staticmethod(lambda sm, st: None)
        )
        with pytest.raises(InfrastructureError, match="not available"):
            await RegistrySettings.grant_registry_permissions_result(MagicMock(), MagicMock())

    @pytest.mark.parametrize("exc", [ValidationError("v"), InfrastructureError("i")])
    async def test_domain_errors_propagate_unchanged(self, rcfg, monkeypatch, exc):
        def _raise(sm, st):
            raise exc

        monkeypatch.setattr(SettingsService, "_grant_registry_permissions", staticmethod(_raise))
        with pytest.raises(type(exc)) as exc_info:
            await RegistrySettings.grant_registry_permissions_result(MagicMock(), MagicMock())
        assert exc_info.value is exc

    async def test_unexpected_error_is_wrapped(self, rcfg, monkeypatch):
        def _raise(sm, st):
            raise RuntimeError("pg down")

        monkeypatch.setattr(SettingsService, "_grant_registry_permissions", staticmethod(_raise))
        with pytest.raises(InfrastructureError, match="pg down"):
            await RegistrySettings.grant_registry_permissions_result(MagicMock(), MagicMock())

