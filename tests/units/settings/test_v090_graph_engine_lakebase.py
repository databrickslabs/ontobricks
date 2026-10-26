"""Unit tests for :class:`GraphEngineLakebaseSettings` (extracted in 0.9.0).

psycopg, the Databricks SDK ``api_client`` and Lakebase auth objects are
replaced by in-process doubles; calls go through ``SettingsService``.
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
from back.objects.domain.settings.graphengine.GraphEngineLakebaseSettings import (
    GraphEngineLakebaseSettings,
)
from back.objects.domain.settings.SettingsService import SettingsService

pytestmark = pytest.mark.unit

_ss = importlib.import_module("back.objects.domain.settings.SettingsService")
_pool_mod = importlib.import_module("back.core.graphdb.lakebase.pool")
_dbx_pkg = importlib.import_module("back.core.databricks")
_lb_pkg = importlib.import_module("back.core.databricks.lakebase")
_synced_mod = importlib.import_module("back.core.graphdb.lakebase.SyncedTableManager")
_prov_mod = importlib.import_module("back.core.graphdb.lakebase.LakebaseGraphProvisioner")
_tm_pkg = importlib.import_module("back.core.task_manager")

REG = {"catalog": "cat", "schema": "sch", "volume": "vol"}
BRANCH = "projects/p1/branches/main"


# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------


class _ScriptedCursor:
    """Cursor returning one scripted result per ``execute`` call."""

    def __init__(self, results, log):
        self._results = list(results)
        self._log = log
        self._current = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self._log.append((" ".join(sql.split()), params))
        self._current = self._results.pop(0) if self._results else []

    def fetchone(self):
        return self._current[0] if self._current else None

    def fetchall(self):
        return list(self._current or [])


class _FakePsycopg:
    def __init__(self, results=(), connect_exc=None):
        self.results = list(results)
        self.connect_exc = connect_exc
        self.connect_kwargs = None
        self.sql = []

    def connect(self, **kwargs):
        self.connect_kwargs = kwargs
        if self.connect_exc is not None:
            raise self.connect_exc
        outer = self

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def cursor(self):
                return _ScriptedCursor(outer.results, outer.sql)

        return _Conn()


def _auth(available=True, host="lb-host", branch_path=BRANCH, database="regdb"):
    auth = MagicMock()
    auth.is_available = available
    auth.host = host
    auth.branch_path = branch_path
    auth.database = database
    auth.kwargs.side_effect = lambda application_name: {
        "host": host,
        "dbname": "default",
        "application_name": application_name,
    }
    return auth


def _patch_psycopg(fake):
    return patch.object(_pool_mod, "_require_psycopg", return_value=(fake, None))


def _patch_engine_auth(auth, db=""):
    return patch.object(SettingsService, "_graph_engine_auth", return_value=(auth, db))


def _ws(api):
    return patch("databricks.sdk.WorkspaceClient", return_value=SimpleNamespace(api_client=api))


@pytest.fixture
def ctx():
    gcs = MagicMock()
    gcs.get_graph_engine_config.return_value = {}
    with patch.object(
        SettingsService, "_resolve_context", return_value=(MagicMock(), "h", "t", REG)
    ), patch.object(_ss, "global_config_service", gcs):
        yield gcs


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


class TestLakebaseHealth:
    def test_context_failure_maps_to_infrastructure_error(self, ctx):
        ctx.load.side_effect = RuntimeError("down")
        with pytest.raises(InfrastructureError, match="Could not load graph engine config"):
            SettingsService.graph_engine_lakebase_health_result(MagicMock(), MagicMock())

    def test_configured_branch_uses_branch_auth_and_reports_missing_schema(
        self, ctx, monkeypatch
    ):
        monkeypatch.setenv("PGDATABASE", "registry_db")
        monkeypatch.setenv("PGPORT", "6543")
        ctx.get_graph_engine_config.return_value = {
            "lakebase": {"lakebase_branch": BRANCH, "database": "graph_db", "schema": "g"}
        }
        auth = _auth()
        type(auth).host = property(lambda self: (_ for _ in ()).throw(RuntimeError("x")))
        monkeypatch.setenv("PGHOST", "pg-fallback")
        fake = _FakePsycopg(results=[[(False,)]])
        with patch.object(_lb_pkg, "BranchLakebaseAuth", return_value=auth) as bla, patch.object(
            _dbx_pkg, "get_lakebase_auth"
        ) as bound, _patch_psycopg(fake):
            out = SettingsService.graph_engine_lakebase_health_result(
                MagicMock(), MagicMock()
            )
        bla.assert_called_once_with(BRANCH, "graph_db")
        bound.assert_not_called()
        assert fake.connect_kwargs["dbname"] == "graph_db"
        assert out["success"] is True
        assert out["schema_exists"] is False
        assert out["host"] == "pg-fallback"
        assert out["port"] == 6543
        assert out["registry_database"] == "registry_db"
        assert "does not exist yet" in out["message"]

    def test_missing_psycopg_is_infrastructure_error(self, ctx, monkeypatch):
        monkeypatch.setenv("PGDATABASE", "registry_db")
        with patch.object(_dbx_pkg, "get_lakebase_auth", return_value=_auth()), patch.object(
            _pool_mod, "_require_psycopg", side_effect=ImportError("psycopg")
        ):
            with pytest.raises(InfrastructureError, match="missing psycopg"):
                SettingsService.graph_engine_lakebase_health_result(
                    MagicMock(), MagicMock()
                )

    def test_unavailable_bound_auth_is_validation_error(self, ctx):
        with patch.object(
            _dbx_pkg, "get_lakebase_auth", return_value=_auth(available=False)
        ):
            with pytest.raises(ValidationError, match="Lakebase not available"):
                SettingsService.graph_engine_lakebase_health_result(
                    MagicMock(), MagicMock()
                )


# ---------------------------------------------------------------------------
# Projects / branches / databases
# ---------------------------------------------------------------------------


class TestLakebaseProjects:
    def test_projects_without_name_skipped(self):
        api = MagicMock()
        with _ws(api), patch(
            "back.core.databricks.lakebase.LakebaseProjectService.LakebaseProjectService.list_projects",
            return_value=[{"name": ""}, {"name": "projects/a"}],
        ):
            out = SettingsService.graph_engine_lakebase_projects_result(
                MagicMock(), MagicMock()
            )
        assert out == {
            "success": True,
            "projects": [{"name": "projects/a", "short_name": "a", "state": ""}],
        }

    def test_missing_api_client(self):
        with _ws(None):
            with pytest.raises(InfrastructureError, match="api_client unavailable"):
                SettingsService.graph_engine_lakebase_projects_result(
                    MagicMock(), MagicMock()
                )

    def test_listing_failure_wrapped(self):
        with patch("databricks.sdk.WorkspaceClient", side_effect=RuntimeError("auth")):
            with pytest.raises(InfrastructureError, match="list Lakebase projects failed"):
                SettingsService.graph_engine_lakebase_projects_result(
                    MagicMock(), MagicMock()
                )


class TestLakebaseBranches:
    def test_project_path_required(self):
        with pytest.raises(ValidationError, match="project_path is required"):
            SettingsService.graph_engine_lakebase_branches_result("", MagicMock(), MagicMock())

    def test_short_project_name_normalised_and_branches_mapped(self):
        api = MagicMock()
        api.do.return_value = {
            "branches": [
                {"name": "projects/p1/branches/main", "status": {"state": "READY"}},
                {"name": ""},
            ]
        }
        with _ws(api):
            out = SettingsService.graph_engine_lakebase_branches_result(
                "p1", MagicMock(), MagicMock()
            )
        api.do.assert_called_once_with("GET", "/api/2.0/postgres/projects/p1/branches")
        assert out == {
            "success": True,
            "branches": [
                {"name": "projects/p1/branches/main", "short_name": "main", "state": "READY"}
            ],
        }

    def test_full_project_path_kept(self):
        api = MagicMock()
        api.do.return_value = None
        with _ws(api):
            out = SettingsService.graph_engine_lakebase_branches_result(
                "projects/p1", MagicMock(), MagicMock()
            )
        api.do.assert_called_once_with("GET", "/api/2.0/postgres/projects/p1/branches")
        assert out["branches"] == []

    def test_missing_api_client(self):
        with _ws(None):
            with pytest.raises(InfrastructureError, match="api_client unavailable"):
                SettingsService.graph_engine_lakebase_branches_result(
                    "p1", MagicMock(), MagicMock()
                )

    def test_rest_failure_wrapped(self):
        api = MagicMock()
        api.do.side_effect = RuntimeError("404")
        with _ws(api):
            with pytest.raises(InfrastructureError, match="list Lakebase branches failed"):
                SettingsService.graph_engine_lakebase_branches_result(
                    "p1", MagicMock(), MagicMock()
                )


class TestLakebasePgDatabases:
    def test_branch_path_required(self):
        with pytest.raises(ValidationError, match="branch_path is required"):
            SettingsService.graph_engine_lakebase_pg_databases_result(
                "", MagicMock(), MagicMock()
            )

    def test_postgres_names_sorted_and_unnamed_skipped(self):
        api = MagicMock()
        api.do.return_value = {
            "databases": [
                {"status": {"postgres_database": "zeta"}},
                {"status": {}},
                {"status": {"postgres_database": "alpha"}},
            ]
        }
        with _ws(api):
            out = SettingsService.graph_engine_lakebase_pg_databases_result(
                BRANCH, MagicMock(), MagicMock()
            )
        api.do.assert_called_once_with("GET", f"/api/2.0/postgres/{BRANCH}/databases")
        assert out == {"success": True, "databases": ["alpha", "zeta"]}

    def test_missing_api_client(self):
        with _ws(None):
            with pytest.raises(InfrastructureError, match="api_client unavailable"):
                SettingsService.graph_engine_lakebase_pg_databases_result(
                    BRANCH, MagicMock(), MagicMock()
                )

    def test_rest_failure_wrapped(self):
        api = MagicMock()
        api.do.side_effect = RuntimeError("boom")
        with _ws(api):
            with pytest.raises(InfrastructureError, match="Postgres databases failed"):
                SettingsService.graph_engine_lakebase_pg_databases_result(
                    BRANCH, MagicMock(), MagicMock()
                )


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class TestLakebasePgSchemas:
    def test_form_values_forwarded_and_schemas_listed(self):
        fake = _FakePsycopg(results=[[("graph",), ("public",)]])
        auth = _auth()
        with _patch_engine_auth(auth, "graph_db") as gea, _patch_psycopg(fake):
            out = SettingsService.graph_engine_lakebase_pg_schemas_result(
                "graph_db", MagicMock(), MagicMock(), BRANCH
            )
        assert gea.call_args.kwargs == {
            "form_branch_path": BRANCH,
            "form_database": "graph_db",
        }
        assert fake.connect_kwargs["dbname"] == "graph_db"
        assert fake.connect_kwargs["application_name"] == "ontobricks-schema-list"
        assert out == {"success": True, "schemas": ["graph", "public"]}

    def test_unbound_auth_rejected(self):
        with _patch_engine_auth(_auth(available=False)):
            with pytest.raises(ValidationError, match="not bound"):
                SettingsService.graph_engine_lakebase_pg_schemas_result(
                    "", MagicMock(), MagicMock()
                )

    def test_missing_psycopg(self):
        with _patch_engine_auth(_auth()), patch.object(
            _pool_mod, "_require_psycopg", side_effect=ImportError("x")
        ):
            with pytest.raises(InfrastructureError, match="missing psycopg"):
                SettingsService.graph_engine_lakebase_pg_schemas_result(
                    "", MagicMock(), MagicMock()
                )

    def test_connection_failure_wrapped(self):
        fake = _FakePsycopg(connect_exc=RuntimeError("refused"))
        with _patch_engine_auth(_auth()), _patch_psycopg(fake):
            with pytest.raises(InfrastructureError, match="Postgres schemas failed"):
                SettingsService.graph_engine_lakebase_pg_schemas_result(
                    "", MagicMock(), MagicMock()
                )
        assert fake.connect_kwargs["dbname"] == "default"


# ---------------------------------------------------------------------------
# Provision
# ---------------------------------------------------------------------------


class _SyncThread:
    def __init__(self, target, daemon=False):
        self._target = target

    def start(self):
        self._target()


class TestLakebaseProvision:
    def _params(self, **overrides):
        params = {
            "name": "graph-proj",
            "database": "graph_db",
            "schema": "graph",
            "grant_uc_catalog": True,
        }
        params.update(overrides)
        return params

    def _run(self, params, settings=None):
        return SettingsService.graph_engine_lakebase_provision_result(
            params, "admin@x.com", "tok", MagicMock(), settings or MagicMock()
        )

    def test_non_admin_rejected_before_validation(self, ctx):
        with patch.object(_ss, "is_databricks_app", return_value=True), patch.object(
            _ss.permission_service, "is_admin", return_value=False
        ):
            with pytest.raises(AuthorizationError):
                self._run({})

    @pytest.mark.parametrize(
        "overrides, match",
        [
            ({"name": " "}, "instance/project name is required"),
            ({"database": ""}, "Postgres database name is required"),
            ({"schema": "bad-schema!"}, "Invalid Postgres schema identifier"),
        ],
    )
    def test_parameter_validation(self, ctx, overrides, match):
        with patch.object(_ss, "is_databricks_app", return_value=False):
            with pytest.raises(ValidationError, match=match):
                self._run(self._params(**overrides))

    def test_pguser_required(self, ctx, monkeypatch):
        monkeypatch.delenv("PGUSER", raising=False)
        with patch.object(_ss, "is_databricks_app", return_value=False):
            with pytest.raises(ValidationError, match="PGUSER is not set"):
                self._run(self._params())

    def test_app_mode_without_app_name_rejected(self, ctx, monkeypatch):
        monkeypatch.setenv("PGUSER", "sp")
        settings = SimpleNamespace(ontobricks_app_name="")
        with patch.object(_ss, "is_databricks_app", return_value=True), patch.object(
            _ss.permission_service, "is_admin", return_value=True
        ), patch.object(_ss, "resolve_mcp_app_name", return_value=""):
            with pytest.raises(ValidationError, match="Could not determine the app name"):
                self._run(self._params(), settings)

    def test_app_mode_grants_both_apps_and_uc_catalog_for_managed_sync(
        self, ctx, monkeypatch
    ):
        monkeypatch.setenv("PGUSER", "sp")
        ctx.get_graph_engine_config.return_value = {
            "lakebase": {"sync_mode": "managed_synced", "sync_uc_catalog": " uc_cat "}
        }
        settings = SimpleNamespace(ontobricks_app_name="ontobricks")
        tm = MagicMock()
        tm.create_task.return_value = SimpleNamespace(id="task-1")
        with patch.object(_ss, "is_databricks_app", return_value=True), patch.object(
            _ss.permission_service, "is_admin", return_value=True
        ), patch.object(
            _ss, "resolve_mcp_app_name", return_value="ontobricks-mcp"
        ) as mcp, patch.object(_tm_pkg, "get_task_manager", return_value=tm), patch.object(
            _prov_mod, "LakebaseGraphProvisioner"
        ) as prov, patch("threading.Thread", _SyncThread):
            out = self._run(self._params(mcp_app_name=" explicit "), settings)

        assert out == {
            "success": True,
            "task_id": "task-1",
            "message": "Lakebase graph DB provisioning started",
        }
        assert mcp.call_args.kwargs == {"explicit": "explicit"}
        kwargs = prov.call_args.kwargs
        assert kwargs["app_names"] == ["ontobricks", "ontobricks-mcp"]
        assert kwargs["uc_catalog"] == "uc_cat"
        assert kwargs["sync_mode"] == "managed_synced"
        assert kwargs["pg_user"] == "sp"
        assert kwargs["operator_email"] == "admin@x.com"
        assert kwargs["task_id"] == "task-1"
        prov.return_value.run.assert_called_once()
        assert tm.create_task.call_args.kwargs["task_type"] == "lakebase_provision"

    def test_app_name_not_duplicated_when_mcp_resolves_same(self, ctx, monkeypatch):
        monkeypatch.setenv("PGUSER", "sp")
        tm = MagicMock()
        tm.create_task.return_value = SimpleNamespace(id="t")
        with patch.object(_ss, "is_databricks_app", return_value=True), patch.object(
            _ss.permission_service, "is_admin", return_value=True
        ), patch.object(_ss, "resolve_mcp_app_name", return_value="ontobricks"), patch.object(
            _tm_pkg, "get_task_manager", return_value=tm
        ), patch.object(_prov_mod, "LakebaseGraphProvisioner") as prov, patch(
            "threading.Thread", _SyncThread
        ):
            self._run(self._params(), SimpleNamespace(ontobricks_app_name="ontobricks"))
        assert prov.call_args.kwargs["app_names"] == ["ontobricks"]
        assert prov.call_args.kwargs["uc_catalog"] == ""


# ---------------------------------------------------------------------------
# Saved-config helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def saved_cfg():
    gcs = MagicMock()
    with patch.object(_ss, "get_domain", return_value=MagicMock()), patch.object(
        _ss, "get_databricks_host_and_token", return_value=("h", "t")
    ), patch.object(_ss.RegistryCfg, "from_domain") as from_domain, patch.object(
        _ss, "global_config_service", gcs
    ):
        from_domain.return_value.as_dict.return_value = REG
        yield gcs


class TestGraphEngineDatabase:
    def test_returns_saved_database(self, saved_cfg):
        saved_cfg.get_graph_engine_config.return_value = {"lakebase": {"database": " gdb "}}
        assert SettingsService._graph_engine_database(MagicMock(), MagicMock()) == "gdb"

    def test_failure_returns_empty_string(self, saved_cfg):
        saved_cfg.get_graph_engine_config.side_effect = RuntimeError("down")
        assert SettingsService._graph_engine_database(MagicMock(), MagicMock()) == ""


class TestGraphEngineAuth:
    def test_form_values_take_priority(self, saved_cfg):
        saved_cfg.get_graph_engine_config.return_value = {
            "lakebase": {"lakebase_branch": "projects/saved/branches/b", "database": "saved"}
        }
        with patch.object(_lb_pkg, "BranchLakebaseAuth") as bla:
            auth, db = SettingsService._graph_engine_auth(
                MagicMock(), MagicMock(), f" {BRANCH} ", " form_db "
            )
        bla.assert_called_once_with(BRANCH, "form_db")
        assert auth is bla.return_value
        assert db == "form_db"

    def test_saved_branch_and_database_fill_gaps(self, saved_cfg):
        saved_cfg.get_graph_engine_config.return_value = {
            "lakebase": {"lakebase_branch": BRANCH, "database": "saved"}
        }
        with patch.object(_lb_pkg, "BranchLakebaseAuth") as bla:
            _, db = SettingsService._graph_engine_auth(MagicMock(), MagicMock())
        bla.assert_called_once_with(BRANCH, "saved")
        assert db == "saved"

    def test_falls_back_to_bound_auth_when_config_unreadable(self, saved_cfg):
        saved_cfg.get_graph_engine_config.side_effect = RuntimeError("down")
        bound = _auth()
        with patch.object(_dbx_pkg, "get_lakebase_auth", return_value=bound), patch.object(
            _lb_pkg, "BranchLakebaseAuth"
        ) as bla:
            auth, db = SettingsService._graph_engine_auth(MagicMock(), MagicMock())
        bla.assert_not_called()
        assert auth is bound
        assert db == ""


class TestLakebaseKwargsForBranch:
    def _api(self, endpoints, token="jwt"):
        api = MagicMock()

        def _do(method, path, body=None):
            if method == "GET":
                return {"endpoints": endpoints}
            return {"token": token}

        api.do.side_effect = _do
        return api

    def test_resolves_first_endpoint_with_host_and_mints_jwt(self, monkeypatch):
        monkeypatch.setenv("PGUSER", "me@x.com")
        monkeypatch.setenv("PGPORT", "5433")
        api = self._api(
            [
                {"name": "ep-0", "status": {"hosts": {}}},
                {"name": "ep-1", "status": {"hosts": {"host": " h1 "}}},
            ]
        )
        with _ws(api):
            kwargs = SettingsService._lakebase_kwargs_for_branch(BRANCH, "", "app")
        api.do.assert_any_call(
            "POST", "/api/2.0/postgres/credentials", body={"endpoint": "ep-1"}
        )
        assert kwargs == {
            "host": "h1",
            "port": 5433,
            "user": "me@x.com",
            "password": "jwt",
            "dbname": "postgres",
            "sslmode": "require",
            "connect_timeout": 10,
            "application_name": "app",
        }

    def test_missing_api_client(self):
        with _ws(None):
            with pytest.raises(RuntimeError, match="api_client unavailable"):
                SettingsService._lakebase_kwargs_for_branch(BRANCH, "db", "app")

    def test_no_active_endpoint(self):
        with _ws(self._api([])):
            with pytest.raises(RuntimeError, match="No active endpoint"):
                SettingsService._lakebase_kwargs_for_branch(BRANCH, "db", "app")

    def test_jwt_mint_failure(self):
        api = self._api([{"name": "ep", "status": {"hosts": {"host": "h"}}}], token="")
        with _ws(api):
            with pytest.raises(RuntimeError, match="Failed to mint Lakebase JWT"):
                SettingsService._lakebase_kwargs_for_branch(BRANCH, "db", "app")

    def test_pguser_required(self, monkeypatch):
        monkeypatch.delenv("PGUSER", raising=False)
        api = self._api([{"name": "ep", "status": {"hosts": {"host": "h"}}}])
        with _ws(api):
            with pytest.raises(RuntimeError, match="PGUSER is not set"):
                SettingsService._lakebase_kwargs_for_branch(BRANCH, "db", "app")


# ---------------------------------------------------------------------------
# Objects listing
# ---------------------------------------------------------------------------


class TestLakebaseObjects:
    def _rcfg(self, schema=""):
        return patch.object(
            _ss.RegistryCfg, "from_session", return_value=SimpleNamespace(lakebase_schema=schema)
        )

    def test_lists_schemas_tables_and_views(self):
        fake = _FakePsycopg(
            results=[
                [("app_sp",)],
                [("graph", "app_sp"), ("reg", "deployer")],
                [("graph", "t1", "app_sp")],
                [("reg", "v1", "deployer")],
            ]
        )
        with _patch_psycopg(fake), _patch_engine_auth(_auth(), "gdb"), self._rcfg("reg"):
            out = SettingsService.graph_engine_lakebase_objects_result(
                "gdb", BRANCH, MagicMock(), MagicMock()
            )
        assert out == {
            "success": True,
            "current_user": "app_sp",
            "registry_schema": "reg",
            "schemas": [
                {"name": "graph", "owner": "app_sp"},
                {"name": "reg", "owner": "deployer"},
            ],
            "tables": [{"schema": "graph", "name": "t1", "owner": "app_sp"}],
            "views": [{"schema": "reg", "name": "v1", "owner": "deployer"}],
        }
        assert fake.connect_kwargs["dbname"] == "gdb"
        assert fake.connect_kwargs["application_name"] == "ontobricks-obj-list"
        assert fake.sql[2][1] == (["graph", "reg"],)

    def test_no_visible_schema_skips_table_queries(self):
        fake = _FakePsycopg(results=[[], [], [], []])
        with _patch_psycopg(fake), _patch_engine_auth(_auth()), self._rcfg(""):
            out = SettingsService.graph_engine_lakebase_objects_result(
                "", "", MagicMock(), MagicMock()
            )
        assert out["current_user"] == ""
        assert out["registry_schema"] == "ontobricks_registry"
        assert out["schemas"] == out["tables"] == out["views"] == []
        assert "WHERE FALSE" in fake.sql[2][0]
        assert "WHERE FALSE" in fake.sql[3][0]

    def test_unavailable_auth(self):
        with _patch_psycopg(_FakePsycopg()), _patch_engine_auth(_auth(available=False)):
            with pytest.raises(ValidationError, match="Lakebase not available"):
                SettingsService.graph_engine_lakebase_objects_result(
                    "", "", MagicMock(), MagicMock()
                )

    def test_missing_psycopg(self):
        with patch.object(_pool_mod, "_require_psycopg", side_effect=ImportError("x")):
            with pytest.raises(InfrastructureError, match="missing psycopg"):
                SettingsService.graph_engine_lakebase_objects_result(
                    "", "", MagicMock(), MagicMock()
                )

    def test_query_failure_wrapped(self):
        fake = _FakePsycopg(connect_exc=RuntimeError("refused"))
        with _patch_psycopg(fake), _patch_engine_auth(_auth()):
            with pytest.raises(InfrastructureError, match="database objects failed"):
                SettingsService.graph_engine_lakebase_objects_result(
                    "", "", MagicMock(), MagicMock()
                )


# ---------------------------------------------------------------------------
# Sync objects (UC + Lakeflow)
# ---------------------------------------------------------------------------


class TestLakebaseSyncObjects:
    def _call(self):
        return SettingsService.graph_engine_lakebase_sync_objects_result(
            "", "", MagicMock(), MagicMock()
        )

    def test_unconfigured_catalog_or_schema(self, ctx):
        with patch.object(
            _ss.RegistryCfg, "from_session", return_value=SimpleNamespace(catalog="")
        ):
            out = self._call()
        assert out["success"] is True
        assert out["sync_mode"] == "app_managed"
        assert out["uc_tables"] == []
        assert "not configured" in out["message"]

    def test_app_managed_lists_uc_tables_without_lakeflow_probe(self, ctx):
        ctx.get_graph_engine_config.return_value = {
            "lakebase": {"schema": "graph", "sync_mode": "app_managed"}
        }
        api = MagicMock()
        api.do.return_value = {
            "tables": [
                {"name": "t_sync", "table_type": "MANAGED"},
                {"name": "t", "full_name": "x.y.t", "table_type": None},
            ]
        }
        with patch.object(
            _ss.RegistryCfg, "from_session", return_value=SimpleNamespace(catalog="regcat")
        ), _ws(api), patch.object(_synced_mod, "SyncedTableManager") as mgr_cls:
            out = self._call()
        api.do.assert_called_once_with(
            "GET",
            "/api/2.1/unity-catalog/tables",
            query={"catalog_name": "regcat", "schema_name": "graph"},
        )
        mgr_cls.assert_not_called()
        assert out["uc_catalog"] == "regcat"
        assert out["uc_schema"] == "graph"
        assert out["uc_tables"] == [
            {
                "name": "t_sync",
                "full_name": "regcat.graph.t_sync",
                "table_type": "MANAGED",
                "is_sync": True,
                "state": "",
                "pipeline_id": "",
                "source_table": "",
                "error": "",
            },
            {
                "name": "t",
                "full_name": "x.y.t",
                "table_type": "",
                "is_sync": False,
                "state": "",
                "pipeline_id": "",
                "source_table": "",
                "error": "",
            },
        ]

    def test_managed_synced_attaches_lakeflow_state_per_sync_table(self, ctx):
        ctx.get_graph_engine_config.return_value = {
            "lakebase": {
                "sync_mode": "managed_synced",
                "sync_uc_catalog": "uc",
                "sync_uc_schema": "gs",
                "schema": "ignored",
            }
        }
        api = MagicMock()
        api.do.return_value = {
            "tables": [
                {"name": "a_sync"},
                {"name": "b_sync"},
                {"name": "c_sync"},
                {"name": "d_sync"},
                {"name": "plain"},
            ]
        }
        synced_attr = SimpleNamespace(spec=SimpleNamespace(source_table_full_name="uc.src.a"))
        synced_dict = {"spec": {"source_table_full_name": "uc.src.b"}}

        known = {
            "uc.gs.a_sync": synced_attr,
            "uc.gs.b_sync": synced_dict,
            "uc.gs.c_sync": None,
        }

        def _get(full_name):
            if full_name not in known:
                raise RuntimeError("rate limited")
            return known[full_name]

        mgr_cls = MagicMock()
        mgr_cls.return_value.get.side_effect = _get
        mgr_cls._extract_state.side_effect = lambda s: "ONLINE" if s is synced_attr else None
        mgr_cls._extract_pipeline_id.return_value = "pipe-1"
        with _ws(api), patch.object(_synced_mod, "SyncedTableManager", mgr_cls):
            out = self._call()

        rows = {r["name"]: r for r in out["uc_tables"]}
        assert out["sync_mode"] == "managed_synced"
        assert rows["a_sync"]["state"] == "ONLINE"
        assert rows["a_sync"]["source_table"] == "uc.src.a"
        assert rows["a_sync"]["pipeline_id"] == "pipe-1"
        assert rows["b_sync"]["state"] == "UNKNOWN"
        assert rows["b_sync"]["source_table"] == "uc.src.b"
        assert rows["c_sync"]["state"] == "NOT_FOUND"
        assert rows["d_sync"]["state"] == "ERROR"
        assert rows["d_sync"]["error"] == "rate limited"
        assert rows["plain"]["state"] == ""
        mgr_cls.return_value.get.assert_any_call("uc.gs.a_sync")
        assert all(
            c.args[0] != "uc.gs.plain" for c in mgr_cls.return_value.get.call_args_list
        )

    def test_missing_api_client(self, ctx):
        ctx.get_graph_engine_config.return_value = {
            "lakebase": {"sync_uc_catalog": "uc", "schema": "g"}
        }
        with _ws(None):
            with pytest.raises(InfrastructureError, match="api_client unavailable"):
                self._call()

    def test_unexpected_failure_wrapped(self, ctx):
        ctx.get_graph_engine_config.side_effect = RuntimeError("down")
        with pytest.raises(InfrastructureError, match="list Lakebase sync objects failed"):
            self._call()


# ---------------------------------------------------------------------------
# Drop Postgres object
# ---------------------------------------------------------------------------


class TestLakebaseDropObject:
    def _drop(self, kind, schema, name, database="", branch=""):
        return SettingsService.graph_engine_lakebase_drop_object_result(
            kind, schema, name, database, branch, MagicMock(), MagicMock()
        )

    def test_unknown_kind_rejected(self):
        with pytest.raises(ValidationError, match="kind must be one of"):
            self._drop("database", "", "x")

    @pytest.mark.parametrize("kind", ["table", "view"])
    def test_schema_required_for_tables_and_views(self, kind):
        with pytest.raises(ValidationError, match=f"schema is required for kind={kind}"):
            self._drop(kind, "", "x")

    @pytest.mark.parametrize(
        "kind, schema, name, ddl",
        [
            ("schema", "", 'we"ird', 'DROP SCHEMA IF EXISTS "we""ird" CASCADE'),
            ("table", "g", "t", 'DROP TABLE IF EXISTS "g"."t" CASCADE'),
            ("view", "g", "v", 'DROP VIEW IF EXISTS "g"."v" CASCADE'),
        ],
    )
    def test_quoted_ddl_executed_on_form_connection(self, kind, schema, name, ddl):
        fake = _FakePsycopg(results=[[]])
        with _patch_psycopg(fake), _patch_engine_auth(_auth(), "gdb") as gea:
            out = self._drop(kind, schema, name, database="gdb", branch=BRANCH)
        assert gea.call_args.kwargs == {"form_branch_path": BRANCH, "form_database": "gdb"}
        assert fake.sql == [(ddl, None)]
        assert fake.connect_kwargs["dbname"] == "gdb"
        assert fake.connect_kwargs["application_name"] == "ontobricks-obj-drop"
        assert out == {"success": True, "message": f"Dropped {kind}: {ddl}"}

    def test_unavailable_auth(self):
        with _patch_psycopg(_FakePsycopg()), _patch_engine_auth(_auth(available=False)):
            with pytest.raises(ValidationError, match="Lakebase not available"):
                self._drop("schema", "", "g")

    def test_missing_psycopg(self):
        with patch.object(_pool_mod, "_require_psycopg", side_effect=ImportError("x")):
            with pytest.raises(InfrastructureError, match="missing psycopg"):
                self._drop("schema", "", "g")

    def test_execution_failure_wrapped(self):
        fake = _FakePsycopg(connect_exc=RuntimeError("permission denied"))
        with _patch_psycopg(fake), _patch_engine_auth(_auth()):
            with pytest.raises(InfrastructureError, match="Lakebase drop object failed"):
                self._drop("schema", "", "g")
        assert fake.connect_kwargs["dbname"] == "default"


# ---------------------------------------------------------------------------
# Postgres roles / grant superuser
# ---------------------------------------------------------------------------


def _roles_payload():
    return {
        "roles": [
            {
                "name": f"{BRANCH}/roles/r-admin",
                "status": {
                    "postgres_role": "Admin@X.com",
                    "membership_roles": ["DATABRICKS_SUPERUSER"],
                },
            },
            {"name": f"{BRANCH}/roles/r-dev", "status": {"postgres_role": "dev@x.com"}},
            {"name": f"{BRANCH}/roles/r-none", "status": {}},
        ]
    }


class TestLakebasePgRoles:
    def test_roles_and_app_users_listed(self, ctx):
        api = MagicMock()
        api.do.return_value = _roles_payload()
        principals = {
            "users": [
                {"email": "a@x.com", "display_name": "Alice"},
                {"email": "b@x.com"},
                {"email": " "},
            ]
        }
        settings = SimpleNamespace(ontobricks_app_name="ontobricks")
        with _patch_engine_auth(_auth()), _ws(api), patch.object(
            _ss.permission_service, "list_app_principals", return_value=principals
        ) as lap:
            out = SettingsService.graph_engine_lakebase_pg_roles_result(MagicMock(), settings)
        api.do.assert_called_once_with("GET", f"/api/2.0/postgres/{BRANCH}/roles")
        lap.assert_called_once_with("h", "t", "ontobricks")
        assert out == {
            "success": True,
            "branch_path": BRANCH,
            "roles": [
                {"email": "admin@x.com", "role_id": "r-admin", "has_superuser": True},
                {"email": "dev@x.com", "role_id": "r-dev", "has_superuser": False},
            ],
            "app_users": [
                {"email": "a@x.com", "display_name": "Alice"},
                {"email": "b@x.com", "display_name": "b@x.com"},
            ],
        }

    def test_app_user_lookup_failure_is_best_effort(self, ctx):
        api = MagicMock()
        api.do.return_value = {}
        with _patch_engine_auth(_auth()), _ws(api), patch.object(
            _ss.permission_service, "list_app_principals", side_effect=RuntimeError("403")
        ):
            out = SettingsService.graph_engine_lakebase_pg_roles_result(
                MagicMock(), SimpleNamespace(ontobricks_app_name="a")
            )
        assert out["roles"] == []
        assert out["app_users"] == []

    def test_unavailable_auth(self):
        with _patch_engine_auth(_auth(available=False)):
            with pytest.raises(ValidationError, match="Lakebase not available"):
                SettingsService.graph_engine_lakebase_pg_roles_result(MagicMock(), MagicMock())

    def test_missing_branch_path(self):
        with _patch_engine_auth(_auth(branch_path="")):
            with pytest.raises(ValidationError, match="Could not resolve Lakebase branch path"):
                SettingsService.graph_engine_lakebase_pg_roles_result(MagicMock(), MagicMock())


class TestLakebaseGrantSuperuser:
    def _grant(self, email):
        return SettingsService.graph_engine_lakebase_grant_superuser_result(
            email, MagicMock(), MagicMock()
        )

    def _api(self, roles_payload, create_op=None):
        api = MagicMock()

        def _do(method, path, body=None):
            if method == "GET":
                return roles_payload
            if method == "POST":
                return create_op
            return {}

        api.do.side_effect = _do
        return api

    def test_email_required(self):
        with pytest.raises(ValidationError, match="user_email is required"):
            self._grant("  ")

    def test_unavailable_auth(self):
        with _patch_engine_auth(_auth(available=False)):
            with pytest.raises(ValidationError, match="Lakebase not available"):
                self._grant("a@x.com")

    def test_missing_branch_path(self):
        with _patch_engine_auth(_auth(branch_path="")):
            with pytest.raises(ValidationError, match="Could not resolve Lakebase branch path"):
                self._grant("a@x.com")

    def test_existing_superuser_is_idempotent_noop(self):
        api = self._api(_roles_payload())
        with _patch_engine_auth(_auth()), _ws(api):
            out = self._grant("ADMIN@x.com")
        assert out["message"] == "ADMIN@x.com already has DATABRICKS_SUPERUSER"
        assert [c.args[0] for c in api.do.call_args_list] == ["GET"]

    def test_existing_role_is_patched_without_creating(self):
        api = self._api(_roles_payload())
        with _patch_engine_auth(_auth()), _ws(api), patch("time.sleep") as sleep:
            out = self._grant(" dev@x.com ")
        sleep.assert_not_called()
        api.do.assert_called_with(
            "PATCH",
            f"/api/2.0/postgres/{BRANCH}/roles/r-dev?update_mask=spec.membership_roles",
            body={"spec": {"membership_roles": ["DATABRICKS_SUPERUSER"]}},
        )
        assert out == {"success": True, "message": "DATABRICKS_SUPERUSER granted to dev@x.com"}

    def test_new_role_created_then_granted(self):
        api = self._api(
            {"roles": []},
            create_op={"name": f"{BRANCH}/roles/r-new/operations/op-1"},
        )
        with _patch_engine_auth(_auth()), _ws(api), patch("time.sleep") as sleep:
            self._grant("new@x.com")
        post = api.do.call_args_list[1]
        assert post.args == ("POST", f"/api/2.0/postgres/{BRANCH}/roles")
        assert post.kwargs["body"]["spec"] == {
            "identity_type": "USER",
            "postgres_role": "new@x.com",
            "auth_method": "LAKEBASE_OAUTH_V1",
        }
        sleep.assert_called_once()
        assert api.do.call_args_list[-1].args[1].endswith(
            "/roles/r-new?update_mask=spec.membership_roles"
        )

    @pytest.mark.parametrize(
        "op",
        [None, {"name": f"{BRANCH}/roles/operations/op-1"}, {"name": "operations/op"}],
    )
    def test_unparseable_create_operation_raises(self, op):
        api = self._api({"roles": []}, create_op=op)
        with _patch_engine_auth(_auth()), _ws(api), patch("time.sleep"):
            with pytest.raises(InfrastructureError, match="Could not create Postgres role"):
                self._grant("new@x.com")
        assert all(c.args[0] != "PATCH" for c in api.do.call_args_list)


def test_lakebase_class_is_the_delegation_target():
    """``SettingsService`` routes Lakebase calls to the extracted class."""
    with patch.object(
        GraphEngineLakebaseSettings, "graph_engine_lakebase_projects_result", return_value={"ok": 1}
    ) as target:
        assert SettingsService.graph_engine_lakebase_projects_result("s", "c") == {"ok": 1}
    target.assert_called_once_with("s", "c")
