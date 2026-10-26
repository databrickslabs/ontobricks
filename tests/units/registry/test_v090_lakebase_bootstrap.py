"""Real SQL of :class:`Bootstrap` against a scripted cursor.

Covers the initialise probe reasons, the permission diagnostic, the
idempotent initialise/upgrade path (registry row upserted, never
recreated), and the in-app GRANT orchestration.
"""

from __future__ import annotations

import sys
import types

import pytest

from back.objects.registry.store.lakebase.store import _KNOWN_TABLES

from ._v090_fake_pg import QSCHEMA, SCHEMA, FakeDB, make_store

pytestmark = pytest.mark.unit

_PROBE = "has_schema_privilege(current_user, %s, 'USAGE'), EXISTS"


# ---------------------------------------------------------------------------
# init_status / is_initialized
# ---------------------------------------------------------------------------


def test_init_status_ok_when_usage_table_and_row_exist():
    db = FakeDB()
    db.on(_PROBE, rows=[("db", "sp", True, True)])
    db.on("to_regclass", rows=[(True,)])
    store = make_store(db)

    assert store.init_status() == {"initialized": True, "reason": "ok", "error": None}
    probe = db.calls[0]
    assert probe.params == (SCHEMA, SCHEMA)
    assert db.calls[1].params == (f"{SCHEMA}.registries",)


def test_is_initialized_mirrors_init_status_flag():
    ok_db = FakeDB().on(_PROBE, rows=[("db", "sp", True, True)]).on("to_regclass", rows=[(True,)])
    assert make_store(ok_db).is_initialized() is True
    assert make_store(FakeDB()).is_initialized() is False


def test_init_status_reports_missing_usage_on_existing_schema():
    db = FakeDB().on(_PROBE, rows=[("db1", "sp-x", False, True)])

    status = make_store(db).init_status()

    assert status["initialized"] is False
    assert status["reason"] == "no_usage"
    assert "lacks USAGE on schema 'ontobricks_registry' in database 'db1'" in status["error"]
    assert not db.sql_with("to_regclass")


def test_init_status_reports_absent_schema_and_empty_probe():
    db = FakeDB().on(_PROBE, rows=[("db1", "sp-x", False, False)])
    status = make_store(db).init_status()
    assert status["reason"] == "no_usage"
    assert "does not exist in database 'db1'" in status["error"]

    status = make_store(FakeDB()).init_status()
    assert status["reason"] == "no_usage"
    assert "databricks_postgres" in status["error"], "falls back to effective database"


def test_init_status_no_registries_table():
    db = FakeDB().on(_PROBE, rows=[("d", "u", True, True)]).on("to_regclass", rows=[(False,)])

    assert make_store(db).init_status()["reason"] == "no_registries_table"


def test_init_status_no_registry_row_triggers_lookup():
    db = FakeDB().on(_PROBE, rows=[("d", "u", True, True)]).on("to_regclass", rows=[(True,)])
    store = make_store(db, registry_id=None)

    assert store.init_status()["reason"] == "no_registry_row"
    assert db.sql_with("FROM \"ontobricks_registry\".registries WHERE name = %s")


def test_init_status_adopts_existing_row_found_by_lookup():
    db = FakeDB().on(_PROBE, rows=[("d", "u", True, True)]).on("to_regclass", rows=[(True,)])
    db.on("registries WHERE name = %s", rows=[("reg-existing",)])
    store = make_store(db, registry_id=None)

    assert store.init_status()["initialized"] is True
    assert store._registry_id == "reg-existing"


def test_init_status_connect_failure():
    db = FakeDB()
    db.connect_error = RuntimeError("refused")

    status = make_store(db).init_status()

    assert status == {
        "initialized": False,
        "reason": "connect_failed",
        "error": "Lakebase probe failed: refused",
    }


# ---------------------------------------------------------------------------
# check_permissions
# ---------------------------------------------------------------------------


@pytest.fixture
def _psycopg_ok(monkeypatch):
    from back.objects.registry.store.lakebase import store as store_module

    monkeypatch.setattr(store_module, "_require_psycopg", lambda: (None, None))


def _by_id(result):
    return {c["id"]: c for c in result["checks"]}


def test_check_permissions_missing_schema_marks_every_table_missing(_psycopg_ok):
    db = FakeDB()
    db.on("SELECT current_database(), current_user", rows=[("db1", "sp")])
    db.on("EXISTS(SELECT 1 FROM pg_namespace", rows=[(False, False, False)])

    result = make_store(db).check_permissions()

    assert result["success"] is True
    checks = _by_id(result)
    assert checks["connect"]["status"] == "ok"
    assert checks["schema_exists"]["status"] == "error"
    assert {k for k, v in checks.items() if v["status"] == "missing"} == {
        f"tbl_{t}" for t in _KNOWN_TABLES
    }
    assert not db.sql_with("has_table_privilege")


def test_check_permissions_reports_privileges_per_table(_psycopg_ok):
    db = FakeDB()
    db.on("SELECT current_database(), current_user", rows=[("db1", "sp")])
    db.on("EXISTS(SELECT 1 FROM pg_namespace", rows=[(True, False, False)])
    db.on("SELECT relname FROM pg_class", rows=[("domains",), ("registries",)])
    db.on("has_table_privilege", rows=[(True, True, True, True)])
    db.on("has_table_privilege", rows=[(True, False, True, False)])

    result = make_store(db).check_permissions()

    assert result == {**result, "success": True, "database": "db1", "user": "sp", "schema": SCHEMA}
    checks = _by_id(result)
    assert checks["schema_usage"]["status"] == "error"
    assert 'GRANT USAGE ON SCHEMA "ontobricks_registry" TO "sp"' in checks["schema_usage"]["detail"]
    assert checks["schema_create"]["status"] == "warning"
    # sorted(): "domains" is checked before "registries"
    assert checks["tbl_domains"]["status"] == "ok"
    assert checks["tbl_registries"]["status"] == "error"
    assert "Missing: INSERT, DELETE" in checks["tbl_registries"]["detail"]
    assert checks["tbl_build_runs"]["status"] == "missing"
    priv_calls = db.sql_with("has_table_privilege")
    assert priv_calls[0].params == (f"{SCHEMA}.domains",) * 4


def test_check_permissions_all_ok_schema_privileges(_psycopg_ok):
    db = FakeDB()
    db.on("SELECT current_database(), current_user", rows=[("db1", "sp")])
    db.on("EXISTS(SELECT 1 FROM pg_namespace", rows=[(True, True, True)])

    checks = _by_id(make_store(db).check_permissions())

    assert checks["schema_usage"] == {
        "id": "schema_usage",
        "label": f"USAGE on schema '{SCHEMA}'",
        "status": "ok",
        "detail": None,
    }
    assert checks["schema_create"]["status"] == "ok"


def test_check_permissions_connection_failure(_psycopg_ok):
    db = FakeDB()
    db.connect_error = RuntimeError("refused")

    result = make_store(db).check_permissions()

    assert result["success"] is False
    assert result["error"] == "refused"
    assert result["database"] == "databricks_postgres"
    assert result["checks"] == [
        {"id": "connect", "label": "Connect to Lakebase", "status": "error", "detail": "refused"}
    ]


def test_check_permissions_midway_failure_keeps_collected_checks(_psycopg_ok):
    db = FakeDB()
    db.on("SELECT current_database(), current_user", rows=[("db1", "sp")])
    db.on("EXISTS(SELECT 1 FROM pg_namespace", raises=RuntimeError("timeout"))

    result = make_store(db).check_permissions()

    assert result["success"] is False
    assert [c["id"] for c in result["checks"]] == ["connect"]


# ---------------------------------------------------------------------------
# initialize / _ensure_registry_row
# ---------------------------------------------------------------------------


def test_initialize_applies_ddl_and_upserts_registry_row():
    db = FakeDB().on("INSERT INTO \"ontobricks_registry\".registries", rows=[("reg-9",)])
    store = make_store(db, registry_id=None)

    ok, msg = store.initialize(client=object())

    assert ok is True
    assert "lakebase.example/databricks_postgres" in msg
    assert store._registry_id == "reg-9"
    ddl = db.calls[0].sql
    assert "__SCHEMA__" not in ddl
    assert f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};" in ddl
    assert f"SET search_path TO {SCHEMA};" in ddl
    assert "CREATE TABLE IF NOT EXISTS registries" in ddl
    upsert = db.one("INSERT INTO \"ontobricks_registry\".registries")
    assert "ON CONFLICT (name) DO UPDATE" in upsert.sql, "existing registries are reused"
    assert upsert.params == (SCHEMA, "cat", "sch", "vol")
    assert db.sql_with("UPDATE \"ontobricks_registry\".global_config")
    assert db.calls[-1].sql == "SELECT 1"


def test_initialize_reports_failure():
    db = FakeDB().on(
        "INSERT INTO \"ontobricks_registry\".registries",
        raises=RuntimeError("perm"),
        once=False,
    )

    ok, msg = make_store(db, registry_id=None).initialize()

    assert ok is False
    assert msg == "Failed to initialise Lakebase registry: perm"


def test_initialize_continues_when_ddl_fails_as_non_owner():
    """App SP is DML-only: schema.sql owner errors must not block pinning the row."""
    db = FakeDB().on(
        "CREATE SCHEMA IF NOT EXISTS",
        raises=RuntimeError("must be owner of table registries"),
        once=False,
    )
    db.on("INSERT INTO \"ontobricks_registry\".registries", rows=[("reg-9",)])
    store = make_store(db, registry_id=None)

    ok, msg = store.initialize()

    assert ok is True
    assert store._registry_id == "reg-9"
    assert "lakebase.example/databricks_postgres" in msg


# ---------------------------------------------------------------------------
# grant_app_permissions
# ---------------------------------------------------------------------------


class _Api:
    def do(self, *a, **k):  # pragma: no cover - never reached through fakes
        return {}


@pytest.fixture
def grants(monkeypatch):
    """Stub the SDK client and the grants helpers; record their calls."""
    from back.core.databricks.lakebase import grants as grants_mod

    calls = {}
    state = {"sp_ids": {"mcp-app": "sp-1"}, "api": _Api()}

    sdk = types.ModuleType("databricks.sdk")

    class _WC:
        def __init__(self):
            self.api_client = state["api"]

    sdk.WorkspaceClient = _WC
    monkeypatch.setitem(sys.modules, "databricks.sdk", sdk)

    def resolve(api, names):
        calls["resolve"] = names
        return dict(state["sp_ids"]), ["resolve-warning"]

    def can_use(api, project, sp_ids):
        calls["can_use"] = project
        return ["CAN_USE"], []

    def schema(conn, sch, sp_ids):
        calls["schema"] = sch
        return ["USAGE"], ["schema-warning"]

    def uc(api, catalog, sp_ids):
        calls["uc"] = catalog
        return ["ALL_PRIVILEGES"], []

    monkeypatch.setattr(grants_mod, "resolve_app_service_principals", resolve)
    monkeypatch.setattr(grants_mod, "grant_can_use_on_project", can_use)
    monkeypatch.setattr(grants_mod, "grant_schema_privileges", schema)
    monkeypatch.setattr(grants_mod, "grant_uc_catalog", uc)
    return calls, state


def test_grant_app_permissions_runs_all_grant_phases(grants):
    calls, _ = grants

    result = make_store(FakeDB()).grant_app_permissions(app_names=["mcp-app"], uc_catalog="main")

    assert result == {
        "success": True,
        "granted": ["CAN_USE", "USAGE", "ALL_PRIVILEGES"],
        "warnings": ["resolve-warning", "schema-warning"],
        "error": None,
        "schema": SCHEMA,
        "apps": ["mcp-app"],
    }
    assert calls == {"resolve": ["mcp-app"], "can_use": "proj-1", "schema": SCHEMA, "uc": "main"}


def test_grant_app_permissions_without_catalog_skips_uc(grants):
    calls, _ = grants

    result = make_store(FakeDB()).grant_app_permissions(app_names=["mcp-app"])

    assert result["success"] is True
    assert "uc" not in calls


def test_grant_app_permissions_fails_when_no_principal_resolves(grants):
    _, state = grants
    state["sp_ids"] = {}

    result = make_store(FakeDB()).grant_app_permissions(app_names=["ghost"])

    assert result["success"] is False
    assert "Could not resolve any app service principal" in result["error"]
    assert result["warnings"] == ["resolve-warning"]


def test_grant_app_permissions_requires_api_client(grants):
    _, state = grants
    state["api"] = None

    result = make_store(FakeDB()).grant_app_permissions(app_names=["a"])

    assert result == {
        "success": False,
        "granted": [],
        "warnings": [],
        "error": "Databricks api_client unavailable",
    }


def test_grant_app_permissions_sdk_failure(monkeypatch, grants):
    class _Boom:
        def __init__(self):
            raise RuntimeError("no creds")

    sys.modules["databricks.sdk"].WorkspaceClient = _Boom

    result = make_store(FakeDB()).grant_app_permissions(app_names=["a"])

    assert result["success"] is False
    assert result["error"] == "Databricks SDK unavailable: no creds"


def test_grant_app_permissions_degrades_project_and_schema_failures(grants):
    calls, _ = grants

    class _Auth:
        host = "h"
        database = "d"

        @property
        def instance_name(self):
            raise RuntimeError("no project")

    db = FakeDB()
    db.connect_error = RuntimeError("pg down")
    store = make_store(db)
    store._auth = _Auth()

    result = store.grant_app_permissions(app_names=["mcp-app"])

    assert result["success"] is True
    assert result["granted"] == []
    assert any("CAN_USE grant (no project)" in w for w in result["warnings"])
    assert "Schema grants could not run (pg down)." in result["warnings"]
    assert "can_use" not in calls


def test_ensure_registry_row_returns_string_id():
    db = FakeDB().on("INSERT INTO", rows=[(12345,)])

    assert make_store(db)._ensure_registry_row() == "12345"
