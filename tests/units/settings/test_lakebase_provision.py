"""Unit tests for the in-app Lakebase graph DB provisioner.

Covers the :class:`LakebaseGraphProvisioner` step sequence (with the
Databricks control-plane API and psycopg mocked) and the admin gating of
``SettingsService.graph_engine_lakebase_provision_result``.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from back.core.task_manager import get_task_manager
from back.core.graphdb.lakebase.provisioner import (
    LakebaseGraphProvisioner,
    provision_steps,
)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, log):
        self._log = log

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, *args):
        self._log.append(sql)


class _FakeConn:
    def __init__(self, log):
        self._log = log

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def cursor(self):
        return _FakeCursor(self._log)

    def close(self):
        pass


class _FakeApi:
    """Stateful fake of ``WorkspaceClient.api_client``."""

    def __init__(self, *, apps_resolve=None, existing_instance=False,
                 project_short="ontobricks-graph", roles=None):
        self.instance_created = existing_instance
        self.databases = []
        self.calls = []
        self.permission_calls = []
        self.uc_calls = []
        # app_name -> sp id, or None to simulate a missing app
        self.apps_resolve = apps_resolve or {}
        # canonical project short-name surfaced by /postgres/projects
        self.project_short = project_short
        # Branch roles surfaced by /roles (default: an SP role for "app-sp").
        self.roles = roles if roles is not None else [
            {
                "name": "projects/" + project_short
                + "/branches/production/roles/app-sp",
                "status": {
                    "identity_type": "SERVICE_PRINCIPAL",
                    "postgres_role": "app-sp",
                },
            }
        ]

    def do(self, method, path, body=None):
        self.calls.append((method, path, body))

        if path.startswith("/api/2.0/database/instances/"):
            if not self.instance_created:
                return {}
            return {"state": "AVAILABLE"}

        if method == "POST" and path == "/api/2.0/database/instances":
            self.instance_created = True
            return {}

        if method == "GET" and path == "/api/2.0/postgres/projects":
            return {"projects": [{"name": "projects/" + self.project_short}]}

        if path.endswith("/branches"):
            return {
                "branches": [
                    {
                        "name": "projects/" + self.project_short
                        + "/branches/production"
                    }
                ]
            }

        if method == "GET" and path.endswith("/roles"):
            return {"roles": self.roles}

        if path.endswith("/endpoints"):
            return {
                "endpoints": [
                    {
                        "name": "projects/x/branches/production/endpoints/ep",
                        "status": {"hosts": {"host": "h.example.com"}},
                    }
                ]
            }

        if path.endswith("/databases"):
            if method == "GET":
                return {
                    "databases": [
                        {
                            "name": "projects/x/branches/production/databases/" + d,
                            "status": {"postgres_database": d},
                        }
                        for d in self.databases
                    ]
                }
            if method == "POST":
                spec = (body or {}).get("spec") or {}
                name = spec.get("postgres_database") or (body or {}).get("name")
                self.databases.append(name)
                return {}

        if path == "/api/2.0/postgres/credentials":
            return {"token": "fake-jwt"}

        if path.startswith("/api/2.0/apps/"):
            app_name = path.rsplit("/", 1)[-1]
            sp = self.apps_resolve.get(app_name, "sp-" + app_name)
            if sp is None:
                return {}
            return {"service_principal_client_id": sp}

        if path.startswith("/api/2.0/permissions/"):
            self.permission_calls.append((path, body))
            return {}

        if path.startswith("/api/2.1/unity-catalog/permissions/"):
            self.uc_calls.append((path, body))
            return {}

        return {}


def _make_provisioner(api, sql_log, *, grant_uc=False, **overrides):
    tm = get_task_manager()
    task = tm.create_task(
        name="test-provision",
        task_type="lakebase_provision",
        steps=provision_steps(grant_uc=grant_uc),
    )
    kwargs = dict(
        tm=tm,
        task_id=task.id,
        name="ontobricks-graph",
        capacity="CU_2",
        branch="production",
        database="ontobricks_graph_db",
        schema="ontobricks_graph",
        app_names=["ontobricks", "mcp-ontobricks"],
        pg_user="app-sp",
    )
    kwargs.update(overrides)
    prov = LakebaseGraphProvisioner(**kwargs)
    prov._api = lambda: api  # bypass WorkspaceClient construction

    fake_psycopg = MagicMock()
    fake_psycopg.connect.return_value = _FakeConn(sql_log)
    patcher = patch(
        "back.core.graphdb.lakebase.pool._require_psycopg",
        return_value=(fake_psycopg, None),
    )
    return prov, task, patcher


# ---------------------------------------------------------------------------
# Provisioner step sequence
# ---------------------------------------------------------------------------


class TestProvisioner:
    def test_full_provision_success(self):
        api = _FakeApi()
        sql_log = []
        prov, task, patcher = _make_provisioner(api, sql_log)
        on_success = MagicMock()
        prov._on_success = on_success

        with patcher:
            prov.run()

        assert task.status.value == "completed"
        assert task.result["database"] == "ontobricks_graph_db"
        assert task.result["schema"] == "ontobricks_graph"
        # Both apps got CAN_USE + schema grants.
        joined = " ".join(task.result["granted"])
        assert "ontobricks: CAN_USE on project" in joined
        assert "mcp-ontobricks: CAN_USE on project" in joined
        assert "USAGE + DML on schema ontobricks_graph" in joined
        assert task.result["warnings"] == []
        # Instance + database were created.
        assert "ontobricks_graph_db" in api.databases
        # The create-database call carried the owner role (PGUSER).
        create_db = next(
            c for c in api.calls
            if c[0] == "POST" and c[1].endswith("/databases")
        )
        assert create_db[2]["spec"]["postgres_database"] == "ontobricks_graph_db"
        assert create_db[2]["spec"]["role"].endswith("/roles/app-sp")
        # Schema was created before grants.
        assert any("CREATE SCHEMA IF NOT EXISTS" in s for s in sql_log)
        on_success.assert_called_once()

    def test_existing_instance_and_database_are_idempotent(self):
        api = _FakeApi(existing_instance=True)
        api.databases.append("ontobricks_graph_db")
        sql_log = []
        prov, task, patcher = _make_provisioner(api, sql_log)

        with patcher:
            prov.run()

        assert task.status.value == "completed"
        # No second create-instance POST.
        assert not any(
            c[0] == "POST" and c[1] == "/api/2.0/database/instances"
            for c in api.calls
        )
        # No create-database POST (already present).
        assert not any(
            c[0] == "POST" and c[1].endswith("/databases") for c in api.calls
        )

    def test_missing_mcp_app_warns_but_completes(self):
        api = _FakeApi(apps_resolve={"mcp-ontobricks": None})
        sql_log = []
        prov, task, patcher = _make_provisioner(api, sql_log)

        with patcher:
            prov.run()

        assert task.status.value == "completed"
        assert any("mcp-ontobricks" in w for w in task.result["warnings"])
        # The resolvable app still got its grants.
        assert any("ontobricks: CAN_USE" in g for g in task.result["granted"])

    def test_missing_pg_user_fails(self):
        api = _FakeApi()
        sql_log = []
        prov, task, patcher = _make_provisioner(api, sql_log, pg_user="")

        with patcher:
            prov.run()

        assert task.status.value == "failed"
        assert "PGUSER" in (task.error or "")

    def test_uppercase_name_is_normalised_and_resolved(self):
        # Regression: an uppercase instance name (e.g. "TEST-DB") is
        # registered lowercase by Lakebase. The provisioner must lowercase
        # the name, discover the canonical project path, and target grants
        # at the canonical short-name — not the typed value.
        api = _FakeApi(project_short="test-db")
        sql_log = []
        prov, task, patcher = _make_provisioner(api, sql_log, name="TEST-DB")

        with patcher:
            prov.run()

        assert task.status.value == "completed"
        assert task.result["instance"] == "test-db"
        assert any("normalised to lowercase" in w for w in task.result["warnings"])
        # CAN_USE grant targeted the canonical lowercase project name.
        assert any("database-projects/test-db" in c[0] for c in api.permission_calls)

    def test_names_are_normalised_to_safe_charset(self):
        # Names are lowercased and restricted to [a-z0-9_-]; other characters
        # collapse to "_" and the database create body carries the cleaned
        # value under spec.postgres_database.
        api = _FakeApi(project_short="my_graph")
        sql_log = []
        prov, task, patcher = _make_provisioner(
            api,
            sql_log,
            name="My Graph",
            database="Graph DB!",
            schema="Graph Schema",
        )

        with patcher:
            prov.run()

        assert task.status.value == "completed"
        assert task.result["instance"] == "my_graph"
        assert task.result["database"] == "graph_db"
        assert task.result["schema"] == "graph_schema"
        assert "graph_db" in api.databases
        assert any("normalised" in w for w in task.result["warnings"])

    def test_role_resolved_by_postgres_role_email(self):
        # PGUSER is the Postgres role name (an email); the owner path must be
        # the role *resource id* (benoit-cayla), matched via status.postgres_role.
        roles = [{
            "name": "projects/ontobricks-graph/branches/production/roles/benoit-cayla",
            "status": {
                "identity_type": "USER",
                "postgres_role": "benoit.cayla@databricks.com",
            },
        }]
        api = _FakeApi(roles=roles)
        sql_log = []
        prov, task, patcher = _make_provisioner(
            api, sql_log, pg_user="benoit.cayla@databricks.com"
        )

        with patcher:
            prov.run()

        assert task.status.value == "completed"
        create_db = next(
            c for c in api.calls
            if c[0] == "POST" and c[1].endswith("/databases")
        )
        assert create_db[2]["spec"]["role"].endswith("/roles/benoit-cayla")

    def test_role_falls_back_to_existing_user_owner(self):
        # PGUSER's role isn't on the branch yet; fall back to an existing USER
        # (owner) role and warn.
        roles = [{
            "name": "projects/ontobricks-graph/branches/production/roles/owner-user",
            "status": {
                "identity_type": "USER",
                "postgres_role": "owner@databricks.com",
            },
        }]
        api = _FakeApi(roles=roles)
        sql_log = []
        prov, task, patcher = _make_provisioner(
            api, sql_log, pg_user="not-present-sp-uuid"
        )

        with patcher:
            prov.run()

        assert task.status.value == "completed"
        create_db = next(
            c for c in api.calls
            if c[0] == "POST" and c[1].endswith("/databases")
        )
        assert create_db[2]["spec"]["role"].endswith("/roles/owner-user")
        assert any("database owner" in w for w in task.result["warnings"])

    def test_branch_falls_back_to_sole_default_branch(self):
        # Regression: a freshly created instance auto-creates a single default
        # branch ("production"); a typed branch like "production-1" won't match
        # but provisioning should proceed using the only branch and warn.
        api = _FakeApi(project_short="test-db-1")
        sql_log = []
        prov, task, patcher = _make_provisioner(
            api, sql_log, name="test-db-1", branch="production-1"
        )

        with patcher:
            prov.run()

        assert task.status.value == "completed"
        assert task.result["branch_path"].endswith("/branches/production")
        assert any("default branch" in w for w in task.result["warnings"])

    def test_uc_grant_applied_when_requested(self):
        api = _FakeApi()
        sql_log = []
        prov, task, patcher = _make_provisioner(
            api,
            sql_log,
            grant_uc=True,
            sync_mode="managed_synced",
            uc_catalog="my_catalog",
        )

        with patcher:
            prov.run()

        assert task.status.value == "completed"
        assert len(api.uc_calls) == 2  # one per app
        assert any(
            "ALL_PRIVILEGES on catalog my_catalog" in g
            for g in task.result["granted"]
        )


# ---------------------------------------------------------------------------
# Service-layer admin gating
# ---------------------------------------------------------------------------


class TestProvisionService:
    def _params(self):
        return {
            "name": "ontobricks-graph",
            "capacity": "CU_2",
            "branch": "production",
            "database": "ontobricks_graph_db",
            "schema": "ontobricks_graph",
            "mcp_app_name": "mcp-ontobricks",
            "grant_uc_catalog": False,
        }

    def test_non_admin_rejected(self, monkeypatch):
        import importlib

        from back.core.errors import AuthorizationError

        # ``back.objects.domain.SettingsService`` resolves to the *class*
        # (re-exported in the package __init__), so patch the module object.
        ss = importlib.import_module("back.objects.domain.SettingsService")
        SettingsService = ss.SettingsService

        monkeypatch.setenv("PGUSER", "app-sp")
        with patch.object(ss, "is_databricks_app", return_value=True), \
             patch.object(
                 SettingsService,
                 "_resolve_context",
                 return_value=(None, "h", "t", {}),
             ), \
             patch.object(ss.permission_service, "is_admin", return_value=False):
            with pytest.raises(AuthorizationError):
                SettingsService.graph_engine_lakebase_provision_result(
                    self._params(), "user@x.com", "tok", MagicMock(), MagicMock()
                )

    def test_admin_starts_task_and_returns_id(self, monkeypatch):
        import importlib

        ss = importlib.import_module("back.objects.domain.SettingsService")
        SettingsService = ss.SettingsService

        monkeypatch.setenv("PGUSER", "app-sp")
        settings = MagicMock()
        settings.ontobricks_app_name = "ontobricks"

        gcs = MagicMock()
        gcs.get_graph_engine_config.return_value = {}

        # Replace the provisioner with a no-op so the worker thread does
        # not touch the network.
        with patch.object(ss, "is_databricks_app", return_value=False), \
             patch.object(
                 SettingsService,
                 "_resolve_context",
                 return_value=(None, "h", "t", {}),
             ), \
             patch.object(ss, "global_config_service", gcs), \
             patch(
                 "back.core.graphdb.lakebase.provisioner.LakebaseGraphProvisioner"
             ) as prov_cls:
            prov_cls.return_value.run.return_value = None
            out = SettingsService.graph_engine_lakebase_provision_result(
                self._params(), "user@x.com", "tok", MagicMock(), settings
            )

        assert out["success"] is True
        assert out["task_id"]
