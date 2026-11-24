"""First-run registry setup, run by an admin from Settings → Registry.

A Marketplace install has no deploy step, so the first admin grants the app's
service principal Can Manage on the app (``apps`` user scope), creates the
registry schema and grants the SP its UC privileges (``sql`` user scope), all
as themselves. Initialize then stores the location in Lakebase.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service.sql import StatementState

from back.core.errors import (
    AuthorizationError,
    ConflictError,
    InfrastructureError,
    ValidationError,
)
from back.objects.registry import RegistryCfg, RegistrySetupService

pytestmark = pytest.mark.unit

_SP = "11111111-2222-3333-4444-555555555555"
_APP = "ontobricks-test"
_WAREHOUSE = "wh-1"


def _response(state, message=""):
    error = SimpleNamespace(message=message, error_code=None) if message else None
    return SimpleNamespace(statement_id="st-1", status=SimpleNamespace(state=state, error=error))


def _client(state=StatementState.SUCCEEDED, message=""):
    w = MagicMock()
    w.statement_execution.execute_statement.return_value = _response(state, message)
    return w


def _service(w):
    return RegistrySetupService(w, warehouse_id=_WAREHOUSE, app_name=_APP, sp_client_id=_SP)


def _statements(w):
    return [c.kwargs["statement"] for c in w.statement_execution.execute_statement.call_args_list]


class TestPrepare:
    def test_creates_the_schema_and_grants_the_service_principal(self):
        w = _client()
        _service(w).prepare("main", "ontobricks")
        assert _statements(w) == [
            "CREATE SCHEMA IF NOT EXISTS `main`.`ontobricks`",
            f"GRANT USE CATALOG ON CATALOG `main` TO `{_SP}`",
            "GRANT USE SCHEMA, CREATE TABLE, CREATE VOLUME, SELECT, MODIFY, "
            f"READ VOLUME, WRITE VOLUME ON SCHEMA `main`.`ontobricks` TO `{_SP}`",
        ]

    def test_sql_runs_on_the_bound_warehouse(self):
        w = _client()
        _service(w).prepare("main", "ontobricks")
        assert w.statement_execution.execute_statement.call_args.kwargs["warehouse_id"] == _WAREHOUSE

    def test_grants_the_service_principal_can_manage_on_the_app(self):
        w = _client()
        _service(w).prepare("main", "ontobricks")
        acl = w.apps.update_permissions.call_args.kwargs["access_control_list"]
        assert [(a.service_principal_name, a.permission_level.value) for a in acl] == [(_SP, "CAN_MANAGE")]

    def test_app_grant_failure_is_a_warning_not_an_error(self):
        w = _client()
        w.apps.update_permissions.side_effect = PermissionDenied("no apps scope")
        result = _service(w).prepare("main", "ontobricks")
        assert len(_statements(w)) == 3 and "Can Manage" in result["warnings"][0]

    def test_invalid_name_runs_nothing(self):
        w = _client()
        with pytest.raises(ValidationError):
            _service(w).prepare("main", "bad`name")
        assert not w.statement_execution.execute_statement.called

    def test_insufficient_privileges_is_an_authorization_error(self):
        w = _client(StatementState.FAILED, "[INSUFFICIENT_PERMISSIONS] User does not have CREATE SCHEMA")
        with pytest.raises(AuthorizationError, match="CREATE SCHEMA"):
            _service(w).prepare("main", "ontobricks")

    def test_unknown_catalog_is_a_validation_error(self):
        w = _client(StatementState.FAILED, "[NO_SUCH_CATALOG_EXCEPTION] Catalog 'mian' was not found.")
        with pytest.raises(ValidationError, match="mian"):
            _service(w).prepare("mian", "ontobricks")

    def test_timed_out_statement_is_cancelled(self, monkeypatch):
        monkeypatch.setattr(RegistrySetupService, "_POLL_INTERVAL_S", 0)
        monkeypatch.setattr(RegistrySetupService, "_POLL_LIMIT_S", -1)
        w = _client(StatementState.RUNNING)
        with pytest.raises(InfrastructureError):
            _service(w).prepare("main", "ontobricks")
        w.statement_execution.cancel_execution.assert_called_once_with("st-1")

    def test_other_sql_failure_is_an_infrastructure_error(self):
        w = _client(StatementState.FAILED, "warehouse is unavailable")
        with pytest.raises(InfrastructureError):
            _service(w).prepare("main", "ontobricks")

    def test_running_statement_is_polled_until_done(self, monkeypatch):
        monkeypatch.setattr(RegistrySetupService, "_POLL_INTERVAL_S", 0)
        w = _client(StatementState.RUNNING)
        w.statement_execution.get_statement.side_effect = [
            _response(StatementState.RUNNING),
            _response(StatementState.SUCCEEDED),
        ] * 3
        _service(w).prepare("main", "ontobricks")
        assert w.statement_execution.get_statement.call_count == 6


class TestForUser:
    def test_requires_the_app_service_principal_id(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        with pytest.raises(InfrastructureError, match="service principal"):
            RegistrySetupService.for_user("https://h.example", "user-token", _WAREHOUSE, _APP)

    def test_authenticates_with_the_user_token_only(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", _SP)
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "sp-secret-placeholder")
        svc = RegistrySetupService.for_user("https://h.example", "user-token", _WAREHOUSE, _APP)
        assert svc._w.config.authenticate()["Authorization"] == "Bearer user-token"


# --- RegistrySettings.setup_registry_result ---------------------------------

_rs = importlib.import_module("back.objects.domain.settings.RegistrySettings")


def _settings():
    return SimpleNamespace(ontobricks_app_name=_APP, databricks_host="https://h.example")


def _run_setup(**kw):
    return _run_setup_with_domain(SimpleNamespace(settings={"registry": {}}), **kw)


def _run_setup_with_domain(domain, *, configured=False, user_token="user-token", configured_after_prepare=False, init_error=None):
    setup = MagicMock()
    setup.prepare.return_value = {"granted": ["g"], "warnings": []}
    unset = RegistryCfg("", "", "v")
    taken = RegistryCfg(catalog="c", schema="s", volume="v")
    cfgs = [taken] if configured else [unset, taken if configured_after_prepare else unset]
    with patch.object(_rs, "get_domain", return_value=domain), patch.object(
        _rs, "resolve_warehouse_id", return_value=_WAREHOUSE
    ), patch.object(_rs.RegistryCfg, "from_session", side_effect=cfgs), patch.object(
        _rs.RegistrySetupService, "for_user", return_value=setup
    ) as for_user, patch.object(
        _rs.RegistrySettings,
        "initialize_registry_result",
        return_value={"success": True},
        side_effect=init_error,
    ) as init:
        result = _rs.RegistrySettings.setup_registry_result(
            MagicMock(), _settings(), "main", "ontobricks", user_token
        )
    return SimpleNamespace(result=result, domain=domain, setup=setup, for_user=for_user, init=init)


class TestSetupRegistryResult:
    def test_requires_the_callers_token(self):
        with pytest.raises(AuthorizationError):
            _run_setup(user_token="")

    def test_refuses_to_move_a_configured_registry(self):
        with pytest.raises(ConflictError):
            _run_setup(configured=True)

    def test_prepares_then_initializes_with_the_new_location(self):
        run = _run_setup()
        run.setup.prepare.assert_called_once_with("main", "ontobricks")
        assert run.domain.settings["registry"] == {"catalog": "main", "schema": "ontobricks"}
        assert run.init.called and run.result["setup"] == {"granted": ["g"], "warnings": []}

    def test_another_admin_finishing_first_wins(self):
        with pytest.raises(ConflictError):
            _run_setup(configured_after_prepare=True)

    def test_failed_initialize_unpins_the_location_for_a_retry(self):
        domain = SimpleNamespace(settings={"registry": {}})
        with patch.object(_rs, "get_domain", return_value=domain), pytest.raises(InfrastructureError):
            _run_setup_with_domain(domain, init_error=InfrastructureError("boom"))
        assert domain.settings["registry"] == {}


# --- Initialize keeps the saved location -------------------------------------


class TestInitializeKeepsSavedLocation:
    def test_falls_back_to_the_lakebase_row_when_nothing_is_pinned(self):
        unpinned = MagicMock()
        unpinned.cfg = RegistryCfg("", "", "v")
        saved = MagicMock()
        saved.cfg = RegistryCfg("c", "s", "v")
        saved.initialize.return_value = (True, "ok")
        with patch.object(_rs, "get_domain"), patch.object(
            _rs.RegistryService, "from_context", side_effect=[unpinned, saved]
        ), patch.object(_rs, "get_databricks_client", return_value=MagicMock()), patch.object(
            _rs.SettingsService, "_resolve_context", side_effect=RuntimeError("skip seed")
        ), patch.object(_rs.SettingsService, "_grant_registry_permissions", return_value=None):
            result = _rs.RegistrySettings.initialize_registry_result(MagicMock(), _settings())
        assert result["success"] and saved.initialize.called


# --- POST /settings/registry/setup ------------------------------------------


def _post_setup(role: str):
    from fastapi.testclient import TestClient

    from shared.fastapi.main import PermissionMiddleware, app

    with patch("back.core.databricks.is_databricks_app", return_value=True), patch.object(
        PermissionMiddleware, "_resolve_roles", return_value=(role, role)
    ), patch.object(
        _rs.RegistrySettings, "setup_registry_result", return_value={"success": True}
    ) as setup:
        resp = TestClient(app).post(
            "/settings/registry/setup",
            json={"catalog": " main ", "schema": "ontobricks"},
            headers={"x-forwarded-access-token": "user-token", "x-forwarded-email": "a@b.com"},
            follow_redirects=False,
        )
    return resp, setup


class TestSetupRoute:
    def test_admin_call_runs_setup_with_the_forwarded_token(self):
        resp, setup = _post_setup("admin")
        assert resp.status_code == 200, resp.text
        assert setup.call_args.args[2:] == ("main", "ontobricks", "user-token")

    def test_non_admin_is_refused(self):
        resp, setup = _post_setup("app_user")
        assert resp.status_code in (302, 403) and not setup.called

    def test_malformed_body_is_a_bad_request(self):
        from fastapi.testclient import TestClient

        from shared.fastapi.main import PermissionMiddleware, app

        with patch("back.core.databricks.is_databricks_app", return_value=True), patch.object(
            PermissionMiddleware, "_resolve_roles", return_value=("admin", "admin")
        ):
            resp = TestClient(app).post(
                "/settings/registry/setup",
                content=b"not json",
                headers={"Content-Type": "application/json", "x-forwarded-access-token": "t"},
                follow_redirects=False,
            )
        assert resp.status_code == 400
