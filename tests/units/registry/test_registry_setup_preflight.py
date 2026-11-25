"""Setup preflight: what the admin's own token and privileges allow, before setup runs.

Every check runs as the signed-in admin and changes nothing. Each failing check
names the identity involved and the fix, so an installer can debug permission
problems from the UI.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service.sql import StatementState

from back.objects.registry import RegistrySetupService

pytestmark = pytest.mark.unit

_SP = "11111111-2222-3333-4444-555555555555"
_ME = "admin@example.com"


def _resp(state=StatementState.SUCCEEDED, message="", rows=None):
    error = SimpleNamespace(message=message, error_code=None) if message else None
    result = SimpleNamespace(data_array=rows or [])
    return SimpleNamespace(statement_id="st", status=SimpleNamespace(state=state, error=error), result=result)


def _acl(*sp_levels):
    entries = [
        SimpleNamespace(
            service_principal_name=sp,
            user_name=None,
            group_name=None,
            all_permissions=[SimpleNamespace(permission_level=SimpleNamespace(value=level))],
        )
        for sp, level in sp_levels
    ]
    return SimpleNamespace(access_control_list=entries)


def _client(*, acl=None, sql=None):
    """``sql`` maps a statement prefix to a response; anything else succeeds."""
    w = MagicMock()
    if isinstance(acl, Exception):
        w.apps.get_permissions.side_effect = acl
    else:
        w.apps.get_permissions.return_value = acl if acl is not None else _acl()
    routes = sql or {}

    def execute(statement, **kwargs):
        for prefix, resp in routes.items():
            if statement.startswith(prefix):
                return resp
        return _resp()

    w.statement_execution.execute_statement.side_effect = execute
    return w


def _checks(w, catalog="main", schema="ontobricks"):
    svc = RegistrySetupService(w, warehouse_id="wh", app_name="app", sp_client_id=_SP, user_email=_ME)
    return {c["name"]: c for c in svc.preflight(catalog, schema)}


def test_all_clear_on_a_fresh_install():
    w = _client(sql={"DESCRIBE SCHEMA": _resp(StatementState.FAILED, "[SCHEMA_NOT_FOUND] not found")})
    checks = _checks(w)
    assert {c["status"] for n, c in checks.items() if n != "app.can_manage"} == {"ok"}


def test_preflight_changes_nothing():
    w = _client()
    _checks(w)
    assert not w.apps.update_permissions.called
    statements = [c.kwargs["statement"] for c in w.statement_execution.execute_statement.call_args_list]
    assert not any(s.startswith(("CREATE", "GRANT")) for s in statements)


def test_missing_apps_scope_is_an_error_with_a_manual_fix():
    checks = _checks(_client(acl=PermissionDenied("insufficient scopes")))
    assert checks["you.app_permissions"]["status"] == "error"
    assert "Apps" in checks["you.app_permissions"]["fix"]


def test_service_principal_without_can_manage_is_a_warning_setup_fixes():
    check = _checks(_client(acl=_acl(("someone-else", "CAN_MANAGE"))))["app.can_manage"]
    assert check["status"] == "warning" and "setup" in check["fix"].lower()


def test_service_principal_with_can_manage_is_ok():
    assert _checks(_client(acl=_acl((_SP, "CAN_MANAGE"))))["app.can_manage"]["status"] == "ok"


def test_warehouse_failure_skips_the_sql_checks():
    checks = _checks(_client(sql={"SELECT 1": _resp(StatementState.FAILED, "PERMISSION_DENIED: no CAN_USE")}))
    assert checks["you.warehouse"]["status"] == "error"
    assert checks["you.catalog"]["status"] == "skipped"


def test_catalog_without_access_names_the_grant_to_ask_for():
    w = _client(sql={"DESCRIBE CATALOG": _resp(StatementState.FAILED, "[INSUFFICIENT_PERMISSIONS] no USE CATALOG")})
    check = _checks(w)["you.catalog"]
    assert check["status"] == "error"
    assert f"GRANT USE CATALOG, CREATE SCHEMA ON CATALOG `main` TO `{_ME}`" in check["fix"]


def test_existing_schema_owned_by_someone_else_is_a_warning():
    rows = [["Namespace Name", "ontobricks"], ["Owner", "someone@example.com"]]
    check = _checks(_client(sql={"DESCRIBE SCHEMA": _resp(rows=rows)}))["you.schema"]
    assert check["status"] == "warning" and "someone@example.com" in check["detail"]


def test_existing_schema_owned_by_you_is_ok():
    rows = [["Owner", _ME]]
    assert _checks(_client(sql={"DESCRIBE SCHEMA": _resp(rows=rows)}))["you.schema"]["status"] == "ok"


def test_invalid_name_is_reported_not_raised():
    check = _checks(_client(), catalog="bad`name")["you.catalog"]
    assert check["status"] == "error" and "name" in check["detail"].lower()


def test_schema_check_is_skipped_when_the_catalog_is_unusable():
    w = _client(sql={"DESCRIBE CATALOG": _resp(StatementState.FAILED, "[NO_SUCH_CATALOG_EXCEPTION] not found")})
    assert _checks(w)["you.schema"]["status"] == "skipped"
