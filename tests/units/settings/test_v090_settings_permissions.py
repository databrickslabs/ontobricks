"""PermissionSettings (0.9.0): permissions/me, admin diagnostics, principal
search, domain permission CRUD and the Teams matrix."""

from __future__ import annotations

import importlib
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from back.core.errors import InfrastructureError, ValidationError
from back.objects.domain.settings.PermissionSettings import PermissionSettings
from back.objects.domain.settings.SettingsService import SettingsService

pytestmark = pytest.mark.unit

_ss = importlib.import_module("back.objects.domain.settings.SettingsService")

CFG = {"catalog": "cat", "schema": "sch"}
SETTINGS = SimpleNamespace(ontobricks_app_name="ontobricks-app")


@pytest.fixture
def perm(monkeypatch):
    service = MagicMock()
    monkeypatch.setattr(_ss, "permission_service", service)
    return service


@pytest.fixture
def ctx(monkeypatch):
    holder = {"domain": SimpleNamespace(domain_folder="claims"), "cfg": dict(CFG)}
    monkeypatch.setattr(
        SettingsService,
        "_resolve_context",
        staticmethod(lambda sm, st: (holder["domain"], "h", "t", holder["cfg"])),
    )
    return holder


# ---------------------------------------------------------------------------
# permissions/me
# ---------------------------------------------------------------------------


class TestBuildPermissionsMe:
    def _call(self, email="a@x", name="Alice", app_role="viewer", domain_role="viewer"):
        return PermissionSettings.build_permissions_me(
            email, name, "utok", app_role, domain_role, MagicMock(), SETTINGS
        )

    def test_local_mode_is_admin_with_defaults(self, monkeypatch, perm):
        monkeypatch.setattr(_ss, "is_databricks_app", lambda: False)
        assert self._call(email="", name="") == {
            "email": "local-user",
            "display_name": "Local User",
            "role": "admin",
            "is_app_mode": False,
        }
        perm.is_admin.assert_not_called()

    def test_app_mode_resolves_fresh_roles(self, monkeypatch, perm, ctx):
        monkeypatch.setattr(_ss, "is_databricks_app", lambda: True)
        perm.is_admin.return_value = False
        perm.get_user_role.return_value = "editor"
        perm.get_domain_role.return_value = "builder"

        result = self._call()

        assert result == {
            "email": "a@x",
            "display_name": "Alice",
            "role": "editor",
            "is_app_admin": False,
            "is_app_mode": True,
            "domain_folder": "claims",
            "domain_role": "builder",
        }
        perm.clear_admin_cache.assert_called_once_with("a@x")
        perm.get_domain_role.assert_called_once_with(
            "a@x", "h", "t", CFG, "ontobricks-app", "claims", user_token="utok", app_role="editor"
        )

    def test_resolution_error_keeps_middleware_domain_role(self, monkeypatch, perm, ctx):
        monkeypatch.setattr(_ss, "is_databricks_app", lambda: True)
        perm.is_admin.side_effect = RuntimeError("scim down")

        result = self._call(domain_role="editor")

        assert result["role"] == "none"
        assert result["is_app_admin"] is False
        assert result["domain_role"] == "editor"
        assert result["domain_folder"] == "claims"


# ---------------------------------------------------------------------------
# permissions/diag
# ---------------------------------------------------------------------------


class TestBuildPermissionsDiag:
    @pytest.fixture
    def sdk(self, monkeypatch):
        import databricks.sdk as dbsdk

        client = MagicMock()
        client.config.host = "https://ws.example/"
        client.config.auth_type = "oauth-m2m"
        monkeypatch.setattr(dbsdk, "WorkspaceClient", lambda: client)
        return client

    def _call(self, user_token=""):
        return PermissionSettings.build_permissions_diag(
            "Alice@X", "Alice", user_token, "viewer", "viewer", SETTINGS
        )

    def test_sdk_path_reports_managers_groups_and_cache(self, monkeypatch, perm, sdk):
        monkeypatch.setattr(_ss, "is_databricks_app", lambda: True)
        perm._extract_can_manage.return_value = ["alice@x", "admins"]
        perm._get_user_groups.return_value = ["Admins", "data-eng"]
        perm._admin_cache = {"alice@x": (True, time.time() - 5)}

        diag = self._call()

        assert diag["is_app_mode"] is True
        assert diag["user_token_present"] is False
        assert diag["sdk_host"] == "https://ws.example/"
        assert diag["sdk_auth_type"] == "oauth-m2m"
        assert diag["sdk_error"] is None
        assert diag["email_is_manager"] is True
        assert diag["group_is_manager"] == ["Admins"]
        assert diag["admin_cache"]["alice@x"]["result"] is True
        assert diag["admin_cache"]["alice@x"]["age_s"] >= 4
        assert "user_token_can_manage" not in diag
        sdk.api_client.do.assert_called_once_with(
            "GET", f"{_ss.PERMISSIONS_APPS_PATH}/ontobricks-app"
        )

    def test_sdk_failure_is_reported_not_raised(self, monkeypatch, perm):
        import databricks.sdk as dbsdk

        def _boom():
            raise RuntimeError("no creds")

        monkeypatch.setattr(dbsdk, "WorkspaceClient", _boom)
        perm._get_user_groups.return_value = []
        perm._admin_cache = {}

        diag = self._call()

        assert diag["sdk_error"] == "RuntimeError: no creds"
        assert diag["sdk_can_manage"] == []
        assert diag["email_is_manager"] is False

    def test_user_token_path_overrides_sdk_managers(self, monkeypatch, perm, sdk):
        import requests

        perm._extract_can_manage.side_effect = [["someone@x"], ["alice@x"]]
        perm._get_user_groups.return_value = []
        perm._admin_cache = {}
        resp = MagicMock()
        resp.json.return_value = {"acl": []}
        get = MagicMock(return_value=resp)
        monkeypatch.setattr(requests, "get", get)

        diag = self._call(user_token="utok")

        assert diag["user_token_can_manage"] == ["alice@x"]
        assert diag["user_token_error"] is None
        assert diag["email_is_manager"] is True
        url = get.call_args.args[0]
        assert url == f"https://ws.example{_ss.PERMISSIONS_APPS_PATH}/ontobricks-app"
        assert get.call_args.kwargs["headers"]["Authorization"] == "Bearer utok"

    def test_user_token_failure_falls_back_to_sdk_managers(self, monkeypatch, perm, sdk):
        import requests

        perm._extract_can_manage.return_value = ["alice@x"]
        perm._get_user_groups.return_value = []
        perm._admin_cache = {}
        monkeypatch.setattr(requests, "get", MagicMock(side_effect=ConnectionError("refused")))

        diag = self._call(user_token="utok")

        assert diag["user_token_error"] == "ConnectionError: refused"
        assert diag["user_token_can_manage"] == []
        assert diag["email_is_manager"] is True


# ---------------------------------------------------------------------------
# Principals
# ---------------------------------------------------------------------------


class TestPrincipals:
    def test_list_app_principals_clears_cache_first(self, perm, ctx):
        perm.list_app_principals.return_value = {"users": [{"email": "a@x"}]}
        result = PermissionSettings.list_app_principals_result(MagicMock(), SETTINGS)
        assert result == {"success": True, "users": [{"email": "a@x"}], "groups": []}
        perm.clear_principals_cache.assert_called_once()
        perm.list_app_principals.assert_called_once_with("h", "t", "ontobricks-app")

    def test_list_principals_is_alias(self, monkeypatch):
        spy = MagicMock(return_value={"success": True})
        monkeypatch.setattr(SettingsService, "list_app_principals_result", spy)
        sm = MagicMock()
        assert PermissionSettings.list_principals_result(sm, SETTINGS) == {"success": True}
        spy.assert_called_once_with(sm, SETTINGS)

    def test_search_groups_by_display_name(self, perm, ctx):
        perm.list_app_principals.return_value = {
            "groups": [{"display_name": "Data-Eng"}, {"display_name": "ops"}, {"display_name": None}],
            "users": [{"email": "data@x"}],
        }
        result = PermissionSettings.search_workspace_principals("dAtA", "group", MagicMock(), SETTINGS)
        assert result == {"success": True, "results": [{"display_name": "Data-Eng"}]}

    def test_search_users_by_email_or_name(self, perm, ctx):
        perm.list_app_principals.return_value = {
            "users": [
                {"email": "bob@x", "display_name": "Robert"},
                {"email": "eve@x", "display_name": "Bobby"},
                {"email": "zed@x", "display_name": None},
            ]
        }
        result = PermissionSettings.search_workspace_principals("BOB", "user", MagicMock(), SETTINGS)
        assert [u["email"] for u in result["results"]] == ["bob@x", "eve@x"]


# ---------------------------------------------------------------------------
# Domain permission CRUD
# ---------------------------------------------------------------------------


class TestDomainPermissions:
    def test_list_entries(self, perm, ctx):
        perm.list_domain_entries.return_value = [{"principal": "a@x", "role": "viewer"}]
        assert PermissionSettings.list_domain_permissions_result("claims", MagicMock(), SETTINGS) == {
            "success": True,
            "domain": "claims",
            "permissions": [{"principal": "a@x", "role": "viewer"}],
        }

    @pytest.mark.parametrize(
        "domain,data,match",
        [
            ("claims", {"principal": "  "}, "Principal"),
            ("claims", {"principal": "a@x", "role": "admin"}, "Role must be"),
            ("", {"principal": "a@x"}, "Domain name is required"),
        ],
    )
    def test_add_validation(self, perm, ctx, domain, data, match):
        with pytest.raises(ValidationError, match=match):
            PermissionSettings.add_domain_permission_result(domain, data, MagicMock(), SETTINGS)
        perm.add_or_update_domain_entry.assert_not_called()

    def test_add_requires_registry(self, perm, ctx):
        ctx["cfg"] = {"catalog": "cat"}
        with pytest.raises(ValidationError, match="Registry not configured"):
            PermissionSettings.add_domain_permission_result("claims", {"principal": "a@x"}, MagicMock(), SETTINGS)

    def test_add_defaults_and_persists(self, perm, ctx):
        perm.add_or_update_domain_entry.return_value = (True, "saved")
        result = PermissionSettings.add_domain_permission_result(
            "claims", {"principal": " a@x "}, MagicMock(), SETTINGS
        )
        assert result == {"success": True, "message": "saved"}
        perm.add_or_update_domain_entry.assert_called_once_with(
            "h", "t", CFG, "claims", "a@x", "user", "a@x", "viewer"
        )

    def test_add_failure_raises_infrastructure(self, perm, ctx):
        perm.add_or_update_domain_entry.return_value = (False, "dup")
        with pytest.raises(InfrastructureError):
            PermissionSettings.add_domain_permission_result(
                "claims", {"principal": "g", "principal_type": "group", "role": "builder"}, MagicMock(), SETTINGS
            )

    def test_delete_success_and_failure(self, perm, ctx):
        perm.remove_domain_entry.return_value = (True, "removed")
        assert PermissionSettings.delete_domain_permission_result("claims", "a@x", MagicMock(), SETTINGS) == {
            "success": True,
            "message": "removed",
        }
        perm.remove_domain_entry.assert_called_once_with("h", "t", CFG, "claims", "a@x")

        perm.remove_domain_entry.return_value = (False, "missing")
        with pytest.raises(InfrastructureError):
            PermissionSettings.delete_domain_permission_result("claims", "a@x", MagicMock(), SETTINGS)

    def test_delete_requires_registry(self, perm, ctx):
        ctx["cfg"] = {}
        with pytest.raises(ValidationError):
            PermissionSettings.delete_domain_permission_result("claims", "a@x", MagicMock(), SETTINGS)


# ---------------------------------------------------------------------------
# Teams matrix
# ---------------------------------------------------------------------------


class TestTeamsMatrix:
    @pytest.fixture
    def registry(self, monkeypatch):
        svc = MagicMock()
        rs_cls = MagicMock()
        rs_cls.from_context.return_value = svc
        monkeypatch.setattr(_ss, "RegistryService", rs_cls)
        return svc

    def test_builds_sorted_domains_principals_and_assignments(self, perm, ctx, registry):
        registry.list_domains_cached.return_value = (True, ["beta", "acme", "gamma"], "")
        perm.list_app_principals.return_value = {
            "users": [{"email": "alice@acme", "display_name": "Alice"}, {"email": ""}, {"email": "bob@acme"}],
            "groups": [{"display_name": "data-eng"}, {"id": "grp-1"}, {}],
        }

        def entries(h, t, cfg, name):
            if name == "gamma":
                raise RuntimeError("table locked")
            return {
                "acme": [{"principal": "alice@acme", "role": "editor"}, {"principal": "x", "role": ""}],
                "beta": [],
            }[name]

        perm.list_domain_entries.side_effect = entries

        result = PermissionSettings.build_teams_matrix_result(MagicMock(), SETTINGS)

        assert result["success"] is True
        assert result["domains"] == ["acme", "beta", "gamma"]
        assert result["principals"] == [
            {"principal": "alice@acme", "principal_type": "user", "display_name": "Alice"},
            {"principal": "bob@acme", "principal_type": "user", "display_name": "bob@acme"},
            {"principal": "data-eng", "principal_type": "group", "display_name": "data-eng"},
            {"principal": "grp-1", "principal_type": "group", "display_name": "grp-1"},
        ]
        assert result["assignments"] == {"acme": {"alice@acme": "editor"}}
        perm.clear_principals_cache.assert_called_once()

    def test_domain_listing_failure_yields_empty_matrix(self, perm, ctx, registry):
        registry.list_domains_cached.side_effect = RuntimeError("down")
        perm.list_app_principals.return_value = {}
        result = PermissionSettings.build_teams_matrix_result(MagicMock(), SETTINGS)
        assert result == {"success": True, "domains": [], "principals": [], "assignments": {}}

    def test_unsuccessful_domain_listing_yields_no_domains(self, perm, ctx, registry):
        registry.list_domains_cached.return_value = (False, ["x"], "err")
        perm.list_app_principals.return_value = {}
        assert PermissionSettings.build_teams_matrix_result(MagicMock(), SETTINGS)["domains"] == []


class TestSaveTeamsBatch:
    @pytest.mark.parametrize(
        "changes,match",
        [
            ("nope", "'changes' array"),
            (["str"], "Change #0 is not an object"),
            ([{"principal": "a"}], "'domain_folder' is required"),
            ([{"domain_folder": "d"}], "'principal' is required"),
            ([{"domain_folder": "d", "principal": "a", "principal_type": "sp"}], "'principal_type'"),
            ([{"domain_folder": "d", "principal": "a", "role": "owner"}], "'role' must be one of"),
        ],
    )
    def test_validation(self, perm, ctx, changes, match):
        with pytest.raises(ValidationError, match=match):
            PermissionSettings.save_teams_batch_result({"changes": changes}, MagicMock(), SETTINGS)
        perm.save_domain_permissions_batch.assert_not_called()

    def test_requires_registry(self, perm, ctx):
        ctx["cfg"] = {"schema": "s"}
        with pytest.raises(ValidationError, match="Registry not configured"):
            PermissionSettings.save_teams_batch_result({"changes": []}, MagicMock(), SETTINGS)

    def test_persists_normalized_changes_and_reports_failures(self, perm, ctx):
        perm.save_domain_permissions_batch.return_value = (1, [{"principal": "bob"}])
        data = {
            "changes": [
                {"domain_folder": " acme ", "principal": " alice ", "role": "editor"},
                {"domain_folder": "beta", "principal": "bob", "principal_type": "group",
                 "display_name": "Bob", "role": None},
            ]
        }

        result = PermissionSettings.save_teams_batch_result(data, MagicMock(), SETTINGS)

        assert result == {"success": False, "saved": 1, "failed": [{"principal": "bob"}], "total_changes": 2}
        validated = perm.save_domain_permissions_batch.call_args.args[3]
        assert validated == [
            {"domain_folder": "acme", "principal": "alice", "principal_type": "user",
             "display_name": "alice", "role": "editor"},
            {"domain_folder": "beta", "principal": "bob", "principal_type": "group",
             "display_name": "Bob", "role": None},
        ]

    def test_all_saved_is_success(self, perm, ctx):
        perm.save_domain_permissions_batch.return_value = (0, [])
        assert PermissionSettings.save_teams_batch_result({}, MagicMock(), SETTINGS)["success"] is True
