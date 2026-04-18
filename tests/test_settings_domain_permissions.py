"""Tests for domain-permission orchestration in SettingsService.

Covers: list, add/update, delete domain permissions, role validation,
and app-level builder role acceptance.
"""

import importlib

import pytest
from unittest.mock import patch, MagicMock

from back.core.errors import ValidationError, InfrastructureError
from back.objects.domain.SettingsService import SettingsService

_svc_module = importlib.import_module("back.objects.domain.SettingsService")


REGISTRY_CFG = {"catalog": "cat", "schema": "sch", "volume": "vol"}
EMPTY_REGISTRY = {"catalog": "", "schema": ""}


def _mock_context():
    """Return (session_mgr, settings) mocks."""
    return MagicMock(), MagicMock()


class TestListDomainPermissions:
    def test_returns_entries(self):
        session_mgr, settings = _mock_context()
        entries = [{"principal": "a@b.com", "role": "viewer"}]

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "permission_service") as ps,
        ):
            ps.list_domain_entries.return_value = entries
            result = SettingsService.list_domain_permissions_result(
                "my_domain", session_mgr, settings
            )

        assert result["success"]
        assert result["domain"] == "my_domain"
        assert len(result["permissions"]) == 1
        assert result["permissions"][0]["principal"] == "a@b.com"


class TestAddDomainPermission:
    def test_add_success(self):
        session_mgr, settings = _mock_context()
        data = {
            "principal": "a@b.com",
            "principal_type": "user",
            "display_name": "Alice",
            "role": "builder",
        }

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "permission_service") as ps,
        ):
            ps.add_or_update_domain_entry.return_value = (True, "ok")
            result = SettingsService.add_domain_permission_result(
                "my_domain", data, session_mgr, settings
            )

        assert result["success"]

    def test_missing_principal_raises(self):
        session_mgr, settings = _mock_context()
        data = {"principal": "", "role": "viewer"}

        with pytest.raises(ValidationError, match="Principal"):
            SettingsService.add_domain_permission_result(
                "my_domain", data, session_mgr, settings
            )

    def test_invalid_role_raises(self):
        session_mgr, settings = _mock_context()
        data = {"principal": "a@b.com", "role": "superadmin"}

        with pytest.raises(ValidationError, match="Role must be"):
            SettingsService.add_domain_permission_result(
                "my_domain", data, session_mgr, settings
            )

    def test_empty_domain_raises(self):
        session_mgr, settings = _mock_context()
        data = {"principal": "a@b.com", "role": "viewer"}

        with pytest.raises(ValidationError, match="Domain name"):
            SettingsService.add_domain_permission_result(
                "", data, session_mgr, settings
            )

    @pytest.mark.parametrize("role", ["viewer", "editor", "builder"])
    def test_accepted_roles(self, role):
        session_mgr, settings = _mock_context()
        data = {
            "principal": "a@b.com",
            "principal_type": "user",
            "display_name": "A",
            "role": role,
        }

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "permission_service") as ps,
        ):
            ps.add_or_update_domain_entry.return_value = (True, "ok")
            result = SettingsService.add_domain_permission_result(
                "my_domain", data, session_mgr, settings
            )

        assert result["success"]

    def test_no_registry_raises(self):
        session_mgr, settings = _mock_context()
        data = {
            "principal": "a@b.com",
            "principal_type": "user",
            "display_name": "A",
            "role": "viewer",
        }

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", EMPTY_REGISTRY),
            ),
            pytest.raises(ValidationError, match="Registry not configured"),
        ):
            SettingsService.add_domain_permission_result(
                "my_domain", data, session_mgr, settings
            )

    def test_save_failure_raises(self):
        session_mgr, settings = _mock_context()
        data = {
            "principal": "a@b.com",
            "principal_type": "user",
            "display_name": "A",
            "role": "viewer",
        }

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "permission_service") as ps,
        ):
            ps.add_or_update_domain_entry.return_value = (False, "disk full")
            with pytest.raises(InfrastructureError):
                SettingsService.add_domain_permission_result(
                    "my_domain", data, session_mgr, settings
                )


class TestDeleteDomainPermission:
    def test_delete_success(self):
        session_mgr, settings = _mock_context()

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "permission_service") as ps,
        ):
            ps.remove_domain_entry.return_value = (True, "ok")
            result = SettingsService.delete_domain_permission_result(
                "my_domain", "a@b.com", session_mgr, settings
            )

        assert result["success"]

    def test_delete_not_found_raises(self):
        session_mgr, settings = _mock_context()

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "permission_service") as ps,
        ):
            ps.remove_domain_entry.return_value = (False, "not found")
            with pytest.raises(InfrastructureError):
                SettingsService.delete_domain_permission_result(
                    "my_domain", "a@b.com", session_mgr, settings
                )

    def test_delete_no_registry_raises(self):
        session_mgr, settings = _mock_context()

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", EMPTY_REGISTRY),
            ),
            pytest.raises(ValidationError, match="Registry not configured"),
        ):
            SettingsService.delete_domain_permission_result(
                "my_domain", "a@b.com", session_mgr, settings
            )


class TestAppLevelRoleValidation:
    """add_permission_result now accepts the builder role."""

    @pytest.mark.parametrize("role", ["viewer", "editor", "builder"])
    def test_accepted_roles(self, role):
        session_mgr, settings = _mock_context()
        data = {
            "principal": "a@b.com",
            "principal_type": "user",
            "display_name": "A",
            "role": role,
        }

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "permission_service") as ps,
        ):
            ps.add_or_update_entry.return_value = (True, "ok")
            result = SettingsService.add_permission_result(data, session_mgr, settings)

        assert result["success"]

    def test_admin_role_rejected(self):
        data = {"principal": "a@b.com", "role": "admin"}
        with pytest.raises(ValidationError, match="Role must be"):
            SettingsService.add_permission_result(data, MagicMock(), MagicMock())

    def test_none_role_rejected(self):
        data = {"principal": "a@b.com", "role": "none"}
        with pytest.raises(ValidationError, match="Role must be"):
            SettingsService.add_permission_result(data, MagicMock(), MagicMock())

    def test_missing_principal_rejected(self):
        data = {"principal": "", "role": "viewer"}
        with pytest.raises(ValidationError, match="Principal"):
            SettingsService.add_permission_result(data, MagicMock(), MagicMock())


class TestSearchWorkspacePrincipals:
    """search_workspace_principals filters app principals, not SCIM."""

    def test_search_users_filters_by_email(self):
        session_mgr, settings = _mock_context()
        settings.ontobricks_app_name = "myapp"

        principals = {
            "users": [
                {
                    "email": "alice@acme.com",
                    "display_name": "Alice Smith",
                    "active": True,
                },
                {"email": "bob@acme.com", "display_name": "Bob Jones", "active": True},
                {
                    "email": "carol@acme.com",
                    "display_name": "Carol White",
                    "active": True,
                },
            ],
            "groups": [],
        }

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "permission_service") as ps,
        ):
            ps.list_app_principals.return_value = principals
            result = SettingsService.search_workspace_principals(
                "alice", "user", session_mgr, settings
            )

        assert result["success"]
        assert len(result["results"]) == 1
        assert result["results"][0]["email"] == "alice@acme.com"

    def test_search_users_matches_display_name(self):
        session_mgr, settings = _mock_context()
        settings.ontobricks_app_name = "myapp"

        principals = {
            "users": [
                {
                    "email": "alice@acme.com",
                    "display_name": "Alice Smith",
                    "active": True,
                },
                {"email": "bob@acme.com", "display_name": "Bob Jones", "active": True},
            ],
            "groups": [],
        }

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "permission_service") as ps,
        ):
            ps.list_app_principals.return_value = principals
            result = SettingsService.search_workspace_principals(
                "jones", "user", session_mgr, settings
            )

        assert len(result["results"]) == 1
        assert result["results"][0]["email"] == "bob@acme.com"

    def test_search_users_case_insensitive(self):
        session_mgr, settings = _mock_context()
        settings.ontobricks_app_name = "myapp"

        principals = {
            "users": [
                {"email": "Alice@Acme.COM", "display_name": "Alice", "active": True}
            ],
            "groups": [],
        }

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "permission_service") as ps,
        ):
            ps.list_app_principals.return_value = principals
            result = SettingsService.search_workspace_principals(
                "alice", "user", session_mgr, settings
            )

        assert len(result["results"]) == 1

    def test_search_groups(self):
        session_mgr, settings = _mock_context()
        settings.ontobricks_app_name = "myapp"

        principals = {
            "users": [],
            "groups": [
                {"display_name": "data-engineers", "id": "g1"},
                {"display_name": "analysts", "id": "g2"},
                {"display_name": "data-science", "id": "g3"},
            ],
        }

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "permission_service") as ps,
        ):
            ps.list_app_principals.return_value = principals
            result = SettingsService.search_workspace_principals(
                "data", "group", session_mgr, settings
            )

        assert result["success"]
        assert len(result["results"]) == 2
        names = {g["display_name"] for g in result["results"]}
        assert names == {"data-engineers", "data-science"}

    def test_search_no_match(self):
        session_mgr, settings = _mock_context()
        settings.ontobricks_app_name = "myapp"

        principals = {
            "users": [
                {"email": "alice@acme.com", "display_name": "Alice", "active": True}
            ],
            "groups": [],
        }

        with (
            patch.object(
                SettingsService,
                "_resolve_context",
                return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
            ),
            patch.object(_svc_module, "permission_service") as ps,
        ):
            ps.list_app_principals.return_value = principals
            result = SettingsService.search_workspace_principals(
                "zzz", "user", session_mgr, settings
            )

        assert result["success"]
        assert len(result["results"]) == 0
