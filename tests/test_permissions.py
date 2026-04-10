"""Tests for back.objects.registry.permissions — permission service."""
import json
import time
import pytest
from unittest.mock import patch, MagicMock

from back.objects.registry.permissions import (
    PermissionService,
    ROLE_ADMIN,
    ROLE_EDITOR,
    ROLE_VIEWER,
    ROLE_NONE,
)


@pytest.fixture
def svc():
    """Return a fresh PermissionService for each test."""
    return PermissionService()


REGISTRY_CFG = {"catalog": "cat", "schema": "sch", "volume": "OntoBricksRegistry"}


class TestRoleConstants:
    def test_values(self):
        assert ROLE_ADMIN == "admin"
        assert ROLE_EDITOR == "editor"
        assert ROLE_VIEWER == "viewer"
        assert ROLE_NONE == "none"


class TestPermissionsPath:
    def test_path(self, svc):
        path = svc._permissions_path(REGISTRY_CFG)
        assert path == "/Volumes/cat/sch/OntoBricksRegistry/.permissions.json"

    def test_custom_volume(self, svc):
        cfg = {"catalog": "c", "schema": "s", "volume": "MyVol"}
        path = svc._permissions_path(cfg)
        assert "/MyVol/" in path


class TestLoadPermissions:
    def test_load_empty_when_file_missing(self, svc):
        with patch.object(svc, "_new_uc") as mock_uc:
            mock_uc.return_value.read_file.return_value = (False, "", "Not found")
            data = svc.load_permissions("h", "t", REGISTRY_CFG)
            assert data == {"version": 1, "permissions": []}

    def test_load_existing(self, svc):
        perm_data = {"version": 1, "permissions": [{"principal": "a@b.com", "role": "editor"}]}
        with patch.object(svc, "_new_uc") as mock_uc:
            mock_uc.return_value.read_file.return_value = (True, json.dumps(perm_data), "ok")
            data = svc.load_permissions("h", "t", REGISTRY_CFG)
            assert len(data["permissions"]) == 1

    def test_caching(self, svc):
        with patch.object(svc, "_new_uc") as mock_uc:
            mock_uc.return_value.read_file.return_value = (False, "", "")
            svc.load_permissions("h", "t", REGISTRY_CFG)
            svc.load_permissions("h", "t", REGISTRY_CFG)
            assert mock_uc.call_count == 1

    def test_force_bypass_cache(self, svc):
        with patch.object(svc, "_new_uc") as mock_uc:
            mock_uc.return_value.read_file.return_value = (False, "", "")
            svc.load_permissions("h", "t", REGISTRY_CFG)
            svc.load_permissions("h", "t", REGISTRY_CFG, force=True)
            assert mock_uc.call_count == 2


class TestSavePermissions:
    def test_save_success(self, svc):
        data = {"version": 1, "permissions": []}
        with patch.object(svc, "_new_uc") as mock_uc:
            mock_uc.return_value.write_file.return_value = (True, "ok")
            ok, msg = svc.save_permissions("h", "t", REGISTRY_CFG, data)
            assert ok is True

    def test_save_no_registry(self, svc):
        data = {"version": 1, "permissions": []}
        ok, msg = svc.save_permissions("h", "t", {"catalog": "", "schema": ""}, data)
        assert ok is False
        assert "Registry not configured" in msg

    def test_save_write_failure(self, svc):
        data = {"version": 1, "permissions": []}
        with patch.object(svc, "_new_uc") as mock_uc:
            mock_uc.return_value.write_file.return_value = (False, "disk full")
            ok, msg = svc.save_permissions("h", "t", REGISTRY_CFG, data)
            assert ok is False


class TestGetUserRole:
    def test_empty_email(self, svc):
        assert svc.get_user_role("", "h", "t", REGISTRY_CFG, "app") == ROLE_NONE

    def test_admin_gets_admin_role(self, svc):
        with patch.object(svc, "is_admin", return_value=True):
            role = svc.get_user_role("admin@b.com", "h", "t", REGISTRY_CFG, "app")
            assert role == ROLE_ADMIN

    def test_user_in_list(self, svc):
        perms = {
            "version": 1,
            "permissions": [{"principal": "user@b.com", "principal_type": "user", "role": "editor"}],
        }
        with patch.object(svc, "is_admin", return_value=False), \
             patch.object(svc, "load_permissions", return_value=perms):
            role = svc.get_user_role("user@b.com", "h", "t", REGISTRY_CFG, "app")
            assert role == ROLE_EDITOR

    def test_user_not_in_list(self, svc):
        perms = {"version": 1, "permissions": [{"principal": "other@b.com", "principal_type": "user", "role": "viewer"}]}
        with patch.object(svc, "is_admin", return_value=False), \
             patch.object(svc, "load_permissions", return_value=perms), \
             patch.object(svc, "_get_user_groups", return_value=[]):
            role = svc.get_user_role("user@b.com", "h", "t", REGISTRY_CFG, "app")
            assert role == ROLE_NONE

    def test_empty_permissions_non_admin(self, svc):
        perms = {"version": 1, "permissions": []}
        with patch.object(svc, "is_admin", return_value=False), \
             patch.object(svc, "load_permissions", return_value=perms):
            role = svc.get_user_role("user@b.com", "h", "t", REGISTRY_CFG, "app")
            assert role == ROLE_NONE

    def test_group_membership(self, svc):
        perms = {
            "version": 1,
            "permissions": [{"principal": "editors-group", "principal_type": "group", "role": "editor"}],
        }
        with patch.object(svc, "is_admin", return_value=False), \
             patch.object(svc, "load_permissions", return_value=perms), \
             patch.object(svc, "_get_user_groups", return_value=["editors-group"]):
            role = svc.get_user_role("user@b.com", "h", "t", REGISTRY_CFG, "app")
            assert role == ROLE_EDITOR

    def test_case_insensitive_match(self, svc):
        perms = {
            "version": 1,
            "permissions": [{"principal": "User@B.COM", "principal_type": "user", "role": "viewer"}],
        }
        with patch.object(svc, "is_admin", return_value=False), \
             patch.object(svc, "load_permissions", return_value=perms):
            role = svc.get_user_role("user@b.com", "h", "t", REGISTRY_CFG, "app")
            assert role == ROLE_VIEWER


class TestIsAdmin:
    def test_empty_email(self, svc):
        assert svc.is_admin("", "h", "t", "app") is False

    def test_empty_app_name(self, svc):
        assert svc.is_admin("a@b.com", "h", "t", "") is False

    def test_sdk_returns_true(self, svc):
        with patch.object(svc, "_check_admin_sdk", return_value=True):
            assert svc.is_admin("a@b.com", "h", "t", "app") is True

    def test_sdk_returns_false(self, svc):
        with patch.object(svc, "_check_admin_sdk", return_value=False):
            assert svc.is_admin("a@b.com", "h", "t", "app") is False

    def test_sdk_fails_rest_succeeds(self, svc):
        with patch.object(svc, "_check_admin_sdk", return_value=None), \
             patch.object(svc, "_check_admin_rest", return_value=True):
            assert svc.is_admin("a@b.com", "h", "t", "app") is True

    def test_sdk_fails_rest_fails(self, svc):
        with patch.object(svc, "_check_admin_sdk", return_value=None), \
             patch.object(svc, "_check_admin_rest", return_value=False):
            assert svc.is_admin("a@b.com", "h", "t", "app") is False

    def test_cache_hit(self, svc):
        svc._admin_cache["a@b.com"] = (True, time.time())
        assert svc.is_admin("a@b.com", "h", "t", "app") is True


class TestAdminCache:
    def test_clear_all(self, svc):
        svc._admin_cache["a@b.com"] = (True, time.time())
        svc._admin_cache["c@d.com"] = (False, time.time())
        svc.clear_admin_cache()
        assert len(svc._admin_cache) == 0

    def test_clear_specific(self, svc):
        svc._admin_cache["a@b.com"] = (True, time.time())
        svc._admin_cache["c@d.com"] = (False, time.time())
        svc.clear_admin_cache("a@b.com")
        assert "a@b.com" not in svc._admin_cache
        assert "c@d.com" in svc._admin_cache


class TestPrincipalsCache:
    def test_clear(self, svc):
        svc._users_cache = [{"email": "a"}]
        svc._groups_cache = [{"name": "g"}]
        svc.clear_principals_cache()
        assert svc._users_cache is None
        assert svc._groups_cache is None


class TestCRUD:
    def test_list_entries(self, svc):
        perms = {"version": 1, "permissions": [{"principal": "a@b.com"}]}
        with patch.object(svc, "load_permissions", return_value=perms):
            entries = svc.list_entries("h", "t", REGISTRY_CFG)
            assert len(entries) == 1

    def test_add_new_entry(self, svc):
        perms = {"version": 1, "permissions": []}
        with patch.object(svc, "load_permissions", return_value=perms), \
             patch.object(svc, "save_permissions", return_value=(True, "ok")) as mock_save:
            ok, msg = svc.add_or_update_entry("h", "t", REGISTRY_CFG, "a@b.com", "user", "A", "editor")
            assert ok is True
            saved_data = mock_save.call_args[0][3]
            assert len(saved_data["permissions"]) == 1
            assert saved_data["permissions"][0]["role"] == "editor"

    def test_update_existing_entry(self, svc):
        perms = {"version": 1, "permissions": [{"principal": "a@b.com", "principal_type": "user", "display_name": "A", "role": "viewer"}]}
        with patch.object(svc, "load_permissions", return_value=perms), \
             patch.object(svc, "save_permissions", return_value=(True, "ok")) as mock_save:
            ok, msg = svc.add_or_update_entry("h", "t", REGISTRY_CFG, "a@b.com", "user", "A", "editor")
            assert ok is True
            saved_data = mock_save.call_args[0][3]
            assert len(saved_data["permissions"]) == 1
            assert saved_data["permissions"][0]["role"] == "editor"

    def test_remove_entry(self, svc):
        perms = {"version": 1, "permissions": [{"principal": "a@b.com"}]}
        with patch.object(svc, "load_permissions", return_value=perms), \
             patch.object(svc, "save_permissions", return_value=(True, "ok")):
            ok, msg = svc.remove_entry("h", "t", REGISTRY_CFG, "a@b.com")
            assert ok is True

    def test_remove_nonexistent(self, svc):
        perms = {"version": 1, "permissions": [{"principal": "other@b.com"}]}
        with patch.object(svc, "load_permissions", return_value=perms):
            ok, msg = svc.remove_entry("h", "t", REGISTRY_CFG, "a@b.com")
            assert ok is False
            assert "not found" in msg
