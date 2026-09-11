"""Tests for back.objects.registry.PermissionService — permission service."""

import importlib
import json
import time
from unittest.mock import patch

import pytest

# Work around __init__.py re-export shadowing the module path.
_PS_MOD = importlib.import_module("back.objects.registry.PermissionService")

from back.objects.registry.PermissionService import (
    PermissionService,
    ROLE_ADMIN,
    ROLE_APP_USER,
    ROLE_BUILDER,
    ROLE_EDITOR,
    ROLE_VIEWER,
    ROLE_NONE,
    ROLE_HIERARCHY,
    role_level,
    min_role,
)


@pytest.fixture
def svc():
    """Return a fresh PermissionService for each test."""
    return PermissionService()


REGISTRY_CFG = {"catalog": "cat", "schema": "sch", "volume": "OntoBricksRegistry"}
DOMAIN = "my_domain"


class TestRoleConstants:
    def test_values(self):
        assert ROLE_ADMIN == "admin"
        assert ROLE_BUILDER == "builder"
        assert ROLE_EDITOR == "editor"
        assert ROLE_VIEWER == "viewer"
        assert ROLE_APP_USER == "app_user"
        assert ROLE_NONE == "none"

    def test_hierarchy_ordering(self):
        assert ROLE_HIERARCHY[ROLE_NONE] < ROLE_HIERARCHY[ROLE_VIEWER]
        assert ROLE_HIERARCHY[ROLE_VIEWER] < ROLE_HIERARCHY[ROLE_EDITOR]
        assert ROLE_HIERARCHY[ROLE_EDITOR] < ROLE_HIERARCHY[ROLE_BUILDER]
        assert ROLE_HIERARCHY[ROLE_BUILDER] < ROLE_HIERARCHY[ROLE_ADMIN]

    def test_role_level(self):
        assert role_level(ROLE_NONE) == 0
        assert role_level(ROLE_VIEWER) == 1
        assert role_level(ROLE_EDITOR) == 2
        assert role_level(ROLE_BUILDER) == 3
        assert role_level(ROLE_ADMIN) == 4
        assert role_level("unknown") == 0

    def test_min_role(self):
        assert min_role(ROLE_ADMIN, ROLE_EDITOR) == ROLE_EDITOR
        assert min_role(ROLE_EDITOR, ROLE_ADMIN) == ROLE_EDITOR
        assert min_role(ROLE_BUILDER, ROLE_VIEWER) == ROLE_VIEWER
        assert min_role(ROLE_NONE, ROLE_ADMIN) == ROLE_NONE


class TestGetUserRole:
    """New model: app access = admin OR present in list_app_principals."""

    def test_empty_email(self, svc):
        assert svc.get_user_role("", "h", "t", REGISTRY_CFG, "app") == ROLE_NONE

    def test_admin_gets_admin_role(self, svc):
        with patch.object(svc, "is_admin", return_value=True):
            role = svc.get_user_role("admin@b.com", "h", "t", REGISTRY_CFG, "app")
            assert role == ROLE_ADMIN

    def test_app_user_direct_match(self, svc):
        principals = {
            "users": [{"email": "user@b.com", "display_name": "User"}],
            "groups": [],
        }
        with (
            patch.object(svc, "is_admin", return_value=False),
            patch.object(svc, "list_app_principals", return_value=principals),
        ):
            role = svc.get_user_role("user@b.com", "h", "t", REGISTRY_CFG, "app")
            assert role == ROLE_APP_USER

    def test_app_user_via_group(self, svc):
        principals = {
            "users": [],
            "groups": [{"display_name": "data-team"}],
        }
        with (
            patch.object(svc, "is_admin", return_value=False),
            patch.object(svc, "list_app_principals", return_value=principals),
            patch.object(svc, "_get_user_groups", return_value=["data-team"]),
        ):
            role = svc.get_user_role("user@b.com", "h", "t", REGISTRY_CFG, "app")
            assert role == ROLE_APP_USER

    def test_not_in_principals(self, svc):
        principals = {"users": [{"email": "other@b.com"}], "groups": []}
        with (
            patch.object(svc, "is_admin", return_value=False),
            patch.object(svc, "list_app_principals", return_value=principals),
            patch.object(svc, "_get_user_groups", return_value=[]),
        ):
            role = svc.get_user_role("user@b.com", "h", "t", REGISTRY_CFG, "app")
            assert role == ROLE_NONE

    def test_empty_app_name_returns_none(self, svc):
        with patch.object(svc, "is_admin", return_value=False):
            role = svc.get_user_role("user@b.com", "h", "t", REGISTRY_CFG, "")
            assert role == ROLE_NONE

    def test_case_insensitive_email_match(self, svc):
        principals = {"users": [{"email": "User@B.COM"}], "groups": []}
        with (
            patch.object(svc, "is_admin", return_value=False),
            patch.object(svc, "list_app_principals", return_value=principals),
        ):
            role = svc.get_user_role("user@b.com", "h", "t", REGISTRY_CFG, "app")
            assert role == ROLE_APP_USER


class TestIsAdmin:
    def test_empty_email(self, svc):
        assert svc.is_admin("", "h", "t", "app") is False

    def test_empty_app_name(self, svc):
        assert svc.is_admin("a@b.com", "h", "t", "") is False

    def test_sdk_returns_true(self, svc):
        with (
            patch.object(svc, "_can_manage_principals_sdk", return_value=["a@b.com"]),
            patch.object(svc, "_get_user_groups", return_value=[]),
        ):
            assert svc.is_admin("a@b.com", "h", "t", "app") is True

    def test_sdk_returns_false(self, svc):
        with (
            patch.object(svc, "_can_manage_principals_sdk", return_value=["z@b.com"]),
            patch.object(svc, "_get_user_groups", return_value=[]),
        ):
            assert svc.is_admin("a@b.com", "h", "t", "app") is False

    def test_sdk_fails_rest_succeeds(self, svc):
        with (
            patch.object(svc, "_can_manage_principals_sdk", return_value=None),
            patch.object(svc, "_can_manage_principals_rest", return_value=["a@b.com"]),
            patch.object(svc, "_get_user_groups", return_value=[]),
        ):
            assert svc.is_admin("a@b.com", "h", "t", "app") is True

    def test_cache_hit(self, svc):
        svc._admin_cache["a@b.com"] = (True, time.time())
        assert svc.is_admin("a@b.com", "h", "t", "app") is True

    def test_can_manage_group_makes_member_admin(self, svc):
        """CAN_MANAGE on a group is inherited by every member."""
        with (
            patch.object(svc, "_can_manage_principals_sdk", return_value=["data-eng"]),
            patch.object(svc, "_get_user_groups", return_value=["data-eng"]),
        ):
            assert svc.is_admin("a@b.com", "h", "t", "app") is True

    def test_can_manage_group_does_not_leak_to_non_member(self, svc):
        with (
            patch.object(svc, "_can_manage_principals_sdk", return_value=["data-eng"]),
            patch.object(svc, "_get_user_groups", return_value=["marketing"]),
        ):
            assert svc.is_admin("a@b.com", "h", "t", "app") is False

    def test_group_match_is_case_insensitive(self, svc):
        with (
            patch.object(svc, "_can_manage_principals_sdk", return_value=["Data-Eng"]),
            patch.object(svc, "_get_user_groups", return_value=["data-eng"]),
        ):
            assert svc.is_admin("a@b.com", "h", "t", "app") is True

    def test_user_token_path_wins_over_sdk(self, svc):
        """A definitive answer from the user token stops the fallback chain."""
        with (
            patch.object(
                svc, "_can_manage_principals_rest", return_value=["a@b.com"]
            ) as rest,
            patch.object(svc, "_can_manage_principals_sdk") as sdk,
            patch.object(svc, "_get_user_groups", return_value=[]),
        ):
            assert svc.is_admin("a@b.com", "h", "t", "app", user_token="ut") is True
            rest.assert_called_once_with("h", "ut", "app")
            sdk.assert_not_called()

    def test_all_paths_fail_is_not_admin(self, svc):
        with (
            patch.object(svc, "_can_manage_principals_rest", return_value=None),
            patch.object(svc, "_can_manage_principals_sdk", return_value=None),
        ):
            assert svc.is_admin("a@b.com", "h", "t", "app") is False


class TestExtractCanManage:
    def test_users_and_groups_are_both_returned(self, svc):
        payload = {
            "access_control_list": [
                {
                    "user_name": "alice@x.com",
                    "all_permissions": [{"permission_level": "CAN_MANAGE"}],
                },
                {
                    "group_name": "data-eng",
                    "all_permissions": [{"permission_level": "CAN_MANAGE"}],
                },
                {
                    "group_name": "readers",
                    "all_permissions": [{"permission_level": "CAN_USE"}],
                },
            ]
        }
        assert svc._extract_can_manage(payload) == ["alice@x.com", "data-eng"]

    def test_principal_listed_once_per_acl_entry(self, svc):
        payload = {
            "access_control_list": [
                {
                    "group_name": "data-eng",
                    "all_permissions": [
                        {"permission_level": "CAN_MANAGE"},
                        {"permission_level": "CAN_MANAGE", "inherited": True},
                    ],
                }
            ]
        }
        assert svc._extract_can_manage(payload) == ["data-eng"]

    def test_empty_payload(self, svc):
        assert svc._extract_can_manage({}) == []


class TestGetUserGroups:
    def test_scim_me_is_preferred_when_user_token_present(self, svc):
        with (
            patch.object(
                svc, "_groups_via_scim_me", return_value=["data-eng"]
            ) as me,
            patch.object(svc, "_groups_via_scim_users") as users,
        ):
            groups = svc._get_user_groups("a@b.com", "h", "t", user_token="ut")
            assert groups == ["data-eng"]
            me.assert_called_once_with("h", "ut")
            users.assert_not_called()

    def test_falls_back_to_scim_users_when_me_fails(self, svc):
        with (
            patch.object(svc, "_groups_via_scim_me", return_value=None),
            patch.object(svc, "_groups_via_scim_users", return_value=["fallback"]),
        ):
            groups = svc._get_user_groups("a@b.com", "h", "t", user_token="ut")
            assert groups == ["fallback"]

    def test_failure_is_not_cached(self, svc):
        """A transient 403 must not pin an empty list for the cache window."""
        with (
            patch.object(svc, "_groups_via_scim_me", return_value=None),
            patch.object(svc, "_groups_via_scim_users", return_value=None),
        ):
            assert svc._get_user_groups("a@b.com", "h", "t") == []
        assert "a@b.com" not in svc._user_groups_cache

    def test_success_is_cached(self, svc):
        with patch.object(svc, "_groups_via_scim_me", return_value=["g1"]):
            svc._get_user_groups("a@b.com", "h", "t", user_token="ut")
        with patch.object(svc, "_groups_via_scim_me") as me:
            assert svc._get_user_groups("a@b.com", "h", "t", user_token="ut") == ["g1"]
            me.assert_not_called()


class TestAdminCache:
    def test_clear_all(self, svc):
        svc._admin_cache["a@b.com"] = (True, time.time())
        svc.clear_admin_cache()
        assert len(svc._admin_cache) == 0

    def test_clear_specific(self, svc):
        svc._admin_cache["a@b.com"] = (True, time.time())
        svc._admin_cache["c@d.com"] = (False, time.time())
        svc.clear_admin_cache("a@b.com")
        assert "a@b.com" not in svc._admin_cache
        assert "c@d.com" in svc._admin_cache


class TestPrincipalsCache:
    def test_clear_drops_app_and_workspace_caches(self, svc):
        svc._app_users_cache = [{"email": "a"}]
        svc._app_groups_cache = [{"name": "g"}]
        svc._workspace_users_cache = [{"email": "ws"}]
        svc._workspace_groups_cache = [{"name": "wsg"}]
        svc._admin_cache["a@b.com"] = (True, time.time())
        svc._user_groups_cache["a@b.com"] = (["g1"], time.time())
        svc._app_principals_forbidden = True

        svc.clear_principals_cache()

        assert svc._app_users_cache is None
        assert svc._app_groups_cache is None
        assert svc._workspace_users_cache is None
        assert svc._workspace_groups_cache is None
        assert svc._admin_cache == {}
        assert svc._user_groups_cache == {}
        assert svc.is_app_principals_forbidden() is False

    def test_app_and_workspace_caches_are_isolated(self, svc):
        """``list_app_principals`` (app-scoped ACL) and ``list_users`` /
        ``list_groups`` (full SCIM directory) must not share storage —
        otherwise one call poisons the other and admin/user lookups
        return the wrong set.
        """

        class _AppFakeClient:
            def list_app_principals(self, _app_name):
                return {
                    "users": [{"email": "alice@app"}],
                    "groups": [{"display_name": "app-admins"}],
                }

            @property
            def last_app_permissions_status(self):
                return 200

        class _DirFakeClient:
            def list_workspace_users(self):
                return [
                    {"email": "alice@app"},
                    {"email": "bob@workspace"},
                ]

            def list_workspace_groups(self):
                return [
                    {"display_name": "app-admins"},
                    {"display_name": "everyone"},
                ]

        with patch.object(_PS_MOD, "DatabricksClient", return_value=_AppFakeClient()):
            app = svc.list_app_principals("h", "t", "ontobricks")

        with patch.object(_PS_MOD, "DatabricksClient", return_value=_DirFakeClient()):
            ws_users = svc.list_users("h", "t")
            ws_groups = svc.list_groups("h", "t")

        # App-scoped result is unchanged (would be 2 users / 2 groups
        # if the workspace fetch had overwritten the same cache).
        assert len(app["users"]) == 1
        assert len(app["groups"]) == 1
        # Workspace fetch returns the larger directory.
        assert len(ws_users) == 2
        assert len(ws_groups) == 2

        # A second call to list_app_principals must come from the
        # app-scoped cache and still report the small set.
        with patch.object(
            _PS_MOD,
            "DatabricksClient",
            side_effect=AssertionError("should hit cache, not the API"),
        ):
            app_again = svc.list_app_principals("h", "t", "ontobricks")
        assert len(app_again["users"]) == 1
        assert len(app_again["groups"]) == 1


# ===================================================================
# Domain-level permissions
# ===================================================================


class TestLoadDomainPermissions:
    """Per-domain permissions are now read/written through the active
    :class:`RegistryStore`. The tests mock ``_store_for`` to keep the
    coverage independent from the storage backend.
    """

    def test_load_empty_when_missing(self, svc):
        store = type("S", (), {})()
        store.load_domain_permissions = lambda folder: {
            "version": 1,
            "permissions": [],
        }
        with patch.object(PermissionService, "_store_for", return_value=store):
            data = svc.load_domain_permissions("h", "t", REGISTRY_CFG, DOMAIN)
            assert data == {"version": 1, "permissions": []}

    def test_load_existing(self, svc):
        dp = {
            "version": 1,
            "permissions": [{"principal": "a@b.com", "role": "viewer"}],
        }
        store = type("S", (), {})()
        store.backend = "memory"
        store.load_domain_permissions = lambda folder: dp
        with patch.object(PermissionService, "_store_for", return_value=store):
            data = svc.load_domain_permissions("h", "t", REGISTRY_CFG, DOMAIN)
            assert len(data["permissions"]) == 1

    def test_caching(self, svc):
        store = type("S", (), {})()
        calls = {"n": 0}

        def _load(_folder):
            calls["n"] += 1
            return {"version": 1, "permissions": []}

        store.load_domain_permissions = _load
        with patch.object(PermissionService, "_store_for", return_value=store):
            svc.load_domain_permissions("h", "t", REGISTRY_CFG, DOMAIN)
            svc.load_domain_permissions("h", "t", REGISTRY_CFG, DOMAIN)
        assert calls["n"] == 1


class TestSaveDomainPermissions:
    def test_save_success(self, svc):
        data = {"version": 1, "permissions": []}
        store = type("S", (), {})()
        store.backend = "memory"
        store.save_domain_permissions = lambda folder, payload: (True, "ok")
        with patch.object(PermissionService, "_store_for", return_value=store):
            ok, _msg = svc.save_domain_permissions(
                "h", "t", REGISTRY_CFG, DOMAIN, data
            )
            assert ok is True

    def test_save_no_registry(self, svc):
        data = {"version": 1, "permissions": []}
        ok, _msg = svc.save_domain_permissions(
            "h", "t", {"catalog": "", "schema": ""}, DOMAIN, data
        )
        assert ok is False


class TestGetDomainRole:
    """Strict model: admin bypasses, otherwise only the team entry counts."""

    def test_admin_always_admin(self, svc):
        with patch.object(svc, "get_user_role", return_value=ROLE_ADMIN):
            role = svc.get_domain_role(
                "a@b.com", "h", "t", REGISTRY_CFG, "app", DOMAIN
            )
            assert role == ROLE_ADMIN

    def test_admin_via_app_role_kwarg(self, svc):
        role = svc.get_domain_role(
            "a@b.com", "h", "t", REGISTRY_CFG, "app", DOMAIN, app_role=ROLE_ADMIN
        )
        assert role == ROLE_ADMIN

    def test_empty_domain_returns_none(self, svc):
        with patch.object(svc, "get_user_role", return_value=ROLE_APP_USER):
            role = svc.get_domain_role(
                "a@b.com", "h", "t", REGISTRY_CFG, "app", ""
            )
            assert role == ROLE_NONE

    def test_no_entry_returns_none(self, svc):
        with (
            patch.object(svc, "get_user_role", return_value=ROLE_APP_USER),
            patch.object(svc, "_resolve_domain_entry_role", return_value=None),
        ):
            role = svc.get_domain_role(
                "a@b.com", "h", "t", REGISTRY_CFG, "app", DOMAIN
            )
            assert role == ROLE_NONE

    def test_entry_role_is_returned(self, svc):
        with (
            patch.object(svc, "get_user_role", return_value=ROLE_APP_USER),
            patch.object(svc, "_resolve_domain_entry_role", return_value=ROLE_EDITOR),
        ):
            role = svc.get_domain_role(
                "a@b.com", "h", "t", REGISTRY_CFG, "app", DOMAIN
            )
            assert role == ROLE_EDITOR


class TestResolveDomainEntryRole:
    def test_empty_domain(self, svc):
        result = svc._resolve_domain_entry_role(
            "a@b.com", "h", "t", REGISTRY_CFG, ""
        )
        assert result is None

    def test_no_entries(self, svc):
        dp = {"version": 1, "permissions": []}
        with patch.object(svc, "load_domain_permissions", return_value=dp):
            result = svc._resolve_domain_entry_role(
                "a@b.com", "h", "t", REGISTRY_CFG, DOMAIN
            )
            assert result is None

    def test_user_match(self, svc):
        dp = {
            "version": 1,
            "permissions": [
                {"principal": "a@b.com", "principal_type": "user", "role": "builder"}
            ],
        }
        with patch.object(svc, "load_domain_permissions", return_value=dp):
            result = svc._resolve_domain_entry_role(
                "a@b.com", "h", "t", REGISTRY_CFG, DOMAIN
            )
            assert result == ROLE_BUILDER

    def test_group_match(self, svc):
        dp = {
            "version": 1,
            "permissions": [
                {"principal": "data-team", "principal_type": "group", "role": "viewer"}
            ],
        }
        with (
            patch.object(svc, "load_domain_permissions", return_value=dp),
            patch.object(svc, "_get_user_groups", return_value=["data-team"]),
        ):
            result = svc._resolve_domain_entry_role(
                "a@b.com", "h", "t", REGISTRY_CFG, DOMAIN
            )
            assert result == ROLE_VIEWER

    def test_highest_role_wins_across_two_groups(self, svc):
        dp = {
            "version": 1,
            "permissions": [
                {"principal": "readers", "principal_type": "group", "role": "viewer"},
                {"principal": "builders", "principal_type": "group", "role": "builder"},
            ],
        }
        with (
            patch.object(svc, "load_domain_permissions", return_value=dp),
            patch.object(
                svc, "_get_user_groups", return_value=["readers", "builders"]
            ),
        ):
            result = svc._resolve_domain_entry_role(
                "a@b.com", "h", "t", REGISTRY_CFG, DOMAIN
            )
            assert result == ROLE_BUILDER

    def test_group_role_beats_lower_direct_role(self, svc):
        dp = {
            "version": 1,
            "permissions": [
                {"principal": "a@b.com", "principal_type": "user", "role": "viewer"},
                {"principal": "editors", "principal_type": "group", "role": "editor"},
            ],
        }
        with (
            patch.object(svc, "load_domain_permissions", return_value=dp),
            patch.object(svc, "_get_user_groups", return_value=["editors"]),
        ):
            result = svc._resolve_domain_entry_role(
                "a@b.com", "h", "t", REGISTRY_CFG, DOMAIN
            )
            assert result == ROLE_EDITOR

    def test_direct_role_beats_lower_group_role(self, svc):
        dp = {
            "version": 1,
            "permissions": [
                {"principal": "a@b.com", "principal_type": "user", "role": "builder"},
                {"principal": "readers", "principal_type": "group", "role": "viewer"},
            ],
        }
        with (
            patch.object(svc, "load_domain_permissions", return_value=dp),
            patch.object(svc, "_get_user_groups", return_value=["readers"]),
        ):
            result = svc._resolve_domain_entry_role(
                "a@b.com", "h", "t", REGISTRY_CFG, DOMAIN
            )
            assert result == ROLE_BUILDER

    def test_membership_is_not_resolved_without_group_entries(self, svc):
        """No group rows on the domain means no SCIM round-trip."""
        dp = {
            "version": 1,
            "permissions": [
                {"principal": "a@b.com", "principal_type": "user", "role": "editor"}
            ],
        }
        with (
            patch.object(svc, "load_domain_permissions", return_value=dp),
            patch.object(svc, "_get_user_groups") as groups,
        ):
            result = svc._resolve_domain_entry_role(
                "a@b.com", "h", "t", REGISTRY_CFG, DOMAIN
            )
            assert result == ROLE_EDITOR
            groups.assert_not_called()

    def test_non_member_of_group_gets_no_role(self, svc):
        dp = {
            "version": 1,
            "permissions": [
                {"principal": "data-team", "principal_type": "group", "role": "viewer"}
            ],
        }
        with (
            patch.object(svc, "load_domain_permissions", return_value=dp),
            patch.object(svc, "_get_user_groups", return_value=["other-team"]),
        ):
            result = svc._resolve_domain_entry_role(
                "a@b.com", "h", "t", REGISTRY_CFG, DOMAIN
            )
            assert result is None


class TestDomainPermCRUD:
    def test_list_domain_entries(self, svc):
        dp = {"version": 1, "permissions": [{"principal": "a@b.com"}]}
        with patch.object(svc, "load_domain_permissions", return_value=dp):
            entries = svc.list_domain_entries("h", "t", REGISTRY_CFG, DOMAIN)
            assert len(entries) == 1

    def test_add_new_domain_entry(self, svc):
        dp = {"version": 1, "permissions": []}
        with (
            patch.object(svc, "load_domain_permissions", return_value=dp),
            patch.object(
                svc, "save_domain_permissions", return_value=(True, "ok")
            ) as mock_save,
        ):
            ok, _ = svc.add_or_update_domain_entry(
                "h", "t", REGISTRY_CFG, DOMAIN, "a@b.com", "user", "A", "builder"
            )
            assert ok is True
            saved = mock_save.call_args[0][4]
            assert len(saved["permissions"]) == 1
            assert saved["permissions"][0]["role"] == "builder"

    def test_update_existing_domain_entry(self, svc):
        dp = {
            "version": 1,
            "permissions": [
                {
                    "principal": "a@b.com",
                    "principal_type": "user",
                    "display_name": "A",
                    "role": "viewer",
                }
            ],
        }
        with (
            patch.object(svc, "load_domain_permissions", return_value=dp),
            patch.object(
                svc, "save_domain_permissions", return_value=(True, "ok")
            ) as mock_save,
        ):
            ok, _ = svc.add_or_update_domain_entry(
                "h", "t", REGISTRY_CFG, DOMAIN, "a@b.com", "user", "A", "builder"
            )
            assert ok is True
            saved = mock_save.call_args[0][4]
            assert saved["permissions"][0]["role"] == "builder"

    def test_remove_domain_entry(self, svc):
        dp = {"version": 1, "permissions": [{"principal": "a@b.com"}]}
        with (
            patch.object(svc, "load_domain_permissions", return_value=dp),
            patch.object(svc, "save_domain_permissions", return_value=(True, "ok")),
        ):
            ok, _ = svc.remove_domain_entry(
                "h", "t", REGISTRY_CFG, DOMAIN, "a@b.com"
            )
            assert ok is True

    def test_remove_nonexistent_domain_entry(self, svc):
        dp = {"version": 1, "permissions": [{"principal": "other@b.com"}]}
        with patch.object(svc, "load_domain_permissions", return_value=dp):
            ok, msg = svc.remove_domain_entry(
                "h", "t", REGISTRY_CFG, DOMAIN, "a@b.com"
            )
            assert ok is False
            assert "not found" in msg


class TestDomainPermCache:
    def test_clear_specific(self, svc):
        svc._domain_perm_cache["d1"] = ({"version": 1, "permissions": []}, time.time())
        svc._domain_perm_cache["d2"] = ({"version": 1, "permissions": []}, time.time())
        svc.clear_domain_perm_cache("d1")
        assert "d1" not in svc._domain_perm_cache
        assert "d2" in svc._domain_perm_cache

    def test_clear_all(self, svc):
        svc._domain_perm_cache["d1"] = ({"version": 1, "permissions": []}, time.time())
        svc.clear_domain_perm_cache()
        assert len(svc._domain_perm_cache) == 0


# ===================================================================
# Batch save (matrix UI)
# ===================================================================


class TestSaveDomainPermissionsBatch:
    def test_adds_and_removes_across_domains(self, svc):
        # Each domain starts with one existing entry.
        initial = {
            "acme": {
                "version": 1,
                "permissions": [
                    {
                        "principal": "old@x.com",
                        "principal_type": "user",
                        "display_name": "Old",
                        "role": "viewer",
                    }
                ],
            },
            "beta": {"version": 1, "permissions": []},
        }
        saved_payloads: dict[str, dict] = {}

        def fake_load(_h, _t, _cfg, domain_folder, force=False):
            return json.loads(json.dumps(initial[domain_folder]))

        def fake_save(_h, _t, _cfg, domain_folder, data):
            saved_payloads[domain_folder] = data
            return True, "ok"

        with (
            patch.object(svc, "load_domain_permissions", side_effect=fake_load),
            patch.object(svc, "save_domain_permissions", side_effect=fake_save),
        ):
            changes = [
                # remove old@x.com from acme
                {
                    "domain_folder": "acme",
                    "principal": "old@x.com",
                    "principal_type": "user",
                    "display_name": "Old",
                    "role": None,
                },
                # add new@x.com as editor on acme
                {
                    "domain_folder": "acme",
                    "principal": "new@x.com",
                    "principal_type": "user",
                    "display_name": "New",
                    "role": "editor",
                },
                # add group on beta
                {
                    "domain_folder": "beta",
                    "principal": "eng",
                    "principal_type": "group",
                    "display_name": "eng",
                    "role": "builder",
                },
            ]
            saved, failed = svc.save_domain_permissions_batch(
                "h", "t", REGISTRY_CFG, changes
            )

        assert failed == []
        assert {s["domain"] for s in saved} == {"acme", "beta"}

        acme_perms = saved_payloads["acme"]["permissions"]
        assert len(acme_perms) == 1
        assert acme_perms[0]["principal"] == "new@x.com"
        assert acme_perms[0]["role"] == "editor"

        beta_perms = saved_payloads["beta"]["permissions"]
        assert len(beta_perms) == 1
        assert beta_perms[0]["principal"] == "eng"
        assert beta_perms[0]["role"] == "builder"

    def test_missing_domain_folder_goes_to_failed(self, svc):
        with (
            patch.object(svc, "load_domain_permissions"),
            patch.object(svc, "save_domain_permissions"),
        ):
            saved, failed = svc.save_domain_permissions_batch(
                "h",
                "t",
                REGISTRY_CFG,
                [{"principal": "a@b.com", "role": "viewer"}],
            )
        assert saved == []
        assert failed and "missing domain_folder" in failed[0]["message"]

    def test_no_registry_returns_single_error(self, svc):
        saved, failed = svc.save_domain_permissions_batch(
            "h", "t", {"catalog": "", "schema": ""}, []
        )
        assert saved == []
        assert failed and "Registry not configured" in failed[0]["message"]

    def test_partial_failure(self, svc):
        dp = {"version": 1, "permissions": []}

        def fake_load(_h, _t, _cfg, _df, force=False):
            return dict(dp)

        def fake_save(_h, _t, _cfg, domain_folder, _data):
            if domain_folder == "beta":
                return False, "disk full"
            return True, "ok"

        with (
            patch.object(svc, "load_domain_permissions", side_effect=fake_load),
            patch.object(svc, "save_domain_permissions", side_effect=fake_save),
        ):
            changes = [
                {
                    "domain_folder": "acme",
                    "principal": "a@x.com",
                    "principal_type": "user",
                    "display_name": "A",
                    "role": "viewer",
                },
                {
                    "domain_folder": "beta",
                    "principal": "b@x.com",
                    "principal_type": "user",
                    "display_name": "B",
                    "role": "editor",
                },
            ]
            saved, failed = svc.save_domain_permissions_batch(
                "h", "t", REGISTRY_CFG, changes
            )

        assert {s["domain"] for s in saved} == {"acme"}
        assert {f["domain"] for f in failed} == {"beta"}


class TestAppPrincipalsBootstrap:
    """First-deploy bootstrap signal: 403 from the ACL endpoint."""

    class _FakeClient:
        def __init__(self, users, groups, status):
            self._users = users
            self._groups = groups
            self._status = status

        def list_app_principals(self, _app_name):
            return {"users": self._users, "groups": self._groups}

        @property
        def last_app_permissions_status(self):
            return self._status

    def test_success_clears_bootstrap_flag(self, svc):
        fake = self._FakeClient([{"email": "a@b.com"}], [], 200)
        with patch.object(_PS_MOD, "DatabricksClient", return_value=fake):
            svc.list_app_principals("h", "t", "ontobricks")
        assert svc.is_app_principals_forbidden() is False

    def test_403_sets_bootstrap_flag(self, svc):
        fake = self._FakeClient([], [], 403)
        with patch.object(_PS_MOD, "DatabricksClient", return_value=fake):
            svc.list_app_principals("h", "t", "ontobricks")
        assert svc.is_app_principals_forbidden() is True

    def test_non_403_errors_do_not_set_flag(self, svc):
        fake = self._FakeClient([], [], 500)
        with patch.object(_PS_MOD, "DatabricksClient", return_value=fake):
            svc.list_app_principals("h", "t", "ontobricks")
        assert svc.is_app_principals_forbidden() is False

    def test_clear_cache_resets_flag(self, svc):
        fake = self._FakeClient([], [], 403)
        with patch.object(_PS_MOD, "DatabricksClient", return_value=fake):
            svc.list_app_principals("h", "t", "ontobricks")
        assert svc.is_app_principals_forbidden() is True
        svc.clear_principals_cache()
        assert svc.is_app_principals_forbidden() is False
