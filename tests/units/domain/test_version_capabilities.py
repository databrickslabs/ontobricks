"""Tests for server-computed version-card action capabilities."""

import importlib
from unittest.mock import MagicMock, patch

from back.objects.domain.Domain import Domain
from back.objects.domain.SettingsService import SettingsService
from back.objects.registry.PermissionService import ROLE_ADMIN, ROLE_BUILDER

settings_module = importlib.import_module("back.objects.domain.SettingsService")


def _domain_service(*, status="DRAFT", current="3"):
    session = MagicMock()
    session.uc_domain_folder = "acme"
    session.current_version = current
    service = MagicMock()
    service.cfg.is_configured = True
    service.list_versions_sorted.return_value = ["3", "2", "1"]
    service.read_version.side_effect = lambda _folder, version: (
        True,
        {
            "info": {
                "status": status if version == "1" else "DRAFT",
                "description": f"Version {version}",
                "author": "alice@example.com",
                "last_update": "2026-09-25T10:00:00Z",
                "last_build": "2026-09-25T09:00:00Z",
            },
            "ontology": {"classes": [{"name": "Person"}]},
        },
        "",
    )
    return Domain(session), service


def test_version_list_exposes_card_metadata_and_capabilities():
    domain, service = _domain_service()

    result = domain.list_version_details(
        service,
        user_role=ROLE_ADMIN,
        user_domain_role=ROLE_BUILDER,
    )

    old = next(item for item in result["versions"] if item["version"] == "1")
    assert old["last_build"] == "2026-09-25T09:00:00Z"
    assert old["transitions"][0]["target_status"] == "IN-REVIEW"
    assert old["delete_control_visible"] is True
    assert old["can_delete"] is True


def test_latest_version_delete_is_blocked_in_payload():
    domain, service = _domain_service()

    result = domain.list_version_details(service, user_role=ROLE_ADMIN)

    latest = result["versions"][0]
    assert latest["is_active"] is True
    assert latest["can_delete"] is False
    assert "latest" in latest["delete_block_reason"]


def test_unreadable_version_remains_visible_with_actions_blocked():
    domain, service = _domain_service()
    service.read_version.side_effect = [
        (True, {"info": {"status": "DRAFT"}}, ""),
        (False, {}, "permission denied"),
        (True, {"info": {"status": "DRAFT"}}, ""),
    ]

    result = domain.list_version_details(service, user_role=ROLE_ADMIN)

    unreadable = result["versions"][1]
    assert unreadable["version"] == "2"
    assert unreadable["error"] == "permission denied"
    assert unreadable["transitions"] == []
    assert unreadable["can_delete"] is False
    assert unreadable["delete_block_reason"]


def test_registry_listing_gets_same_delete_capability_without_mutating_cache():
    domain = MagicMock()
    domain.domain_folder = "acme"
    domain.current_version = "3"
    cached = [
        {
            "name": "acme",
            "versions": [
                {"version": "3", "status": "DRAFT"},
                {"version": "1", "status": "DRAFT"},
            ],
        }
    ]
    service = MagicMock()
    service.cfg.is_configured = True
    service.list_domain_details_cached.return_value = (True, cached, "")

    with (
        patch.object(settings_module, "get_domain", return_value=domain),
        patch.object(
            settings_module.RegistryService,
            "from_context",
            return_value=service,
        ),
    ):
        result = SettingsService.list_registry_domains_result(
            MagicMock(),
            MagicMock(),
            user_role=ROLE_ADMIN,
        )

    versions = result["domains"][0]["versions"]
    assert versions[0]["can_delete"] is False
    assert versions[1]["can_delete"] is True
    assert "can_delete" not in cached[0]["versions"][0]
