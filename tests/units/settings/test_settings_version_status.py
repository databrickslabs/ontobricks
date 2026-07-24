"""Tests for the version status-change orchestration in SettingsService.

Covers ``SettingsService.set_registry_version_status_result`` — the layer
that wires the lifecycle state machine (``check_status_transition``) to the
registry store, syncs the loaded session, and invalidates caches. The state
machine itself is unit-tested in ``test_version_lifecycle.py``; here we assert
the orchestration honours its verdicts and the surrounding side effects.
"""

import importlib

import pytest
from unittest.mock import MagicMock, patch

from back.core.errors import (
    AuthorizationError,
    InfrastructureError,
    NotFoundError,
    ValidationError,
)
from back.objects.registry.PermissionService import (
    ROLE_ADMIN,
    ROLE_BUILDER,
    ROLE_EDITOR,
    ROLE_NONE,
)
from back.objects.domain.SettingsService import SettingsService

_svc_module = importlib.import_module("back.objects.domain.SettingsService")


def _run(
    *,
    domain_name="acme",
    version="2",
    new_status="IN-REVIEW",
    user_role="",
    user_domain_role=ROLE_BUILDER,
    current_status="DRAFT",
    last_build="2026-01-01",
    versions=("1", "2", "3"),
    configured=True,
    set_result=(True, "ok"),
    loaded_folder="other",
    loaded_version="9",
    actor_email="alice@acme.com",
):
    """Drive ``set_registry_version_status_result`` against mock collaborators.

    Returns ``(result, svc_mock, domain_mock, caches)`` so callers can assert
    both the payload and the side effects (store call, session sync, cache
    invalidation).
    """
    session_mgr, settings = MagicMock(), MagicMock()

    domain = MagicMock()
    domain.domain_folder = loaded_folder
    domain.current_version = loaded_version
    domain.info = {}

    svc = MagicMock()
    svc.cfg.is_configured = configured
    svc.list_versions_sorted.return_value = list(versions)
    svc.read_version.return_value = (
        True,
        {"info": {"status": current_status, "last_build": last_build}},
        "",
    )
    svc.set_version_status.return_value = set_result

    caches = {"registry": MagicMock(), "version": MagicMock()}

    with (
        patch.object(_svc_module, "get_domain", return_value=domain),
        patch.object(_svc_module.RegistryService, "from_context", return_value=svc),
        patch.object(_svc_module, "invalidate_registry_cache", caches["registry"]),
        patch.object(_svc_module, "clear_version_status_cache", caches["version"]),
    ):
        result = SettingsService.set_registry_version_status_result(
            domain_name,
            version,
            new_status,
            user_role=user_role,
            user_domain_role=user_domain_role,
            actor_email=actor_email,
            session_mgr=session_mgr,
            settings=settings,
        )
    return result, svc, domain, caches


# ----------------------------------------------------------------------
# Happy-path transitions
# ----------------------------------------------------------------------


def test_draft_to_review_builder_succeeds():
    result, svc, _, _ = _run(
        new_status="IN-REVIEW", current_status="DRAFT", user_domain_role=ROLE_BUILDER
    )

    assert result["success"] is True
    assert result["status"] == "IN-REVIEW"
    assert result["previous_status"] == "DRAFT"
    assert result["version"] == "2"
    svc.set_version_status.assert_called_once_with("acme", "2", "IN-REVIEW")


def test_review_to_published_builder_succeeds():
    result, svc, _, _ = _run(
        new_status="PUBLISHED",
        current_status="IN-REVIEW",
        user_domain_role=ROLE_BUILDER,
    )

    assert result["status"] == "PUBLISHED"
    assert result["previous_status"] == "IN-REVIEW"
    svc.set_version_status.assert_called_once_with("acme", "2", "PUBLISHED")


def test_published_to_draft_admin_succeeds():
    result, svc, _, _ = _run(
        new_status="DRAFT",
        current_status="PUBLISHED",
        user_role=ROLE_ADMIN,
        user_domain_role=ROLE_NONE,
    )

    assert result["status"] == "DRAFT"
    assert result["previous_status"] == "PUBLISHED"


def test_new_status_is_normalised_to_upper():
    result, svc, _, _ = _run(new_status="  in-review  ", current_status="DRAFT")

    assert result["status"] == "IN-REVIEW"
    svc.set_version_status.assert_called_once_with("acme", "2", "IN-REVIEW")


# ----------------------------------------------------------------------
# State-machine verdicts are enforced
# ----------------------------------------------------------------------


def test_draft_to_review_requires_build():
    with pytest.raises(ValidationError):
        _run(new_status="IN-REVIEW", current_status="DRAFT", last_build="")


def test_draft_to_review_denied_below_builder():
    with pytest.raises(AuthorizationError):
        _run(
            new_status="IN-REVIEW",
            current_status="DRAFT",
            user_domain_role=ROLE_EDITOR,
        )


def test_published_to_draft_denied_for_non_admin():
    with pytest.raises(AuthorizationError):
        _run(
            new_status="DRAFT",
            current_status="PUBLISHED",
            user_domain_role=ROLE_BUILDER,
        )


def test_draft_to_published_is_illegal():
    with pytest.raises(ValidationError):
        _run(
            new_status="PUBLISHED",
            current_status="DRAFT",
            user_role=ROLE_ADMIN,
        )


def test_no_op_same_status_rejected():
    with pytest.raises(ValidationError):
        _run(new_status="DRAFT", current_status="DRAFT", user_role=ROLE_ADMIN)


def test_invalid_status_rejected():
    with pytest.raises(ValidationError):
        _run(new_status="ARCHIVED", current_status="DRAFT", user_role=ROLE_ADMIN)


def test_failed_transition_does_not_touch_store_or_caches():
    """A rejected transition must not write to the store or bust caches."""
    session_mgr, settings = MagicMock(), MagicMock()

    domain = MagicMock()
    domain.domain_folder = "other"
    domain.current_version = "9"
    domain.info = {}

    svc = MagicMock()
    svc.cfg.is_configured = True
    svc.list_versions_sorted.return_value = ["1", "2", "3"]
    svc.read_version.return_value = (True, {"info": {"status": "DRAFT"}}, "")

    reg_cache, ver_cache = MagicMock(), MagicMock()

    with (
        patch.object(_svc_module, "get_domain", return_value=domain),
        patch.object(_svc_module.RegistryService, "from_context", return_value=svc),
        patch.object(_svc_module, "invalidate_registry_cache", reg_cache),
        patch.object(_svc_module, "clear_version_status_cache", ver_cache),
    ):
        with pytest.raises(ValidationError):
            SettingsService.set_registry_version_status_result(
                "acme",
                "2",
                "PUBLISHED",  # illegal DRAFT -> PUBLISHED
                user_role=ROLE_ADMIN,
                user_domain_role=ROLE_NONE,
                session_mgr=session_mgr,
                settings=settings,
            )

    svc.set_version_status.assert_not_called()
    reg_cache.assert_not_called()
    ver_cache.assert_not_called()
    domain.save.assert_not_called()


# ----------------------------------------------------------------------
# Precondition / lookup failures
# ----------------------------------------------------------------------


def test_unknown_version_raises_not_found():
    with pytest.raises(NotFoundError):
        _run(version="99", versions=("1", "2", "3"))


def test_registry_not_configured_raises():
    with pytest.raises(ValidationError, match="Registry not configured"):
        _run(configured=False)


def test_store_failure_raises_infrastructure_error():
    with pytest.raises(InfrastructureError):
        _run(set_result=(False, "disk full"))


# ----------------------------------------------------------------------
# Session sync + cache invalidation side effects
# ----------------------------------------------------------------------


def test_caches_invalidated_on_success():
    _, _, _, caches = _run(new_status="IN-REVIEW", current_status="DRAFT")

    caches["registry"].assert_called_once()
    caches["version"].assert_called_once()


# ----------------------------------------------------------------------
# Audit attribution: who changed the status is recorded
# ----------------------------------------------------------------------


def test_status_change_records_actor_in_audit_log():
    _, svc, _, _ = _run(
        new_status="IN-REVIEW",
        current_status="DRAFT",
        actor_email="alice@acme.com",
    )

    svc.record_review_event.assert_called_once()
    args, kwargs = svc.record_review_event.call_args
    assert args[0] == "acme"          # folder
    assert args[1] == "2"             # version
    assert args[2] == "alice@acme.com"  # actor
    assert args[3] == "submitted"     # action mapped from -> IN-REVIEW
    assert kwargs["from_status"] == "DRAFT"
    assert kwargs["to_status"] == "IN-REVIEW"


def test_publish_status_change_maps_to_published_action():
    _, svc, _, _ = _run(new_status="PUBLISHED", current_status="IN-REVIEW")
    assert svc.record_review_event.call_args.args[3] == "published"


def test_reopen_status_change_maps_to_reopened_action():
    _, svc, _, _ = _run(
        new_status="DRAFT", current_status="PUBLISHED",
        user_role=ROLE_ADMIN, user_domain_role=ROLE_NONE,
    )
    assert svc.record_review_event.call_args.args[3] == "reopened"


def test_loaded_version_is_synced_in_session():
    # Target domain+version matches the one loaded in the session.
    _, _, domain, _ = _run(
        domain_name="acme",
        version="2",
        new_status="IN-REVIEW",
        current_status="DRAFT",
        loaded_folder="acme",
        loaded_version="2",
    )

    assert domain.info["status"] == "IN-REVIEW"
    domain.save.assert_called_once()


def test_unloaded_version_does_not_touch_session():
    # A different domain/version is changed; the loaded session is untouched.
    _, _, domain, _ = _run(
        domain_name="acme",
        version="2",
        loaded_folder="beta",
        loaded_version="2",
    )

    assert domain.info == {}
    domain.save.assert_not_called()
