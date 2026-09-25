import importlib
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import (
    AuthorizationError,
    ConflictError,
    InfrastructureError,
    NotFoundError,
)
from back.objects.domain.SettingsService import SettingsService
from back.objects.registry.PermissionService import ROLE_ADMIN, ROLE_BUILDER

module = importlib.import_module("back.objects.domain.SettingsService")


def _run(
    *,
    user_role=ROLE_ADMIN,
    target="1",
    versions=("1", "2", "3"),
    status="DRAFT",
    loaded_folder="acme",
    loaded_version="3",
    svc=None,
    clear_status=None,
):
    domain = MagicMock()
    domain.domain_folder = loaded_folder
    domain.current_version = loaded_version
    if svc is None:
        svc = MagicMock()
        svc.cfg.is_configured = True
        svc.list_versions.return_value = (True, list(versions), "")
        svc.read_version.return_value = (True, {"info": {"status": status}}, "")
        svc.delete_version.return_value = (True, "")
    clear_status = clear_status or MagicMock()
    with (
        patch.object(module, "get_domain", return_value=domain),
        patch.object(module.RegistryService, "from_context", return_value=svc),
        patch.object(module, "clear_version_status_cache", new=clear_status),
    ):
        result = SettingsService.delete_registry_version_result(
            "acme",
            target,
            user_role=user_role,
            session_mgr=MagicMock(),
            settings=MagicMock(),
        )
    return result, svc, clear_status


def test_old_unloaded_draft_is_deleted():
    result, svc, clear_status = _run()
    assert result["success"] is True
    svc.delete_version.assert_called_once_with("acme", "1")
    clear_status.assert_called_once()


def test_non_admin_cannot_delete():
    with pytest.raises(AuthorizationError):
        _run(user_role=ROLE_BUILDER)


def test_loaded_version_cannot_delete():
    with pytest.raises(ConflictError, match="Load another"):
        _run(target="2", loaded_version="2")


def test_latest_version_cannot_delete():
    with pytest.raises(ConflictError, match="latest"):
        _run(target="3", loaded_version="2")


def test_non_draft_version_cannot_delete():
    with pytest.raises(ConflictError, match="Draft"):
        _run(status="PUBLISHED")


def test_missing_version_is_not_found():
    with pytest.raises(NotFoundError):
        _run(target="99")


def test_list_failure_is_infrastructure_error_and_stops():
    svc = MagicMock()
    svc.cfg.is_configured = True
    svc.list_versions.return_value = (False, [], "registry unavailable")
    clear_status = MagicMock()

    with pytest.raises(InfrastructureError) as exc_info:
        _run(svc=svc, clear_status=clear_status)

    assert exc_info.value.detail == "registry unavailable"
    svc.read_version.assert_not_called()
    svc.delete_version.assert_not_called()
    clear_status.assert_not_called()


def test_read_failure_is_infrastructure_error_and_stops():
    svc = MagicMock()
    svc.cfg.is_configured = True
    svc.list_versions.return_value = (True, ["1", "2", "3"], "")
    svc.read_version.return_value = (False, {}, "version read failed")
    clear_status = MagicMock()

    with pytest.raises(InfrastructureError) as exc_info:
        _run(svc=svc, clear_status=clear_status)

    assert exc_info.value.detail == "version read failed"
    svc.delete_version.assert_not_called()
    clear_status.assert_not_called()


def test_physical_delete_failure_is_infrastructure_error_without_cache_clear():
    svc = MagicMock()
    svc.cfg.is_configured = True
    svc.list_versions.return_value = (True, ["1", "2", "3"], "")
    svc.read_version.return_value = (True, {"info": {"status": "DRAFT"}}, "")
    svc.delete_version.return_value = (False, "delete failed")
    clear_status = MagicMock()

    with pytest.raises(InfrastructureError) as exc_info:
        _run(svc=svc, clear_status=clear_status)

    assert exc_info.value.detail == "delete failed"
    svc.delete_version.assert_called_once_with("acme", "1")
    clear_status.assert_not_called()


def test_guarded_delete_conflict_stays_conflict_and_keeps_caches():
    svc = MagicMock()
    svc.cfg.is_configured = True
    svc.list_versions.return_value = (True, ["1", "2", "3"], "")
    svc.read_version.return_value = (True, {"info": {"status": "DRAFT"}}, "")
    svc.delete_version.side_effect = ConflictError(
        "Version 1 is no longer Draft; refresh and try again"
    )
    clear_status = MagicMock()

    with pytest.raises(ConflictError, match="no longer Draft"):
        _run(svc=svc, clear_status=clear_status)

    svc.delete_version.assert_called_once_with("acme", "1")
    clear_status.assert_not_called()


def test_partial_cleanup_failure_is_not_reported_as_success():
    svc = MagicMock()
    svc.cfg.is_configured = True
    svc.list_versions.return_value = (True, ["1", "2", "3"], "")
    svc.read_version.return_value = (True, {"info": {"status": "DRAFT"}}, "")
    svc.delete_version.side_effect = InfrastructureError(
        "Registry version metadata was deleted, but Knowledge Store cleanup failed",
        detail="lakebase unavailable",
    )
    clear_status = MagicMock()

    with pytest.raises(InfrastructureError, match="Knowledge Store cleanup failed"):
        _run(svc=svc, clear_status=clear_status)

    clear_status.assert_called_once()
