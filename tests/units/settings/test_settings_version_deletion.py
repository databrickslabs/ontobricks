import importlib
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import AuthorizationError, ConflictError, NotFoundError
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
):
    domain = MagicMock()
    domain.domain_folder = loaded_folder
    domain.current_version = loaded_version
    svc = MagicMock()
    svc.cfg.is_configured = True
    svc.list_versions_sorted.return_value = list(reversed(versions))
    svc.read_version.return_value = (True, {"info": {"status": status}}, "")
    svc.delete_version.return_value = (True, "")
    with (
        patch.object(module, "get_domain", return_value=domain),
        patch.object(module.RegistryService, "from_context", return_value=svc),
        patch.object(module, "clear_version_status_cache") as clear_status,
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
