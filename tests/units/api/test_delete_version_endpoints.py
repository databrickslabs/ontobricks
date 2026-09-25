import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import ValidationError

domain_router = importlib.import_module("api.routers.internal.domain")
settings_router = importlib.import_module("api.routers.internal.settings")


def _request(role="admin"):
    req = MagicMock()
    req.state = SimpleNamespace(user_role=role)
    return req


async def test_domain_delete_derives_loaded_folder():
    domain = MagicMock()
    domain.domain_folder = "acme"
    domain.uc_domain_folder = "acme"
    with (
        patch.object(domain_router, "get_domain", return_value=domain),
        patch.object(
            domain_router.SettingsService,
            "delete_registry_version_result",
            return_value={"success": True},
        ) as delete,
    ):
        result = await domain_router.delete_domain_version(
            "1",
            _request(),
            session_mgr=MagicMock(),
            settings=MagicMock(),
        )
    assert result["success"] is True
    assert delete.call_args.args[:2] == ("acme", "1")
    assert delete.call_args.kwargs["user_role"] == "admin"


async def test_unsaved_same_name_session_cannot_delete_saved_domain():
    domain = SimpleNamespace(
        domain_folder="",
        uc_domain_folder="acme",
        info={"name": "Acme"},
        current_version="1",
    )
    with (
        patch.object(domain_router, "get_domain", return_value=domain),
        patch.object(
            domain_router.SettingsService,
            "delete_registry_version_result",
        ) as delete,
        pytest.raises(ValidationError, match="not saved"),
    ):
        await domain_router.delete_domain_version(
            "1",
            _request(),
            session_mgr=MagicMock(),
            settings=MagicMock(),
        )
    delete.assert_not_called()


async def test_settings_delete_forwards_target_and_role():
    with patch.object(
        settings_router.config_service,
        "delete_registry_version_result",
        return_value={"success": True},
    ) as delete:
        result = await settings_router.delete_registry_version(
            "acme",
            "1",
            _request(),
            session_mgr=MagicMock(),
            settings=MagicMock(),
        )
    assert result["success"] is True
    assert delete.call_args.args[:2] == ("acme", "1")
    assert delete.call_args.kwargs["user_role"] == "admin"
