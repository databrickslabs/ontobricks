"""Tests for the POST /domain/set-version-status internal endpoint.

The handler validates the request body, resolves the caller's role against the
*target* domain (which may differ from the loaded session domain), and
delegates to ``SettingsService.set_registry_version_status_result`` (covered in
``tests/units/settings/test_settings_version_status.py``). These tests assert
the request plumbing only.
"""

import importlib
from types import SimpleNamespace

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from back.core.errors import ValidationError

_domain_router = importlib.import_module("api.routers.internal.domain")


def _request(body: dict, *, user_role: str = "admin"):
    req = MagicMock()
    req.json = AsyncMock(return_value=body)
    req.state = SimpleNamespace(user_role=user_role)
    return req


@pytest.mark.parametrize(
    "body",
    [
        {"version": "2", "status": "PUBLISHED"},          # missing domain_name
        {"domain_name": "acme", "status": "PUBLISHED"},    # missing version
        {"domain_name": "acme", "version": "2"},           # missing status
        {"domain_name": " ", "version": "2", "status": "PUBLISHED"},  # blank
    ],
)
async def test_missing_fields_raise_validation_error(body):
    with pytest.raises(ValidationError):
        await _domain_router.set_version_status(
            _request(body),
            session_mgr=MagicMock(),
            settings=MagicMock(),
        )


async def test_delegates_with_target_domain_role():
    body = {"domain_name": "acme", "version": "2", "status": "IN-REVIEW"}
    session_mgr, settings = MagicMock(), MagicMock()

    with (
        patch.object(
            _domain_router.SettingsService,
            "resolve_domain_role",
            return_value="builder",
        ) as resolve,
        patch.object(
            _domain_router.SettingsService,
            "set_registry_version_status_result",
            return_value={"success": True, "status": "IN-REVIEW"},
        ) as orch,
    ):
        result = await _domain_router.set_version_status(
            _request(body, user_role="editor"),
            session_mgr=session_mgr,
            settings=settings,
        )

    assert result["success"] is True
    # Role resolved against the *target* domain, seeded with the app role.
    resolve.assert_called_once()
    assert resolve.call_args.args[1] == "acme"
    assert resolve.call_args.kwargs["app_role"] == "editor"
    # Both the app role and the resolved domain role are forwarded.
    orch.assert_called_once()
    assert orch.call_args.args == ("acme", "2", "IN-REVIEW")
    assert orch.call_args.kwargs["user_role"] == "editor"
    assert orch.call_args.kwargs["user_domain_role"] == "builder"
