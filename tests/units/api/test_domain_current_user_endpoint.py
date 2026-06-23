"""Tests for the ``GET /domain/current-user`` endpoint.

This endpoint backs the discussion pane's "Assign to me" shortcut: the
browser asks the server who it is so the task assignee can be prefilled.
Two identity sources exist — the Databricks Apps proxy headers (app mode)
and the SCIM ``/Me`` lookup via the workspace client (local / PAT mode).
These tests assert the branch selection and the response shape only;
collaborators are mocked.
"""

import importlib
from types import SimpleNamespace

from unittest.mock import MagicMock, patch

_domain = importlib.import_module("api.routers.internal.domain")


def _request(headers=None):
    req = MagicMock()
    req.headers = headers if headers is not None else {}
    return req


async def test_current_user_uses_app_proxy_headers():
    with (
        patch.object(_domain, "is_databricks_app", return_value=True),
        patch.object(_domain, "get_databricks_client") as gdc,
    ):
        result = await _domain.get_current_user(
            _request({"x-forwarded-preferred-username": "alice@acme.com"}),
            session_mgr=MagicMock(), settings=MagicMock(),
        )
    assert result == {"success": True, "email": "alice@acme.com"}
    # App mode must not fall through to the workspace client.
    gdc.assert_not_called()


async def test_current_user_app_mode_prefers_username_over_email():
    headers = {
        "x-forwarded-preferred-username": "alice",
        "x-forwarded-email": "alice@acme.com",
    }
    with (
        patch.object(_domain, "is_databricks_app", return_value=True),
        patch.object(_domain, "get_databricks_client"),
    ):
        result = await _domain.get_current_user(
            _request(headers),
            session_mgr=MagicMock(), settings=MagicMock(),
        )
    assert result["email"] == "alice"


async def test_current_user_app_mode_falls_back_to_email_header():
    with (
        patch.object(_domain, "is_databricks_app", return_value=True),
        patch.object(_domain, "get_databricks_client"),
    ):
        result = await _domain.get_current_user(
            _request({"x-forwarded-email": "bob@acme.com"}),
            session_mgr=MagicMock(), settings=MagicMock(),
        )
    assert result["email"] == "bob@acme.com"


async def test_current_user_local_mode_uses_workspace_client():
    client = SimpleNamespace(
        get_current_user_email=lambda: "carol@acme.com"
    )
    with (
        patch.object(_domain, "is_databricks_app", return_value=False),
        patch.object(_domain, "get_domain", return_value=MagicMock()),
        patch.object(_domain, "get_databricks_client", return_value=client),
    ):
        result = await _domain.get_current_user(
            _request(),
            session_mgr=MagicMock(), settings=MagicMock(),
        )
    assert result == {"success": True, "email": "carol@acme.com"}


async def test_current_user_local_mode_without_client_returns_empty():
    with (
        patch.object(_domain, "is_databricks_app", return_value=False),
        patch.object(_domain, "get_domain", return_value=MagicMock()),
        patch.object(_domain, "get_databricks_client", return_value=None),
    ):
        result = await _domain.get_current_user(
            _request(),
            session_mgr=MagicMock(), settings=MagicMock(),
        )
    assert result == {"success": True, "email": ""}
