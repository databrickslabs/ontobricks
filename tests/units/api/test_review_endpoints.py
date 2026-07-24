"""Tests for the internal /review router request plumbing.

The handlers resolve the caller's role against the *target* domain
(which may differ from the loaded session domain) and delegate to
``ReviewService`` (covered in
``tests/units/registry/test_review_service.py``). These tests assert the
request wiring only: role resolution, body parsing, and argument
forwarding.
"""

import importlib
from types import SimpleNamespace

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from back.core.errors import ValidationError

_review = importlib.import_module("api.routers.internal.review")


def _request(body=None, *, user_role="admin"):
    req = MagicMock()
    req.json = AsyncMock(return_value=body if body is not None else {})
    req.state = SimpleNamespace(user_role=user_role)
    return req


async def test_my_tasks_delegates():
    with patch.object(
        _review.ReviewService, "my_tasks",
        return_value={"success": True, "tasks": []},
    ) as mt:
        result = await _review.my_tasks(
            _request(), session_mgr=MagicMock(), settings=MagicMock()
        )
    assert result["success"] is True
    mt.assert_called_once()


async def test_detail_resolves_target_domain_role():
    with (
        patch.object(
            _review.SettingsService, "resolve_domain_role",
            return_value="viewer",
        ) as resolve,
        patch.object(
            _review.ReviewService, "review_detail",
            return_value={"success": True},
        ) as detail,
    ):
        result = await _review.review_detail(
            "acme", "2",
            _request(user_role="app_user"),
            session_mgr=MagicMock(), settings=MagicMock(),
        )
    assert result["success"] is True
    assert resolve.call_args.args[1] == "acme"
    assert resolve.call_args.kwargs["app_role"] == "app_user"
    assert detail.call_args.kwargs["user_role"] == "app_user"
    assert detail.call_args.kwargs["user_domain_role"] == "viewer"


async def test_team_delegates_to_service():
    with patch.object(
        _review.ReviewService, "review_team",
        return_value={"success": True, "domain": "acme", "members": []},
    ) as team:
        result = await _review.review_team(
            "acme", "2",
            _request(user_role="viewer"),
            session_mgr=MagicMock(), settings=MagicMock(),
        )
    assert result["success"] is True
    assert result["domain"] == "acme"
    # folder is forwarded; version is dropped (per-domain list).
    assert team.call_args.args[3] == "acme"


async def test_submit_forwards_comment_and_roles():
    body = {"comment": "ready to go"}
    with (
        patch.object(
            _review.SettingsService, "resolve_domain_role",
            return_value="builder",
        ),
        patch.object(
            _review.ReviewService, "submit",
            return_value={"success": True, "status": "IN-REVIEW"},
        ) as submit,
    ):
        result = await _review.submit(
            "acme", "2",
            _request(body, user_role="editor"),
            session_mgr=MagicMock(), settings=MagicMock(),
        )
    assert result["status"] == "IN-REVIEW"
    assert submit.call_args.args[3:] == ("acme", "2")
    assert submit.call_args.kwargs["comment"] == "ready to go"
    assert submit.call_args.kwargs["user_role"] == "editor"
    assert submit.call_args.kwargs["user_domain_role"] == "builder"


async def test_signoff_requires_decision():
    with pytest.raises(ValidationError):
        await _review.signoff(
            "acme", "2",
            _request({"comment": "x"}),
            session_mgr=MagicMock(), settings=MagicMock(),
        )


async def test_signoff_forwards_decision():
    body = {"decision": "approve", "comment": "lgtm"}
    with (
        patch.object(
            _review.SettingsService, "resolve_domain_role",
            return_value="viewer",
        ),
        patch.object(
            _review.ReviewService, "signoff",
            return_value={"success": True},
        ) as signoff,
    ):
        await _review.signoff(
            "acme", "2",
            _request(body, user_role="app_user"),
            session_mgr=MagicMock(), settings=MagicMock(),
        )
    assert signoff.call_args.kwargs["decision"] == "approve"
    assert signoff.call_args.kwargs["comment"] == "lgtm"


async def test_publish_delegates():
    with (
        patch.object(
            _review.SettingsService, "resolve_domain_role",
            return_value="builder",
        ),
        patch.object(
            _review.ReviewService, "publish",
            return_value={"success": True, "status": "PUBLISHED"},
        ) as publish,
    ):
        result = await _review.publish(
            "acme", "2",
            _request({"comment": ""}, user_role="admin"),
            session_mgr=MagicMock(), settings=MagicMock(),
        )
    assert result["status"] == "PUBLISHED"
    publish.assert_called_once()


async def test_reopen_delegates():
    with (
        patch.object(
            _review.SettingsService, "resolve_domain_role",
            return_value="admin",
        ) as resolve,
        patch.object(
            _review.ReviewService, "reopen",
            return_value={"success": True, "status": "DRAFT"},
        ) as reopen,
    ):
        result = await _review.reopen(
            "acme", "2",
            _request({"comment": "hotfix"}, user_role="admin"),
            session_mgr=MagicMock(), settings=MagicMock(),
        )
    assert result["status"] == "DRAFT"
    assert resolve.call_args.args[1] == "acme"
    assert reopen.call_args.args[3:] == ("acme", "2")
    assert reopen.call_args.kwargs["comment"] == "hotfix"
    assert reopen.call_args.kwargs["user_role"] == "admin"
    assert reopen.call_args.kwargs["user_domain_role"] == "admin"
    reopen.assert_called_once()
