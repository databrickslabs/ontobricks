"""Tests for the internal /comments router request plumbing.

The handlers resolve the caller's role against the *target* domain and
delegate to ``CommentService`` (covered in
``tests/units/registry/test_comment_service.py``). These tests assert the
request wiring only: role resolution, body parsing, and argument
forwarding.
"""

import importlib
from types import SimpleNamespace

from unittest.mock import AsyncMock, MagicMock, patch

_comments = importlib.import_module("api.routers.internal.comments")


def _request(body=None, *, user_role="admin"):
    req = MagicMock()
    req.json = AsyncMock(return_value=body if body is not None else {})
    req.state = SimpleNamespace(user_role=user_role)
    return req


async def test_list_comments_forwards_roles():
    with (
        patch.object(
            _comments.SettingsService, "resolve_domain_role",
            return_value="viewer",
        ),
        patch.object(
            _comments.CommentService, "list_comments",
            return_value={"success": True, "comments": []},
        ) as lc,
    ):
        result = await _comments.list_comments(
            "acme", "2",
            _request(user_role="app_user"),
            session_mgr=MagicMock(), settings=MagicMock(),
        )
    assert result["success"] is True
    assert "anchor_type" not in lc.call_args.kwargs
    assert "anchor_ref" not in lc.call_args.kwargs
    assert lc.call_args.kwargs["user_role"] == "app_user"
    assert lc.call_args.kwargs["user_domain_role"] == "viewer"


async def test_add_comment_forwards_body_fields():
    body = {
        "body": "fix this", "parent_id": "42",
    }
    with (
        patch.object(
            _comments.SettingsService, "resolve_domain_role",
            return_value="editor",
        ),
        patch.object(
            _comments.CommentService, "add_comment",
            return_value={"success": True, "comment": {}},
        ) as ac,
    ):
        await _comments.add_comment(
            "acme", "2", _request(body, user_role="app_user"),
            session_mgr=MagicMock(), settings=MagicMock(),
        )
    assert "anchor_type" not in ac.call_args.kwargs
    assert "anchor_ref" not in ac.call_args.kwargs
    assert ac.call_args.kwargs["body"] == "fix this"
    assert ac.call_args.kwargs["parent_id"] == "42"
    assert ac.call_args.kwargs["user_domain_role"] == "editor"


async def test_resolve_comment_forwards_flag():
    with (
        patch.object(
            _comments.SettingsService, "resolve_domain_role",
            return_value="editor",
        ),
        patch.object(
            _comments.CommentService, "resolve_comment",
            return_value={"success": True, "resolved": False},
        ) as rc,
    ):
        await _comments.resolve_comment(
            "acme", "2", "cid-1",
            _request({"resolved": False}, user_role="app_user"),
            session_mgr=MagicMock(), settings=MagicMock(),
        )
    assert rc.call_args.args[5] == "cid-1"
    assert rc.call_args.kwargs["resolved"] is False


async def test_resolve_comment_defaults_resolved_true():
    with (
        patch.object(
            _comments.SettingsService, "resolve_domain_role",
            return_value="editor",
        ),
        patch.object(
            _comments.CommentService, "resolve_comment",
            return_value={"success": True, "resolved": True},
        ) as rc,
    ):
        await _comments.resolve_comment(
            "acme", "2", "cid-1", _request({}),
            session_mgr=MagicMock(), settings=MagicMock(),
        )
    assert rc.call_args.kwargs["resolved"] is True


async def test_list_tasks_delegates():
    with (
        patch.object(
            _comments.SettingsService, "resolve_domain_role",
            return_value="viewer",
        ),
        patch.object(
            _comments.CommentService, "list_tasks",
            return_value={"success": True, "tasks": []},
        ) as lt,
    ):
        result = await _comments.list_tasks(
            "acme", "2", _request(user_role="app_user"),
            session_mgr=MagicMock(), settings=MagicMock(),
        )
    assert result["success"] is True
    lt.assert_called_once()


async def test_list_assignees_delegates_per_domain():
    with (
        patch.object(
            _comments.SettingsService, "resolve_domain_role",
            return_value="viewer",
        ),
        patch.object(
            _comments.CommentService, "list_assignees",
            return_value={"success": True, "members": []},
        ) as la,
    ):
        result = await _comments.list_assignees(
            "acme", "2", _request(user_role="app_user"),
            session_mgr=MagicMock(), settings=MagicMock(),
        )
    assert result["success"] is True
    assert la.call_args.kwargs["user_role"] == "app_user"
    assert la.call_args.kwargs["user_domain_role"] == "viewer"


async def test_create_task_forwards_fields():
    body = {
        "assignee": "bob@acme.com", "title": "Fix it",
        "description": "details", "due_date": "2026-07-01",
        "comment_id": "9",
    }
    with (
        patch.object(
            _comments.SettingsService, "resolve_domain_role",
            return_value="editor",
        ),
        patch.object(
            _comments.CommentService, "create_task",
            return_value={"success": True, "task": {}},
        ) as ct,
    ):
        await _comments.create_task(
            "acme", "2", _request(body, user_role="app_user"),
            session_mgr=MagicMock(), settings=MagicMock(),
        )
    assert ct.call_args.kwargs["assignee"] == "bob@acme.com"
    assert ct.call_args.kwargs["title"] == "Fix it"
    assert ct.call_args.kwargs["due_date"] == "2026-07-01"
    assert ct.call_args.kwargs["comment_id"] == "9"


async def test_update_task_status_forwards_fields():
    with (
        patch.object(
            _comments.SettingsService, "resolve_domain_role",
            return_value="viewer",
        ),
        patch.object(
            _comments.CommentService, "update_task_status",
            return_value={"success": True, "status": "done"},
        ) as ut,
    ):
        result = await _comments.update_task_status(
            "acme", "2", "task-1",
            _request({"status": "done"}, user_role="app_user"),
            session_mgr=MagicMock(), settings=MagicMock(),
        )
    assert result["status"] == "done"
    assert ut.call_args.args[5] == "task-1"
    assert ut.call_args.kwargs["status"] == "done"
