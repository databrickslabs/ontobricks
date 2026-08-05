"""Multi-user comment & task scenarios (CommentService).

The single-action rules are covered in ``test_comment_service.py``. This
module exercises *collaboration between distinct users* on the shared
domain discussion:

* a threaded conversation (root + replies) authored by several members,
* one member resolving another member's thread (author vs editor vs
  stranger),
* a task created by one user, assigned to a second, worked by the
  assignee, and refused to an unrelated third user, and
* the unified audit rows (``commented``) that each task event appends.

Collaborators are mocked but the comment/task rows are shared across the
sequential calls so the threading and ownership rules run end-to-end.
"""

import importlib

import pytest
from unittest.mock import MagicMock, patch

from back.core.errors import AuthorizationError, ConflictError
from back.objects.registry.PermissionService import (
    ROLE_ADMIN,
    ROLE_BUILDER,
    ROLE_EDITOR,
    ROLE_NONE,
    ROLE_VIEWER,
)
from back.objects.registry.CommentService import CommentService

_mod = importlib.import_module("back.objects.registry.CommentService")


def _request(email="alice@acme.com"):
    req = MagicMock()
    req.state.user_email = email
    req.headers = {}
    return req


def _make_svc(*, status="DRAFT", versions=("1", "2"), configured=True,
              comments=None, tasks=None):
    info = {"status": status}
    comment_rows = [dict(c) for c in (comments or [])]
    task_rows = [dict(t) for t in (tasks or [])]
    events = []

    svc = MagicMock()
    svc.cfg.is_configured = configured
    svc.list_versions_sorted.return_value = list(versions)
    svc.read_version.return_value = (True, {"info": info}, "")

    def _insert_comment(folder, version, *, author, body, parent_id=None):
        row = {
            "id": str(len(comment_rows) + 1), "folder": folder,
            "version": version, "parent_id": parent_id or "",
            "author": author, "body": body, "resolved": False,
            "created_at": "2026-01-01T00:00:00",
        }
        comment_rows.append(row)
        return dict(row)

    def _list_comments(folder, version=None, *, include_resolved=True):
        return [dict(c) for c in comment_rows]

    def _resolve_comment(folder, comment_id, *, resolved=True):
        for c in comment_rows:
            if c["id"] == str(comment_id):
                c["resolved"] = resolved
                return True, ""
        return False, "Comment not found"

    def _insert_task(folder, version, *, assignee, created_by, title,
                     description="", due_date=None, comment_id=None):
        row = {
            "id": str(len(task_rows) + 1), "folder": folder,
            "version": version, "assignee": assignee,
            "created_by": created_by, "title": title,
            "description": description, "status": "open",
            "due_date": due_date or "", "comment_id": comment_id or "",
            "created_at": "2026-01-01T00:00:00",
            "updated_at": "2026-01-01T00:00:00",
        }
        task_rows.append(row)
        return dict(row)

    def _list_tasks(folder, version=None):
        return [dict(t) for t in task_rows]

    def _update_task_status(folder, task_id, status):
        for t in task_rows:
            if t["id"] == str(task_id):
                t["status"] = status
                return True, ""
        return False, "Task not found"

    def _record(folder, version, actor, action, *, from_status="",
                to_status="", comment="", meta=None):
        events.append({"actor": actor, "action": action,
                       "comment": comment, "meta": meta or {}})
        return True, ""

    svc.insert_comment.side_effect = _insert_comment
    svc.list_comments.side_effect = _list_comments
    svc.resolve_comment.side_effect = _resolve_comment
    svc.insert_task.side_effect = _insert_task
    svc.list_tasks.side_effect = _list_tasks
    svc.update_task_status.side_effect = _update_task_status
    svc.record_review_event.side_effect = _record
    return svc, comment_rows, task_rows, events


def _patch(svc):
    domain = MagicMock()
    return patch.object(_mod, "get_domain", MagicMock(return_value=domain)), \
        patch.object(_mod.RegistryService, "from_context", return_value=svc)


def _call(method, svc, **kwargs):
    email = kwargs.pop("email", "alice@acme.com")
    p1, p2 = _patch(svc)
    with p1, p2:
        return getattr(CommentService, method)(
            _request(email), MagicMock(), MagicMock(), "acme", "2", **kwargs
        )


# ----------------------------------------------------------------------
# Threaded conversation across users
# ----------------------------------------------------------------------


def test_comment_thread_across_three_users():
    svc, comments, _, _ = _make_svc(status="DRAFT")

    root = _call("add_comment", svc, body="Person should be Individual",
                 parent_id=None, email="alice@acme.com",
                 user_role="", user_domain_role=ROLE_EDITOR)
    root_id = root["comment"]["id"]

    _call("add_comment", svc, body="agreed", parent_id=root_id,
          email="bob@acme.com", user_role="", user_domain_role=ROLE_VIEWER)
    _call("add_comment", svc, body="renamed", parent_id=root_id,
          email="carol@acme.com", user_role="", user_domain_role=ROLE_BUILDER)

    listing = _call("list_comments", svc, user_role="",
                    user_domain_role=ROLE_VIEWER)
    rows = listing["comments"]
    assert [c["author"] for c in rows] == [
        "alice@acme.com", "bob@acme.com", "carol@acme.com",
    ]
    # Both replies are anchored to the root comment.
    replies = [c for c in rows if c["parent_id"] == root_id]
    assert {c["author"] for c in replies} == {
        "bob@acme.com", "carol@acme.com",
    }
    assert all(c["parent_id"] == "" for c in rows if c["id"] == root_id)


def test_non_member_cannot_join_the_thread():
    svc, _, _, _ = _make_svc(status="DRAFT")
    with pytest.raises(AuthorizationError):
        _call("add_comment", svc, body="hi", parent_id=None,
              email="outsider@acme.com", user_role="",
              user_domain_role=ROLE_NONE)


# ----------------------------------------------------------------------
# Resolving another user's thread
# ----------------------------------------------------------------------


def _seed_comment(author="bob@acme.com"):
    return [{
        "id": "1", "folder": "acme", "version": "2", "parent_id": "",
        "author": author, "body": "x", "resolved": False, "created_at": "t",
    }]


def test_author_resolves_own_thread():
    svc, comments, _, _ = _make_svc(status="DRAFT",
                                    comments=_seed_comment("bob@acme.com"))
    _call("resolve_comment", svc, comment_id="1", resolved=True,
          email="bob@acme.com", user_role="", user_domain_role=ROLE_VIEWER)
    assert comments[0]["resolved"] is True


def test_editor_resolves_another_users_thread():
    svc, comments, _, _ = _make_svc(status="DRAFT",
                                    comments=_seed_comment("bob@acme.com"))
    _call("resolve_comment", svc, comment_id="1", resolved=True,
          email="carol@acme.com", user_role="", user_domain_role=ROLE_EDITOR)
    assert comments[0]["resolved"] is True


def test_viewer_cannot_resolve_another_users_thread():
    svc, _, _, _ = _make_svc(status="DRAFT",
                             comments=_seed_comment("bob@acme.com"))
    with pytest.raises(AuthorizationError):
        _call("resolve_comment", svc, comment_id="1", resolved=True,
              email="carol@acme.com", user_role="",
              user_domain_role=ROLE_VIEWER)


# ----------------------------------------------------------------------
# Task delegation between distinct users
# ----------------------------------------------------------------------


def test_task_delegated_and_completed_by_assignee_only():
    svc, _, tasks, events = _make_svc(status="DRAFT")

    # Alice (editor) creates a task assigned to Bob.
    created = _call("create_task", svc, assignee="bob@acme.com",
                    title="Fix the SDTM mapping", description="",
                    due_date=None, comment_id="9",
                    email="alice@acme.com", user_role="",
                    user_domain_role=ROLE_EDITOR)
    task_id = created["task"]["id"]
    assert tasks[-1]["assignee"] == "bob@acme.com"
    assert tasks[-1]["created_by"] == "alice@acme.com"
    # Creating the task drops a linked audit row.
    assert events[-1]["action"] == "commented"
    assert events[-1]["meta"]["event"] == "task_created"
    assert events[-1]["meta"]["task_id"] == task_id

    # An unrelated viewer may not advance someone else's task.
    with pytest.raises(AuthorizationError):
        _call("update_task_status", svc, task_id=task_id, status="in_progress",
              email="carol@acme.com", user_role="",
              user_domain_role=ROLE_VIEWER)

    # The assignee can, and completing it audits a task_done row.
    _call("update_task_status", svc, task_id=task_id, status="done",
          email="bob@acme.com", user_role="", user_domain_role=ROLE_VIEWER)
    assert tasks[-1]["status"] == "done"
    assert events[-1]["actor"] == "bob@acme.com"
    assert events[-1]["meta"]["event"] == "task_done"
    assert events[-1]["meta"]["task_id"] == task_id


def test_task_creator_can_also_update_their_delegated_task():
    svc, _, tasks, _ = _make_svc(status="DRAFT")
    created = _call("create_task", svc, assignee="bob@acme.com",
                    title="t", description="", due_date=None, comment_id=None,
                    email="alice@acme.com", user_role="",
                    user_domain_role=ROLE_EDITOR)
    task_id = created["task"]["id"]
    # The creator (alice) is an owner too.
    _call("update_task_status", svc, task_id=task_id, status="cancelled",
          email="alice@acme.com", user_role="", user_domain_role=ROLE_EDITOR)
    assert tasks[-1]["status"] == "cancelled"


def test_admin_can_update_any_users_task():
    svc, _, tasks, _ = _make_svc(status="DRAFT")
    created = _call("create_task", svc, assignee="bob@acme.com",
                    title="t", description="", due_date=None, comment_id=None,
                    email="alice@acme.com", user_role="",
                    user_domain_role=ROLE_BUILDER)
    task_id = created["task"]["id"]
    _call("update_task_status", svc, task_id=task_id, status="in_progress",
          email="zoe@acme.com", user_role=ROLE_ADMIN,
          user_domain_role=ROLE_NONE)
    assert tasks[-1]["status"] == "in_progress"


def test_collaboration_blocked_once_published():
    svc, _, _, _ = _make_svc(status="PUBLISHED")
    with pytest.raises(ConflictError):
        _call("create_task", svc, assignee="bob@acme.com", title="late",
              description="", due_date=None, comment_id=None,
              email="alice@acme.com", user_role=ROLE_ADMIN,
              user_domain_role=ROLE_NONE)
