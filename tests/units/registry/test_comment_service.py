"""Tests for the collaborative comments & tasks workflow (CommentService).

Covers the orchestration layer: the DRAFT/IN-REVIEW
write gate, per-action role rules, and the audit-log side effects when a
task is created or completed. Collaborators (registry service, session)
are mocked; the focus is the workflow rules and side effects.
"""

import importlib

import pytest
from unittest.mock import MagicMock, patch

from back.core.errors import (
    AuthorizationError,
    ConflictError,
    NotFoundError,
    ValidationError,
)
from back.objects.registry.PermissionService import (
    ROLE_ADMIN,
    ROLE_BUILDER,
    ROLE_EDITOR,
    ROLE_VIEWER,
    ROLE_NONE,
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
        events.append({"action": action, "comment": comment, "meta": meta or {}})
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
# Comments — create
# ----------------------------------------------------------------------


def test_add_comment_succeeds_on_draft():
    svc, comments, _, _ = _make_svc(status="DRAFT")
    result = _call("add_comment", svc, body="rename this",
                   parent_id=None, user_role="", user_domain_role=ROLE_VIEWER)
    assert result["success"] is True
    assert result["comment"]["body"] == "rename this"
    assert comments[-1]["body"] == "rename this"


def test_add_comment_allowed_in_review():
    svc, _, _, _ = _make_svc(status="IN-REVIEW")
    result = _call("add_comment", svc,
                   body="reviewing", parent_id=None,
                   user_role="", user_domain_role=ROLE_VIEWER)
    assert result["success"] is True


def test_add_comment_blocked_when_published():
    svc, _, _, _ = _make_svc(status="PUBLISHED")
    with pytest.raises(ConflictError):
        _call("add_comment", svc,
              body="late", parent_id=None,
              user_role=ROLE_ADMIN, user_domain_role=ROLE_NONE)


def test_add_comment_requires_member():
    svc, _, _, _ = _make_svc(status="DRAFT")
    with pytest.raises(AuthorizationError):
        _call("add_comment", svc,
              body="hi", parent_id=None,
              user_role="", user_domain_role=ROLE_NONE)


def test_add_comment_requires_body():
    svc, _, _, _ = _make_svc(status="DRAFT")
    with pytest.raises(ValidationError):
        _call("add_comment", svc,
              body="   ", parent_id=None,
              user_role=ROLE_ADMIN, user_domain_role=ROLE_NONE)


# ----------------------------------------------------------------------
# Comments — resolve
# ----------------------------------------------------------------------


def test_resolve_comment_by_author():
    prior = [{
        "id": "1", "folder": "acme", "version": "2",
        "parent_id": "", "author": "alice@acme.com",
        "body": "x", "resolved": False, "created_at": "t",
    }]
    svc, comments, _, _ = _make_svc(status="DRAFT", comments=prior)
    result = _call("resolve_comment", svc, comment_id="1", resolved=True,
                   user_role="", user_domain_role=ROLE_VIEWER,
                   email="alice@acme.com")
    assert result["success"] is True
    assert comments[0]["resolved"] is True


def test_resolve_comment_viewer_non_author_denied():
    prior = [{
        "id": "1", "folder": "acme", "version": "2",
        "parent_id": "", "author": "bob@acme.com",
        "body": "x", "resolved": False, "created_at": "t",
    }]
    svc, _, _, _ = _make_svc(status="DRAFT", comments=prior)
    with pytest.raises(AuthorizationError):
        _call("resolve_comment", svc, comment_id="1", resolved=True,
              user_role="", user_domain_role=ROLE_VIEWER,
              email="carol@acme.com")


def test_resolve_comment_editor_allowed():
    prior = [{
        "id": "1", "folder": "acme", "version": "2",
        "parent_id": "", "author": "bob@acme.com",
        "body": "x", "resolved": False, "created_at": "t",
    }]
    svc, comments, _, _ = _make_svc(status="DRAFT", comments=prior)
    _call("resolve_comment", svc, comment_id="1", resolved=True,
          user_role="", user_domain_role=ROLE_EDITOR, email="carol@acme.com")
    assert comments[0]["resolved"] is True


# ----------------------------------------------------------------------
# Tasks
# ----------------------------------------------------------------------


def test_create_task_succeeds_and_audits():
    svc, _, tasks, events = _make_svc(status="DRAFT")
    result = _call("create_task", svc, assignee="bob@acme.com",
                   title="Fix the mapping", description="details",
                   due_date=None, comment_id="7",
                   user_role="", user_domain_role=ROLE_VIEWER)
    assert result["success"] is True
    assert tasks[-1]["assignee"] == "bob@acme.com"
    # A `commented` audit row links the task + comment.
    assert events[-1]["action"] == "commented"
    assert events[-1]["meta"]["comment_id"] == "7"
    assert events[-1]["meta"]["task_id"] == tasks[-1]["id"]


def test_create_task_requires_assignee():
    svc, _, _, _ = _make_svc(status="DRAFT")
    with pytest.raises(ValidationError):
        _call("create_task", svc, assignee="", title="x",
              description="", due_date=None, comment_id=None,
              user_role=ROLE_ADMIN, user_domain_role=ROLE_NONE)


def test_create_task_requires_title():
    svc, _, _, _ = _make_svc(status="DRAFT")
    with pytest.raises(ValidationError):
        _call("create_task", svc, assignee="bob@acme.com", title=" ",
              description="", due_date=None, comment_id=None,
              user_role=ROLE_ADMIN, user_domain_role=ROLE_NONE)


def test_create_task_blocked_when_published():
    svc, _, _, _ = _make_svc(status="PUBLISHED")
    with pytest.raises(ConflictError):
        _call("create_task", svc, assignee="bob@acme.com", title="x",
              description="", due_date=None, comment_id=None,
              user_role=ROLE_ADMIN, user_domain_role=ROLE_NONE)


def test_update_task_status_by_assignee():
    prior = [{
        "id": "1", "folder": "acme", "version": "2", "assignee": "bob@acme.com",
        "created_by": "alice@acme.com", "title": "t", "description": "",
        "status": "open", "due_date": "", "comment_id": "",
        "created_at": "t", "updated_at": "t",
    }]
    svc, _, tasks, events = _make_svc(status="DRAFT", tasks=prior)
    result = _call("update_task_status", svc, task_id="1", status="done",
                   user_role="", user_domain_role=ROLE_VIEWER,
                   email="bob@acme.com")
    assert result["status"] == "done"
    assert tasks[0]["status"] == "done"
    # Completing a task appends an audit row.
    assert events[-1]["meta"]["event"] == "task_done"


def test_update_task_status_stranger_denied():
    prior = [{
        "id": "1", "folder": "acme", "version": "2", "assignee": "bob@acme.com",
        "created_by": "alice@acme.com", "title": "t", "description": "",
        "status": "open", "due_date": "", "comment_id": "",
        "created_at": "t", "updated_at": "t",
    }]
    svc, _, _, _ = _make_svc(status="DRAFT", tasks=prior)
    with pytest.raises(AuthorizationError):
        _call("update_task_status", svc, task_id="1", status="done",
              user_role="", user_domain_role=ROLE_VIEWER,
              email="carol@acme.com")


def test_update_task_status_admin_allowed():
    prior = [{
        "id": "1", "folder": "acme", "version": "2", "assignee": "bob@acme.com",
        "created_by": "alice@acme.com", "title": "t", "description": "",
        "status": "open", "due_date": "", "comment_id": "",
        "created_at": "t", "updated_at": "t",
    }]
    svc, _, tasks, _ = _make_svc(status="DRAFT", tasks=prior)
    _call("update_task_status", svc, task_id="1", status="in_progress",
          user_role=ROLE_ADMIN, user_domain_role=ROLE_NONE,
          email="admin@acme.com")
    assert tasks[0]["status"] == "in_progress"


def test_update_task_status_bad_status_rejected():
    svc, _, _, _ = _make_svc(status="DRAFT")
    with pytest.raises(ValidationError):
        _call("update_task_status", svc, task_id="1", status="frozen",
              user_role=ROLE_ADMIN, user_domain_role=ROLE_NONE)


def test_update_unknown_task_raises_not_found():
    svc, _, _, _ = _make_svc(status="DRAFT")
    with pytest.raises(NotFoundError):
        _call("update_task_status", svc, task_id="999", status="done",
              user_role=ROLE_ADMIN, user_domain_role=ROLE_NONE)


# ----------------------------------------------------------------------
# Reads
# ----------------------------------------------------------------------


def test_list_comments_returns_whole_domain_thread():
    prior = [
        {"id": "1", "folder": "acme", "version": "2",
         "parent_id": "", "author": "a", "body": "cls", "resolved": False,
         "created_at": "t"},
        {"id": "2", "folder": "acme", "version": "2", "parent_id": "",
         "author": "a", "body": "dom", "resolved": False, "created_at": "t"},
    ]
    svc, _, _, _ = _make_svc(status="DRAFT", comments=prior)
    result = _call("list_comments", svc,
                   user_role="", user_domain_role=ROLE_VIEWER)
    assert [c["body"] for c in result["comments"]] == ["cls", "dom"]


def test_list_comments_requires_member():
    svc, _, _, _ = _make_svc(status="DRAFT")
    with pytest.raises(AuthorizationError):
        _call("list_comments", svc,
              user_role="", user_domain_role=ROLE_NONE)


def test_reads_allowed_on_published():
    """Published versions are read-only, not invisible."""
    svc, _, _, _ = _make_svc(status="PUBLISHED")
    result = _call("list_tasks", svc, user_role="",
                   user_domain_role=ROLE_BUILDER)
    assert result["success"] is True


def test_unknown_version_raises_not_found():
    svc, _, _, _ = _make_svc(status="DRAFT", versions=("1",))
    with pytest.raises(NotFoundError):
        _call("add_comment", svc,
              body="hi", parent_id=None,
              user_role=ROLE_ADMIN, user_domain_role=ROLE_NONE)


# ----------------------------------------------------------------------
# Assignees (task assignee picker — domain permission holders)
# ----------------------------------------------------------------------


def _call_assignees(*, entries=None, role=ROLE_VIEWER, app_role="",
                    raise_exc=False):
    list_entries = (
        MagicMock(side_effect=RuntimeError("boom")) if raise_exc
        else MagicMock(return_value=list(entries or []))
    )
    with (
        patch.object(_mod, "get_domain", MagicMock(return_value=MagicMock())),
        patch.object(
            _mod.RegistryCfg, "from_domain",
            MagicMock(return_value=MagicMock(as_dict=lambda: {})),
        ),
        patch.object(_mod.permission_service, "list_domain_entries", list_entries),
        patch(
            "back.core.helpers.get_databricks_host_and_token",
            MagicMock(return_value=("https://host", "tok")),
        ),
    ):
        return CommentService.list_assignees(
            _request(), MagicMock(), MagicMock(), "acme",
            user_role=app_role, user_domain_role=role,
        )


def test_list_assignees_returns_domain_holders_sorted():
    entries = [
        {"principal": "v@acme.com", "role": ROLE_VIEWER,
         "display_name": "Vic", "principal_type": "user"},
        {"principal": "b@acme.com", "role": ROLE_BUILDER,
         "display_name": "Bea", "principal_type": "user"},
        {"principal": "e@acme.com", "role": ROLE_EDITOR,
         "display_name": "Ed", "principal_type": "user"},
        {"principal": "x@acme.com", "role": "", "display_name": "Nobody"},
    ]
    result = _call_assignees(entries=entries)
    assert result["success"] is True
    members = result["members"]
    # Domain holders, most-privileged first; the role-less entry is dropped.
    assert [m["principal"] for m in members] == [
        "b@acme.com", "e@acme.com", "v@acme.com",
    ]


def test_list_assignees_requires_member():
    with pytest.raises(AuthorizationError):
        _call_assignees(role=ROLE_NONE, app_role="")


def test_list_assignees_resilient_on_backend_error():
    result = _call_assignees(raise_exc=True)
    assert result["success"] is True
    assert result["members"] == []
