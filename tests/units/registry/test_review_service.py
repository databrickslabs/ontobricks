"""Tests for the ontology review/validation workflow (ReviewService).

Covers the orchestration layer that wires reviewer sign-offs and the
quorum gate on top of the version lifecycle, persisting every decision
to the review audit log. Collaborators (registry service, session,
caches) are mocked; the focus is the workflow rules and side effects.
"""

import importlib

import pytest
from unittest.mock import MagicMock, patch

from back.core.errors import (
    AuthorizationError,
    ConflictError,
    InfrastructureError,
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
from back.objects.registry.ReviewService import (
    ACTION_APPROVED,
    ACTION_CHANGES_REQUESTED,
    ACTION_PUBLISHED,
    ACTION_REOPENED,
    ACTION_SUBMITTED,
    ReviewService,
)

_mod = importlib.import_module("back.objects.registry.ReviewService")


def _request(email="alice@acme.com"):
    req = MagicMock()
    req.state.user_email = email
    req.headers = {}
    return req


def _make_svc(*, status="DRAFT", last_build="2026-01-01", quorum=1,
              versions=("1", "2"), initial_events=None, configured=True):
    info = {"status": status, "last_build": last_build}
    events = [dict(e) for e in (initial_events or [])]

    svc = MagicMock()
    svc.cfg.is_configured = configured
    svc.list_versions_sorted.return_value = list(versions)
    svc.read_version.return_value = (True, {"info": info}, "")
    svc.store.get_domain_quorum.return_value = quorum

    def _set_status(folder, version, new_status):
        info["status"] = new_status
        return True, "ok"

    svc.set_version_status.side_effect = _set_status

    def _record(folder, version, actor, action, *, from_status="",
                to_status="", comment="", meta=None):
        events.append({
            "folder": folder, "version": version, "actor": actor,
            "action": action, "from_status": from_status,
            "to_status": to_status, "comment": comment,
            "meta": meta or {},
            "created_at": "2026-01-01T00:00:%02d" % len(events),
        })
        return True, ""

    svc.record_review_event.side_effect = _record
    svc.list_review_events.side_effect = lambda folder, version=None: list(events)
    return svc, info, events


def _patch(svc):
    domain = MagicMock()
    domain.domain_folder = "other"
    domain.current_version = "9"
    domain.info = {}
    return patch.multiple(
        _mod,
        get_domain=MagicMock(return_value=domain),
        invalidate_registry_cache=MagicMock(),
    ), patch.object(_mod.RegistryService, "from_context", return_value=svc)


def _call(method, svc, **kwargs):
    p1, p2 = _patch(svc)
    with p1, p2:
        return getattr(ReviewService, method)(
            _request(kwargs.pop("email", "alice@acme.com")),
            MagicMock(),  # session_mgr
            MagicMock(),  # settings
            "acme",
            "2",
            **kwargs,
        )


# ----------------------------------------------------------------------
# Submit
# ----------------------------------------------------------------------


def test_submit_draft_to_review_succeeds():
    svc, info, events = _make_svc(status="DRAFT")
    result = _call("submit", svc, comment="ready",
                   user_role="", user_domain_role=ROLE_BUILDER)
    assert result["success"] is True
    assert result["status"] == "IN-REVIEW"
    assert info["status"] == "IN-REVIEW"
    assert events[-1]["action"] == ACTION_SUBMITTED
    assert events[-1]["from_status"] == "DRAFT"
    assert events[-1]["to_status"] == "IN-REVIEW"


def test_submit_requires_builder():
    svc, _, _ = _make_svc(status="DRAFT")
    with pytest.raises(AuthorizationError):
        _call("submit", svc, comment="", user_role="",
              user_domain_role=ROLE_EDITOR)


def test_submit_requires_build():
    svc, _, _ = _make_svc(status="DRAFT", last_build="")
    with pytest.raises(ValidationError):
        _call("submit", svc, comment="", user_role=ROLE_ADMIN,
              user_domain_role=ROLE_NONE)


def test_submit_wrong_status_conflicts():
    svc, _, _ = _make_svc(status="IN-REVIEW")
    with pytest.raises(ConflictError):
        _call("submit", svc, comment="", user_role=ROLE_ADMIN,
              user_domain_role=ROLE_NONE)


# ----------------------------------------------------------------------
# Sign-off
# ----------------------------------------------------------------------


def test_signoff_approve_records_event():
    svc, info, events = _make_svc(status="IN-REVIEW", quorum=2)
    result = _call("signoff", svc, decision="approve", comment="lgtm",
                   user_role="", user_domain_role=ROLE_VIEWER)
    assert result["success"] is True
    assert info["status"] == "IN-REVIEW"  # approve does not change status
    assert events[-1]["action"] == ACTION_APPROVED
    assert result["approvals"] == 1


def test_signoff_approve_twice_conflicts():
    prior = [{
        "folder": "acme", "version": "2", "actor": "alice@acme.com",
        "action": ACTION_SUBMITTED, "from_status": "DRAFT",
        "to_status": "IN-REVIEW", "comment": "", "meta": {},
        "created_at": "2026-01-01T00:00:00",
    }, {
        "folder": "acme", "version": "2", "actor": "alice@acme.com",
        "action": ACTION_APPROVED, "from_status": "", "to_status": "",
        "comment": "", "meta": {}, "created_at": "2026-01-01T00:00:01",
    }]
    svc, _, _ = _make_svc(status="IN-REVIEW", initial_events=prior)
    with pytest.raises(ConflictError):
        _call("signoff", svc, decision="approve", comment="",
              user_role="", user_domain_role=ROLE_VIEWER,
              email="alice@acme.com")


def test_signoff_request_changes_reopens_to_draft():
    svc, info, events = _make_svc(status="IN-REVIEW")
    _call("signoff", svc, decision="request_changes", comment="fix names",
          user_role="", user_domain_role=ROLE_VIEWER)
    assert info["status"] == "DRAFT"
    assert events[-1]["action"] == ACTION_CHANGES_REQUESTED
    assert events[-1]["to_status"] == "DRAFT"


def test_signoff_requires_membership():
    svc, _, _ = _make_svc(status="IN-REVIEW")
    with pytest.raises(AuthorizationError):
        _call("signoff", svc, decision="approve", comment="",
              user_role="", user_domain_role=ROLE_NONE)


def test_signoff_bad_decision_rejected():
    svc, _, _ = _make_svc(status="IN-REVIEW")
    with pytest.raises(ValidationError):
        _call("signoff", svc, decision="maybe", comment="",
              user_role=ROLE_ADMIN, user_domain_role=ROLE_NONE)


# ----------------------------------------------------------------------
# Publish
# ----------------------------------------------------------------------


def test_publish_blocked_until_quorum():
    # A non-admin builder is gated by the quorum.
    svc, _, _ = _make_svc(status="IN-REVIEW", quorum=2)
    with pytest.raises(ConflictError):
        _call("publish", svc, comment="", user_role="",
              user_domain_role=ROLE_BUILDER)


def test_publish_admin_overrides_quorum():
    # An admin may publish even when the quorum is not met.
    svc, info, events = _make_svc(status="IN-REVIEW", quorum=3)
    result = _call("publish", svc, comment="override",
                   user_role=ROLE_ADMIN, user_domain_role=ROLE_NONE)
    assert result["status"] == "PUBLISHED"
    assert info["status"] == "PUBLISHED"
    assert events[-1]["action"] == ACTION_PUBLISHED
    assert events[-1]["meta"]["quorum_override"] is True


def test_publish_domain_admin_overrides_quorum():
    svc, info, _ = _make_svc(status="IN-REVIEW", quorum=2)
    result = _call("publish", svc, comment="override",
                   user_role="", user_domain_role=ROLE_ADMIN)
    assert result["status"] == "PUBLISHED"
    assert info["status"] == "PUBLISHED"


def test_publish_succeeds_when_quorum_met():
    prior = [{
        "folder": "acme", "version": "2", "actor": "bob@acme.com",
        "action": ACTION_SUBMITTED, "from_status": "DRAFT",
        "to_status": "IN-REVIEW", "comment": "", "meta": {},
        "created_at": "2026-01-01T00:00:00",
    }, {
        "folder": "acme", "version": "2", "actor": "carol@acme.com",
        "action": ACTION_APPROVED, "from_status": "", "to_status": "",
        "comment": "", "meta": {}, "created_at": "2026-01-01T00:00:01",
    }]
    svc, info, events = _make_svc(status="IN-REVIEW", quorum=1,
                                  initial_events=prior)
    result = _call("publish", svc, comment="ship it",
                   user_role=ROLE_ADMIN, user_domain_role=ROLE_NONE)
    assert result["status"] == "PUBLISHED"
    assert info["status"] == "PUBLISHED"
    assert events[-1]["action"] == ACTION_PUBLISHED


def test_publish_requires_builder():
    svc, _, _ = _make_svc(status="IN-REVIEW", quorum=1)
    with pytest.raises(AuthorizationError):
        _call("publish", svc, comment="", user_role="",
              user_domain_role=ROLE_VIEWER)


# ----------------------------------------------------------------------
# Reopen
# ----------------------------------------------------------------------


def test_reopen_admin_only():
    svc, _, _ = _make_svc(status="PUBLISHED")
    with pytest.raises(AuthorizationError):
        _call("reopen", svc, comment="", user_role="",
              user_domain_role=ROLE_BUILDER)


def test_reopen_published_to_draft():
    svc, info, events = _make_svc(status="PUBLISHED")
    _call("reopen", svc, comment="bugfix", user_role=ROLE_ADMIN,
          user_domain_role=ROLE_NONE)
    assert info["status"] == "DRAFT"
    assert events[-1]["action"] == "reopened"
    assert events[-1]["from_status"] == "PUBLISHED"


def test_reopen_in_review_to_draft_by_admin():
    svc, info, events = _make_svc(status="IN-REVIEW")
    _call("reopen", svc, comment="needs work", user_role=ROLE_ADMIN,
          user_domain_role=ROLE_NONE)
    assert info["status"] == "DRAFT"
    assert events[-1]["action"] == "reopened"
    assert events[-1]["from_status"] == "IN-REVIEW"


def test_reopen_domain_admin_allowed():
    svc, info, _ = _make_svc(status="IN-REVIEW")
    _call("reopen", svc, comment="", user_role="",
          user_domain_role=ROLE_ADMIN)
    assert info["status"] == "DRAFT"


def test_reopen_rejected_from_draft():
    svc, _, _ = _make_svc(status="DRAFT")
    with pytest.raises(ConflictError):
        _call("reopen", svc, comment="", user_role=ROLE_ADMIN,
              user_domain_role=ROLE_NONE)


def test_review_detail_admin_can_reopen_in_review():
    svc, _, _ = _make_svc(status="IN-REVIEW")
    detail = _call("review_detail", svc, user_role=ROLE_ADMIN,
                   user_domain_role=ROLE_NONE)
    assert detail["actions"]["can_reopen"] is True


def test_review_detail_builder_cannot_reopen():
    svc, _, _ = _make_svc(status="IN-REVIEW")
    detail = _call("review_detail", svc, user_role="",
                   user_domain_role=ROLE_BUILDER)
    assert detail["actions"]["can_reopen"] is False


# ----------------------------------------------------------------------
# Lookup failures
# ----------------------------------------------------------------------


def test_unknown_version_raises_not_found():
    svc, _, _ = _make_svc(status="DRAFT", versions=("1",))
    with pytest.raises(NotFoundError):
        _call("submit", svc, comment="", user_role=ROLE_ADMIN,
              user_domain_role=ROLE_NONE)


def test_registry_not_configured_raises():
    svc, _, _ = _make_svc(configured=False)
    with pytest.raises(ValidationError):
        _call("submit", svc, comment="", user_role=ROLE_ADMIN,
              user_domain_role=ROLE_NONE)


# ----------------------------------------------------------------------
# Pure helpers: summary + pending actions
# ----------------------------------------------------------------------


def test_summarize_resets_approvals_on_resubmit():
    events = [
        {"action": ACTION_SUBMITTED, "actor": "b", "created_at": "t1"},
        {"action": ACTION_APPROVED, "actor": "x@a.com", "created_at": "t2"},
        {"action": ACTION_CHANGES_REQUESTED, "actor": "y@a.com", "created_at": "t3"},
        {"action": ACTION_SUBMITTED, "actor": "b", "created_at": "t4"},
        {"action": ACTION_APPROVED, "actor": "z@a.com", "created_at": "t5"},
    ]
    summary = ReviewService._summarize(events)
    assert summary["approvals"] == 1
    assert summary["approvers"] == ["z@a.com"]
    assert summary["last_activity"] == "t5"


def test_summarize_dedups_same_approver():
    events = [
        {"action": ACTION_SUBMITTED, "actor": "b", "created_at": "t1"},
        {"action": ACTION_APPROVED, "actor": "X@a.com", "created_at": "t2"},
        {"action": ACTION_APPROVED, "actor": "x@a.com", "created_at": "t3"},
    ]
    summary = ReviewService._summarize(events)
    assert summary["approvals"] == 1


def test_pending_actions_draft_builder_can_submit():
    actions = ReviewService._pending_actions(
        "DRAFT", ROLE_BUILDER, "a@a.com",
        {"approvers": [], "approvals": 0}, 1, "2026-01-01")
    assert [a["id"] for a in actions] == ["submit"]


def test_pending_actions_draft_without_build_has_none():
    actions = ReviewService._pending_actions(
        "DRAFT", ROLE_BUILDER, "a@a.com",
        {"approvers": [], "approvals": 0}, 1, "")
    assert actions == []


def test_pending_actions_review_member_can_review():
    actions = ReviewService._pending_actions(
        "IN-REVIEW", ROLE_VIEWER, "a@a.com",
        {"approvers": [], "approvals": 0}, 2, "2026-01-01")
    assert [a["id"] for a in actions] == ["review"]


def test_pending_actions_review_builder_publish_when_quorum_met():
    actions = ReviewService._pending_actions(
        "IN-REVIEW", ROLE_BUILDER, "a@a.com",
        {"approvers": ["x@a.com"], "approvals": 1}, 1, "2026-01-01")
    ids = [a["id"] for a in actions]
    assert "publish" in ids


def test_pending_actions_already_approved_member_has_no_review():
    actions = ReviewService._pending_actions(
        "IN-REVIEW", ROLE_VIEWER, "a@a.com",
        {"approvers": ["a@a.com"], "approvals": 1}, 2, "2026-01-01")
    assert actions == []


def test_pending_actions_builder_no_publish_below_quorum():
    actions = ReviewService._pending_actions(
        "IN-REVIEW", ROLE_BUILDER, "a@a.com",
        {"approvers": [], "approvals": 0}, 2, "2026-01-01")
    assert "publish" not in [a["id"] for a in actions]


def test_pending_actions_admin_publish_below_quorum():
    actions = ReviewService._pending_actions(
        "IN-REVIEW", ROLE_ADMIN, "a@a.com",
        {"approvers": [], "approvals": 0}, 3, "2026-01-01")
    assert "publish" in [a["id"] for a in actions]


def test_review_detail_admin_can_publish_below_quorum():
    svc, _, _ = _make_svc(status="IN-REVIEW", quorum=3)
    result = _call("review_detail", svc,
                   user_role=ROLE_ADMIN, user_domain_role=ROLE_NONE)
    assert result["actions"]["can_publish"] is True
    assert result["publish_override"] is True
    assert result["quorum_met"] is False


# ----------------------------------------------------------------------
# Comment persistence (one comment per status switch -> audit trail)
# ----------------------------------------------------------------------


def test_submit_persists_comment():
    svc, _, events = _make_svc(status="DRAFT")
    _call("submit", svc, comment="please review carefully",
          user_role="", user_domain_role=ROLE_BUILDER)
    assert events[-1]["action"] == ACTION_SUBMITTED
    assert events[-1]["comment"] == "please review carefully"


def test_signoff_approve_persists_comment():
    svc, _, events = _make_svc(status="IN-REVIEW", quorum=2)
    _call("signoff", svc, decision="approve", comment="looks great",
          user_role="", user_domain_role=ROLE_VIEWER)
    assert events[-1]["action"] == ACTION_APPROVED
    assert events[-1]["comment"] == "looks great"


def test_signoff_request_changes_persists_comment():
    svc, _, events = _make_svc(status="IN-REVIEW")
    _call("signoff", svc, decision="request_changes",
          comment="rename Person -> Individual",
          user_role="", user_domain_role=ROLE_VIEWER)
    assert events[-1]["action"] == ACTION_CHANGES_REQUESTED
    assert events[-1]["comment"] == "rename Person -> Individual"


def test_publish_persists_comment():
    prior = [{
        "folder": "acme", "version": "2", "actor": "carol@acme.com",
        "action": ACTION_APPROVED, "from_status": "", "to_status": "",
        "comment": "", "meta": {}, "created_at": "2026-01-01T00:00:01",
    }]
    svc, _, events = _make_svc(status="IN-REVIEW", quorum=1,
                               initial_events=prior)
    _call("publish", svc, comment="shipping v2",
          user_role=ROLE_ADMIN, user_domain_role=ROLE_NONE)
    assert events[-1]["action"] == ACTION_PUBLISHED
    assert events[-1]["comment"] == "shipping v2"


def test_reopen_persists_comment():
    svc, _, events = _make_svc(status="PUBLISHED")
    _call("reopen", svc, comment="hotfix needed",
          user_role=ROLE_ADMIN, user_domain_role=ROLE_NONE)
    assert events[-1]["action"] == "reopened"
    assert events[-1]["comment"] == "hotfix needed"


def test_review_detail_surfaces_events_with_comments():
    """The chat-style comment viewer reads ``events`` from review_detail."""
    prior = [{
        "folder": "acme", "version": "2", "actor": "bob@acme.com",
        "action": ACTION_SUBMITTED, "from_status": "DRAFT",
        "to_status": "IN-REVIEW", "comment": "initial submit",
        "meta": {}, "created_at": "2026-01-01T00:00:00",
    }, {
        "folder": "acme", "version": "2", "actor": "carol@acme.com",
        "action": ACTION_APPROVED, "from_status": "", "to_status": "",
        "comment": "lgtm from me", "meta": {},
        "created_at": "2026-01-01T00:00:01",
    }]
    svc, _, _ = _make_svc(status="IN-REVIEW", initial_events=prior)
    result = _call("review_detail", svc,
                   user_role=ROLE_ADMIN, user_domain_role=ROLE_NONE)
    assert result["success"] is True
    comments = [e["comment"] for e in result["events"]]
    assert comments == ["initial submit", "lgtm from me"]


# ----------------------------------------------------------------------
# _summarize — approvals reset on every new review round
# ----------------------------------------------------------------------


def test_summarize_resets_approvals_on_publish():
    """A publish closes the round; later approvals start a fresh count."""
    events = [
        {"action": ACTION_APPROVED, "actor": "x@a.com", "created_at": "t1"},
        {"action": ACTION_PUBLISHED, "actor": "b", "created_at": "t2"},
        {"action": ACTION_APPROVED, "actor": "y@a.com", "created_at": "t3"},
    ]
    summary = ReviewService._summarize(events)
    assert summary["approvers"] == ["y@a.com"]
    assert summary["approvals"] == 1
    assert summary["last_activity"] == "t3"


def test_summarize_resets_approvals_on_reopen():
    """Reopening to DRAFT discards the prior round's sign-offs."""
    events = [
        {"action": ACTION_APPROVED, "actor": "x@a.com", "created_at": "t1"},
        {"action": ACTION_REOPENED, "actor": "admin", "created_at": "t2"},
    ]
    summary = ReviewService._summarize(events)
    assert summary["approvers"] == []
    assert summary["approvals"] == 0
    assert summary["last_activity"] == "t2"


# ----------------------------------------------------------------------
# Publish meta — quorum bookkeeping (override flag)
# ----------------------------------------------------------------------


def test_publish_meta_records_no_override_when_quorum_met():
    """A builder publishing on a met quorum is not an override."""
    prior = [{
        "folder": "acme", "version": "2", "actor": "carol@acme.com",
        "action": ACTION_APPROVED, "from_status": "", "to_status": "",
        "comment": "", "meta": {}, "created_at": "2026-01-01T00:00:01",
    }]
    svc, _, events = _make_svc(status="IN-REVIEW", quorum=1,
                               initial_events=prior)
    _call("publish", svc, comment="ship",
          user_role="", user_domain_role=ROLE_BUILDER)
    meta = events[-1]["meta"]
    assert meta["quorum_override"] is False
    assert meta["approvals"] == 1
    assert meta["quorum"] == 1


# ----------------------------------------------------------------------
# Domain access roster (review_team)
# ----------------------------------------------------------------------


def _call_team(entries, *, users=None, groups=None):
    """Invoke review_team with mocked app principals + per-domain entries.

    ``users`` / ``groups`` model the Databricks App ACL
    (``list_app_principals``); ``entries`` model the per-domain
    ``.domain_permissions.json`` rows.
    """
    domain = MagicMock()
    app_principals = {"users": list(users or []), "groups": list(groups or [])}
    with (
        patch.object(_mod, "get_domain", return_value=domain),
        patch.object(
            _mod.RegistryCfg, "from_domain",
            return_value=MagicMock(as_dict=lambda: {"catalog": "c", "schema": "s"}),
        ),
        patch(
            "back.core.helpers.get_databricks_host_and_token",
            return_value=("https://host", "tok"),
        ),
        patch.object(
            _mod.permission_service, "list_app_principals",
            return_value=app_principals,
        ),
        patch.object(
            _mod.permission_service, "list_domain_entries",
            return_value=entries,
        ),
    ):
        return ReviewService.review_team(
            _request(), MagicMock(), MagicMock(), "acme"
        )


def test_review_team_sorts_by_role_then_name():
    users = [
        {"email": "v@a.com", "display_name": "Viewer One"},
        {"email": "b@a.com", "display_name": "Builder One"},
        {"email": "e@a.com", "display_name": "Editor One"},
    ]
    entries = [
        {"principal": "v@a.com", "role": ROLE_VIEWER},
        {"principal": "b@a.com", "role": ROLE_BUILDER},
        {"principal": "e@a.com", "role": ROLE_EDITOR},
    ]
    result = _call_team(entries, users=users)
    assert result["success"] is True
    assert result["domain"] == "acme"
    assert [m["role"] for m in result["members"]] == [
        ROLE_BUILDER, ROLE_EDITOR, ROLE_VIEWER
    ]


def test_review_team_filters_non_assignable_roles():
    users = [
        {"email": "admin@a.com", "display_name": "Admin"},
        {"email": "v@a.com", "display_name": "Viewer"},
    ]
    entries = [
        {"principal": "admin@a.com", "role": ROLE_ADMIN},
        {"principal": "v@a.com", "role": ROLE_VIEWER},
    ]
    result = _call_team(entries, users=users)
    assert [m["principal"] for m in result["members"]] == ["v@a.com"]


def test_review_team_excludes_entries_without_app_principal():
    """Orphan .domain_permissions.json entries (no App ACL row) are hidden,
    matching the Registry → Teams matrix."""
    users = [{"email": "known@a.com", "display_name": "Known"}]
    entries = [
        {"principal": "known@a.com", "role": ROLE_EDITOR},
        {"principal": "ghost@a.com", "role": ROLE_BUILDER},  # not an app principal
    ]
    result = _call_team(entries, users=users)
    assert [m["principal"] for m in result["members"]] == ["known@a.com"]


def test_review_team_includes_groups():
    groups = [{"display_name": "data-eng"}]
    entries = [{"principal": "data-eng", "role": ROLE_EDITOR}]
    result = _call_team(entries, groups=groups)
    assert result["members"][0]["principal"] == "data-eng"
    assert result["members"][0]["principal_type"] == "group"
    assert result["members"][0]["display_name"] == "data-eng"


def test_review_team_requires_folder():
    with pytest.raises(ValidationError):
        ReviewService.review_team(_request(), MagicMock(), MagicMock(), "")


# ----------------------------------------------------------------------
# My Tasks — cross-domain worklist of pending review actions
# ----------------------------------------------------------------------


def _my_tasks_svc(domains, events=None, *, configured=True):
    svc = MagicMock()
    svc.cfg.is_configured = configured
    svc.list_domain_details_cached.return_value = (True, list(domains), "")
    svc.list_all_review_events.return_value = list(events or [])
    return svc


def _call_my_tasks(svc, *, email="alice@acme.com", app_role=""):
    # Empty app_role makes ``_resolve_roles`` short-circuit to admin on
    # every folder (the local-dev / unresolved-role case), so the worklist
    # logic can be exercised without the Databricks permission lookup.
    req = _request(email)
    req.state.user_role = app_role
    p1, p2 = _patch(svc)
    with p1, p2:
        return ReviewService.my_tasks(req, MagicMock(), MagicMock())


def test_my_tasks_empty_when_registry_not_configured():
    result = _call_my_tasks(_my_tasks_svc([], configured=False))
    assert result == {"success": True, "tasks": []}


def test_my_tasks_lists_draft_submit_action():
    domains = [{
        "name": "acme", "review_quorum": 1,
        "versions": [
            {"version": "2", "status": "DRAFT", "last_build": "2026-01-01"},
        ],
    }]
    result = _call_my_tasks(_my_tasks_svc(domains))
    assert result["success"] is True
    assert len(result["tasks"]) == 1
    task = result["tasks"][0]
    assert (task["domain"], task["version"]) == ("acme", "2")
    assert [a["id"] for a in task["actions"]] == ["submit"]
    assert task["required"] == 1


def test_my_tasks_in_review_lists_review_and_publish_for_admin():
    domains = [{
        "name": "acme", "review_quorum": 2,
        "versions": [
            {"version": "2", "status": "IN-REVIEW", "last_build": "b"},
        ],
    }]
    result = _call_my_tasks(_my_tasks_svc(domains))
    task = result["tasks"][0]
    ids = [a["id"] for a in task["actions"]]
    assert "review" in ids and "publish" in ids  # admin overrides quorum
    assert task["approvals"] == 0
    assert task["required"] == 2


def test_my_tasks_skips_versions_without_pending_actions():
    domains = [{
        "name": "acme", "review_quorum": 1,
        "versions": [
            {"version": "1", "status": "PUBLISHED", "last_build": "b"},
            {"version": "2", "status": "DRAFT", "last_build": ""},
        ],
    }]
    # PUBLISHED has no pending action; DRAFT without a build cannot submit.
    result = _call_my_tasks(_my_tasks_svc(domains))
    assert result["tasks"] == []


def test_my_tasks_sorts_newest_activity_first():
    domains = [{
        "name": "acme", "review_quorum": 1,
        "versions": [
            {"version": "1", "status": "IN-REVIEW", "last_build": "b"},
            {"version": "2", "status": "IN-REVIEW", "last_build": "b"},
        ],
    }]
    events = [
        {"folder": "acme", "version": "1", "action": ACTION_SUBMITTED,
         "actor": "b", "created_at": "2026-01-01T00:00:00"},
        {"folder": "acme", "version": "2", "action": ACTION_SUBMITTED,
         "actor": "b", "created_at": "2026-02-01T00:00:00"},
    ]
    result = _call_my_tasks(_my_tasks_svc(domains, events))
    assert [t["version"] for t in result["tasks"]] == ["2", "1"]


def test_my_tasks_raises_when_domain_listing_fails():
    svc = MagicMock()
    svc.cfg.is_configured = True
    svc.list_domain_details_cached.return_value = (False, [], "boom")
    with pytest.raises(InfrastructureError):
        _call_my_tasks(svc)


# ----------------------------------------------------------------------
# My Tasks — assigned collaborative tasks merged into the worklist
# ----------------------------------------------------------------------


def test_my_tasks_includes_assigned_open_and_in_progress():
    svc = _my_tasks_svc([])  # no review actions, only assigned tasks
    svc.list_tasks_for_assignee.return_value = [
        {"id": "1", "folder": "acme", "version": "2", "title": "fix",
         "status": "open", "assignee": "alice@acme.com"},
        {"id": "2", "folder": "acme", "version": "2", "title": "wip",
         "status": "in_progress", "assignee": "alice@acme.com"},
    ]
    result = _call_my_tasks(svc)
    assert result["success"] is True
    assert result["tasks"] == []
    assert [t["id"] for t in result["assigned_tasks"]] == ["1", "2"]
    svc.list_tasks_for_assignee.assert_called_once_with("alice@acme.com")


def test_my_tasks_assigned_filters_done_and_cancelled():
    svc = _my_tasks_svc([])
    svc.list_tasks_for_assignee.return_value = [
        {"id": "1", "status": "open"},
        {"id": "2", "status": "done"},
        {"id": "3", "status": "cancelled"},
        {"id": "4", "status": "in_progress"},
    ]
    result = _call_my_tasks(svc)
    assert [t["id"] for t in result["assigned_tasks"]] == ["1", "4"]


def test_my_tasks_assigned_resilient_when_backend_errors():
    svc = _my_tasks_svc([])
    svc.list_tasks_for_assignee.side_effect = RuntimeError("tasks table missing")
    result = _call_my_tasks(svc)
    # Worklist still succeeds; the assigned section degrades to empty.
    assert result["success"] is True
    assert result["assigned_tasks"] == []


def test_my_tasks_assigned_empty_without_email():
    svc = _my_tasks_svc([])
    svc.list_tasks_for_assignee.return_value = [{"id": "1", "status": "open"}]
    # app_role "" short-circuits roles to admin, but an empty email must
    # skip the assignee lookup entirely.
    result = _call_my_tasks(svc, email="")
    assert result["assigned_tasks"] == []
    svc.list_tasks_for_assignee.assert_not_called()
