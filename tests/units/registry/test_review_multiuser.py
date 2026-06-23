"""Multi-user review/validation scenarios (ReviewService).

The single-actor rules are covered in ``test_review_service.py``. This
module exercises the *interactions between several distinct users* that
the audit trail and quorum gate exist for:

* distinct reviewers accumulating sign-offs toward a quorum,
* the same reviewer being unable to satisfy the quorum alone,
* a change-request from one reviewer discarding another's approval,
* a full DRAFT -> IN-REVIEW -> PUBLISHED -> reopen -> resubmit chain
  driven by a builder, two reviewers and an admin, and
* the cross-domain ``my_tasks`` worklist resolving a *real* per-domain
  role for the caller (not the admin short-circuit).

Collaborators are mocked, but the review-event list is shared across the
sequential calls so the workflow rules are exercised end-to-end.
"""

import importlib

import pytest
from unittest.mock import MagicMock, patch

from back.core.errors import ConflictError
from back.objects.registry.PermissionService import (
    ROLE_ADMIN,
    ROLE_BUILDER,
    ROLE_NONE,
    ROLE_VIEWER,
)
from back.objects.registry.ReviewService import (
    ACTION_APPROVED,
    ACTION_PUBLISHED,
    ACTION_SUBMITTED,
    ReviewService,
)

_mod = importlib.import_module("back.objects.registry.ReviewService")


# ----------------------------------------------------------------------
# Shared fixtures (mirror test_review_service.py so a svc accumulates
# events across multiple sequential _call invocations).
# ----------------------------------------------------------------------


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


def _approve(svc, email):
    return _call("signoff", svc, decision="approve", comment="",
                 email=email, user_role="", user_domain_role=ROLE_VIEWER)


# ----------------------------------------------------------------------
# Quorum across distinct reviewers
# ----------------------------------------------------------------------


def test_two_distinct_reviewers_meet_quorum_then_builder_publishes():
    svc, info, events = _make_svc(status="IN-REVIEW", quorum=2)

    _approve(svc, "bob@acme.com")
    detail = _approve(svc, "carol@acme.com")
    assert detail["approvals"] == 2
    assert detail["approvers"] == ["bob@acme.com", "carol@acme.com"]
    assert detail["quorum_met"] is True

    result = _call("publish", svc, comment="ship",
                   email="dave@acme.com", user_role="",
                   user_domain_role=ROLE_BUILDER)
    assert result["status"] == "PUBLISHED"
    assert info["status"] == "PUBLISHED"

    published = events[-1]
    assert published["action"] == ACTION_PUBLISHED
    assert published["actor"] == "dave@acme.com"
    assert published["meta"]["quorum_override"] is False
    assert published["meta"]["approvals"] == 2
    assert [e["actor"] for e in events if e["action"] == ACTION_APPROVED] == [
        "bob@acme.com", "carol@acme.com",
    ]


def test_single_reviewer_below_quorum_blocks_builder_publish():
    svc, info, _ = _make_svc(status="IN-REVIEW", quorum=2)
    _approve(svc, "bob@acme.com")
    with pytest.raises(ConflictError):
        _call("publish", svc, comment="",
              email="dave@acme.com", user_role="",
              user_domain_role=ROLE_BUILDER)
    assert info["status"] == "IN-REVIEW"  # publish was rejected


def test_same_reviewer_cannot_satisfy_quorum_alone():
    svc, _, events = _make_svc(status="IN-REVIEW", quorum=2)
    _approve(svc, "bob@acme.com")
    # Bob signing off a second time is rejected and does not double-count.
    with pytest.raises(ConflictError):
        _approve(svc, "bob@acme.com")
    assert len([e for e in events if e["action"] == ACTION_APPROVED]) == 1
    with pytest.raises(ConflictError):
        _call("publish", svc, comment="",
              email="dave@acme.com", user_role="",
              user_domain_role=ROLE_BUILDER)


def test_approver_email_is_case_insensitive_across_users():
    svc, _, _ = _make_svc(status="IN-REVIEW", quorum=2)
    _approve(svc, "Bob@Acme.com")
    with pytest.raises(ConflictError):
        _approve(svc, "bob@acme.com")  # same person, different casing


def test_request_changes_discards_other_reviewers_approval():
    svc, info, events = _make_svc(status="IN-REVIEW", quorum=2)
    _approve(svc, "bob@acme.com")
    # A second reviewer asks for changes -> reopen + reset the round.
    _call("signoff", svc, decision="request_changes", comment="rename X",
          email="carol@acme.com", user_role="", user_domain_role=ROLE_VIEWER)
    assert info["status"] == "DRAFT"
    summary = ReviewService._summarize(events)
    assert summary["approvals"] == 0
    assert summary["approvers"] == []


# ----------------------------------------------------------------------
# Full lifecycle chain driven by four different users
# ----------------------------------------------------------------------


def test_full_lifecycle_chain_multiuser():
    svc, info, events = _make_svc(status="DRAFT", last_build="2026-01-01",
                                  quorum=2)

    # Builder submits.
    _call("submit", svc, comment="ready", email="alice@acme.com",
          user_role="", user_domain_role=ROLE_BUILDER)
    assert info["status"] == "IN-REVIEW"
    assert events[-1]["action"] == ACTION_SUBMITTED

    # Two distinct reviewers approve.
    _approve(svc, "bob@acme.com")
    _approve(svc, "carol@acme.com")

    # Builder publishes on a met quorum (not an override).
    pub = _call("publish", svc, comment="ship", email="alice@acme.com",
                user_role="", user_domain_role=ROLE_BUILDER)
    assert pub["status"] == "PUBLISHED"
    assert events[-1]["meta"]["quorum_override"] is False

    # An admin reopens for a hotfix.
    _call("reopen", svc, comment="hotfix", email="zoe@acme.com",
          user_role=ROLE_ADMIN, user_domain_role=ROLE_NONE)
    assert info["status"] == "DRAFT"

    # Resubmitting starts a fresh review round with zero approvals.
    detail = _call("submit", svc, comment="round 2", email="alice@acme.com",
                   user_role="", user_domain_role=ROLE_BUILDER)
    assert detail["status"] == "IN-REVIEW"
    assert detail["approvals"] == 0

    # The audit log preserves the whole interleaved history.
    actions = [e["action"] for e in events]
    assert actions == [
        ACTION_SUBMITTED, ACTION_APPROVED, ACTION_APPROVED,
        ACTION_PUBLISHED, "reopened", ACTION_SUBMITTED,
    ]


# ----------------------------------------------------------------------
# My Tasks — real per-domain role resolution for the caller
# ----------------------------------------------------------------------


def _my_tasks_svc(domains, events=None, *, configured=True):
    svc = MagicMock()
    svc.cfg.is_configured = configured
    svc.list_domain_details_cached.return_value = (True, list(domains), "")
    svc.list_all_review_events.return_value = list(events or [])
    svc.list_tasks_for_assignee.return_value = []
    return svc


def _call_my_tasks_roles(svc, role_map, *, email="alice@acme.com",
                         app_role="viewer"):
    """Invoke my_tasks resolving a *real* per-domain role per folder.

    ``app_role`` is non-empty and non-admin so ``_resolve_roles`` performs
    the registry lookup instead of the admin short-circuit; the lookup is
    stubbed from ``role_map`` (folder -> role).
    """
    req = _request(email)
    req.state.user_role = app_role
    domain = MagicMock()
    domain.domain_folder = "other"
    domain.current_version = "9"
    domain.info = {}

    def _role(*args, **kwargs):
        folder = args[5]  # (email, host, token, cfg, app_name, folder, ...)
        return role_map.get(folder, ROLE_NONE)

    with (
        patch.object(_mod, "get_domain", return_value=domain),
        patch.object(_mod.RegistryService, "from_context", return_value=svc),
        patch.object(
            _mod.RegistryCfg, "from_domain",
            return_value=MagicMock(as_dict=lambda: {}),
        ),
        patch(
            "back.core.helpers.get_databricks_host_and_token",
            return_value=("https://host", "tok"),
        ),
        patch.object(_mod.permission_service, "get_domain_role",
                     side_effect=_role),
    ):
        return ReviewService.my_tasks(req, MagicMock(), MagicMock())


def test_my_tasks_lists_only_domains_where_user_has_a_role():
    domains = [
        {"name": "acme", "review_quorum": 1,
         "versions": [{"version": "2", "status": "IN-REVIEW",
                       "last_build": "b"}]},
        {"name": "beta", "review_quorum": 1,
         "versions": [{"version": "1", "status": "IN-REVIEW",
                       "last_build": "b"}]},
    ]
    result = _call_my_tasks_roles(
        _my_tasks_svc(domains), {"acme": ROLE_VIEWER, "beta": ROLE_NONE}
    )
    assert [t["domain"] for t in result["tasks"]] == ["acme"]
    task = result["tasks"][0]
    assert task["your_role"] == ROLE_VIEWER
    assert [a["id"] for a in task["actions"]] == ["review"]


def test_my_tasks_builder_has_no_publish_below_quorum():
    domains = [{
        "name": "acme", "review_quorum": 2,
        "versions": [{"version": "2", "status": "IN-REVIEW",
                      "last_build": "b"}],
    }]
    events = [
        {"folder": "acme", "version": "2", "action": ACTION_SUBMITTED,
         "actor": "x", "created_at": "t1"},
        {"folder": "acme", "version": "2", "action": ACTION_APPROVED,
         "actor": "bob@acme.com", "created_at": "t2"},
    ]
    result = _call_my_tasks_roles(
        _my_tasks_svc(domains, events), {"acme": ROLE_BUILDER},
        email="alice@acme.com",
    )
    task = result["tasks"][0]
    ids = [a["id"] for a in task["actions"]]
    assert task["approvals"] == 1
    assert "publish" not in ids
    # The builder has not approved yet, so they can still review.
    assert "review" in ids


def test_my_tasks_builder_gets_publish_when_distinct_quorum_met():
    domains = [{
        "name": "acme", "review_quorum": 2,
        "versions": [{"version": "2", "status": "IN-REVIEW",
                      "last_build": "b"}],
    }]
    events = [
        {"folder": "acme", "version": "2", "action": ACTION_SUBMITTED,
         "actor": "x", "created_at": "t1"},
        {"folder": "acme", "version": "2", "action": ACTION_APPROVED,
         "actor": "bob@acme.com", "created_at": "t2"},
        {"folder": "acme", "version": "2", "action": ACTION_APPROVED,
         "actor": "carol@acme.com", "created_at": "t3"},
    ]
    result = _call_my_tasks_roles(
        _my_tasks_svc(domains, events), {"acme": ROLE_BUILDER},
        email="alice@acme.com",
    )
    task = result["tasks"][0]
    assert task["approvals"] == 2
    assert "publish" in [a["id"] for a in task["actions"]]
