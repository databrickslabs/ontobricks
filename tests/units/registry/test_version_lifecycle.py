"""Unit tests for the domain-version lifecycle state machine.

Covers the transition rules, per-transition role tiers, and the
DRAFT->IN-REVIEW build precondition defined in
``back.objects.registry.version_lifecycle``.
"""

import pytest

from back.core.errors import AuthorizationError, ConflictError, ValidationError
from back.objects.registry.PermissionService import (
    ROLE_ADMIN,
    ROLE_BUILDER,
    ROLE_EDITOR,
    ROLE_VIEWER,
    ROLE_NONE,
)
from back.objects.registry.version_lifecycle import (
    STATUS_DRAFT,
    STATUS_IN_REVIEW,
    STATUS_PUBLISHED,
    check_status_transition,
    check_version_deletion,
    is_editable,
    transition_capabilities,
    version_deletion_capability,
)


def _check(
    current,
    new,
    *,
    user_role="",
    user_domain_role="",
    last_build="2026-01-01",
    has_ontology=False,
):
    return check_status_transition(
        current,
        new,
        user_role=user_role,
        user_domain_role=user_domain_role,
        last_build=last_build,
        has_ontology=has_ontology,
    )


# ----------------------------------------------------------------------
# is_editable
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "status,editable",
    [
        (STATUS_DRAFT, True),
        (STATUS_IN_REVIEW, False),
        (STATUS_PUBLISHED, False),
        ("", True),
        (None, True),
    ],
)
def test_is_editable(status, editable):
    assert is_editable(status) is editable


# ----------------------------------------------------------------------
# Allowed transitions (builder tier)
# ----------------------------------------------------------------------


def test_draft_to_review_allowed_for_builder():
    _check(STATUS_DRAFT, STATUS_IN_REVIEW, user_domain_role=ROLE_BUILDER)


def test_draft_to_review_allowed_for_admin():
    _check(STATUS_DRAFT, STATUS_IN_REVIEW, user_role=ROLE_ADMIN)


def test_review_to_draft_allowed_for_builder():
    _check(STATUS_IN_REVIEW, STATUS_DRAFT, user_domain_role=ROLE_BUILDER)


def test_review_to_published_allowed_for_builder():
    _check(STATUS_IN_REVIEW, STATUS_PUBLISHED, user_domain_role=ROLE_BUILDER)


def test_published_to_draft_allowed_for_admin_only():
    _check(STATUS_PUBLISHED, STATUS_DRAFT, user_role=ROLE_ADMIN)


# ----------------------------------------------------------------------
# Role enforcement
# ----------------------------------------------------------------------


@pytest.mark.parametrize("role", [ROLE_EDITOR, ROLE_VIEWER, ROLE_NONE])
def test_draft_to_review_denied_for_below_builder(role):
    with pytest.raises(AuthorizationError):
        _check(STATUS_DRAFT, STATUS_IN_REVIEW, user_domain_role=role)


@pytest.mark.parametrize("role", [ROLE_BUILDER, ROLE_EDITOR, ROLE_VIEWER])
def test_published_to_draft_denied_for_non_admin(role):
    with pytest.raises(AuthorizationError):
        _check(STATUS_PUBLISHED, STATUS_DRAFT, user_domain_role=role)


# ----------------------------------------------------------------------
# Illegal transitions
# ----------------------------------------------------------------------


def test_draft_to_published_is_illegal():
    with pytest.raises(ValidationError):
        _check(STATUS_DRAFT, STATUS_PUBLISHED, user_role=ROLE_ADMIN)


def test_same_status_is_rejected():
    with pytest.raises(ValidationError):
        _check(STATUS_DRAFT, STATUS_DRAFT, user_role=ROLE_ADMIN)


def test_unknown_status_is_rejected():
    with pytest.raises(ValidationError):
        _check(STATUS_DRAFT, "ARCHIVED", user_role=ROLE_ADMIN)


# ----------------------------------------------------------------------
# Precondition: DRAFT -> IN-REVIEW requires a build or ontology
# ----------------------------------------------------------------------


def test_draft_to_review_requires_build_or_ontology():
    with pytest.raises(ValidationError):
        _check(
            STATUS_DRAFT,
            STATUS_IN_REVIEW,
            user_role=ROLE_ADMIN,
            last_build="",
            has_ontology=False,
        )


def test_draft_to_review_allowed_with_ontology_and_no_build():
    _check(
        STATUS_DRAFT,
        STATUS_IN_REVIEW,
        user_role=ROLE_ADMIN,
        last_build="",
        has_ontology=True,
    )


def test_review_to_published_does_not_require_build():
    # Already past the build gate; no last_build needed.
    _check(
        STATUS_IN_REVIEW,
        STATUS_PUBLISHED,
        user_role=ROLE_ADMIN,
        last_build="",
    )


def _check_delete(
    *,
    user_role=ROLE_ADMIN,
    status=STATUS_DRAFT,
    is_loaded=False,
    is_latest=False,
    version_count=3,
):
    return check_version_deletion(
        user_role=user_role,
        status=status,
        is_loaded=is_loaded,
        is_latest=is_latest,
        version_count=version_count,
    )


def test_old_unloaded_draft_is_deletable_by_app_admin():
    _check_delete()


@pytest.mark.parametrize("role", [ROLE_BUILDER, ROLE_EDITOR, ROLE_VIEWER, ROLE_NONE])
def test_version_deletion_requires_app_admin(role):
    with pytest.raises(AuthorizationError, match="administrator"):
        _check_delete(user_role=role)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"is_loaded": True}, "loaded"),
        ({"is_latest": True}, "latest"),
        ({"version_count": 1}, "at least one"),
        ({"status": STATUS_IN_REVIEW}, "Draft"),
        ({"status": STATUS_PUBLISHED}, "Draft"),
    ],
)
def test_protected_version_cannot_be_deleted(overrides, message):
    with pytest.raises(ConflictError, match=message):
        _check_delete(**overrides)


def test_delete_capability_returns_precise_block_reason():
    result = version_deletion_capability(
        user_role=ROLE_ADMIN,
        status=STATUS_PUBLISHED,
        is_loaded=False,
        is_latest=False,
        version_count=3,
    )
    assert result == {
        "delete_control_visible": True,
        "can_delete": False,
        "delete_block_reason": "Reopen this version as Draft before deleting it.",
    }


def test_non_admin_delete_control_is_hidden():
    result = version_deletion_capability(
        user_role=ROLE_BUILDER,
        status=STATUS_DRAFT,
        is_loaded=False,
        is_latest=False,
        version_count=3,
    )
    assert result["delete_control_visible"] is False
    assert result["can_delete"] is False


def test_transition_capabilities_include_enabled_builder_action():
    result = transition_capabilities(
        STATUS_DRAFT,
        user_role="",
        user_domain_role=ROLE_BUILDER,
        last_build="2026-09-25T10:00:00Z",
        has_ontology=False,
    )
    assert result == [
        {
            "target_status": STATUS_IN_REVIEW,
            "label": "Submit for Review",
            "enabled": True,
            "blocked_reason": "",
        }
    ]


def test_transition_capabilities_keep_readiness_failure_disabled():
    result = transition_capabilities(
        STATUS_DRAFT,
        user_role=ROLE_ADMIN,
        user_domain_role=ROLE_NONE,
        last_build="",
        has_ontology=False,
    )
    assert result[0]["enabled"] is False
    assert "ontology" in result[0]["blocked_reason"]


def test_transition_capabilities_omit_unauthorized_actions():
    result = transition_capabilities(
        STATUS_PUBLISHED,
        user_role="",
        user_domain_role=ROLE_BUILDER,
        last_build="",
        has_ontology=True,
    )
    assert result == []
