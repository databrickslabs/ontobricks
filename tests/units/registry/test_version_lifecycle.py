"""Unit tests for the domain-version lifecycle state machine.

Covers the transition rules, per-transition role tiers, and the
DRAFT->IN-REVIEW build precondition defined in
``back.objects.registry.version_lifecycle``.
"""

import pytest

from back.core.errors import AuthorizationError, ValidationError
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
    is_editable,
)


def _check(current, new, *, user_role="", user_domain_role="", last_build="2026-01-01"):
    return check_status_transition(
        current,
        new,
        user_role=user_role,
        user_domain_role=user_domain_role,
        last_build=last_build,
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
# Precondition: DRAFT -> IN-REVIEW requires a build
# ----------------------------------------------------------------------


def test_draft_to_review_requires_build():
    with pytest.raises(ValidationError):
        _check(
            STATUS_DRAFT,
            STATUS_IN_REVIEW,
            user_role=ROLE_ADMIN,
            last_build="",
        )


def test_review_to_published_does_not_require_build():
    # Already past the build gate; no last_build needed.
    _check(
        STATUS_IN_REVIEW,
        STATUS_PUBLISHED,
        user_role=ROLE_ADMIN,
        last_build="",
    )
