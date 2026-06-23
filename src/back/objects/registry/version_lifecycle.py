"""Domain-version lifecycle state machine.

A domain version moves through three lifecycle states::

    DRAFT  ->  IN-REVIEW  ->  PUBLISHED
      ^___________|  |____________|
                          (admin only)

Rules (see plan ``domain_status_lifecycle``):

* ``DRAFT -> IN-REVIEW``    — Admin or Builder. Precondition: the version
  has been built at least once (``last_build`` is set). Locks editing.
* ``IN-REVIEW -> DRAFT``    — Admin or Builder. Re-enables editing.
* ``IN-REVIEW -> PUBLISHED``— Admin or Builder.
* ``PUBLISHED -> DRAFT``    — Admin only (reversible publish).
* No direct ``DRAFT -> PUBLISHED`` (must pass through IN-REVIEW).
* New versions are always created as ``DRAFT``.

This module is the single source of truth for the transition rules so the
HTTP endpoint, the service layer and the tests all agree.
"""

from __future__ import annotations

from back.core.errors import AuthorizationError, ValidationError
from back.objects.registry.PermissionService import ROLE_ADMIN, ROLE_BUILDER

STATUS_DRAFT = "DRAFT"
STATUS_IN_REVIEW = "IN-REVIEW"
STATUS_PUBLISHED = "PUBLISHED"

VALID_STATUSES = (STATUS_DRAFT, STATUS_IN_REVIEW, STATUS_PUBLISHED)

# (from, to) -> required-role tier. ``"builder"`` means Admin or Builder;
# ``"admin"`` means Admin only.
ALLOWED_TRANSITIONS = {
    (STATUS_DRAFT, STATUS_IN_REVIEW): "builder",
    (STATUS_IN_REVIEW, STATUS_DRAFT): "builder",
    (STATUS_IN_REVIEW, STATUS_PUBLISHED): "builder",
    (STATUS_PUBLISHED, STATUS_DRAFT): "admin",
}


def is_editable(status: str) -> bool:
    """True when *status* permits editing the version's content."""
    return (status or STATUS_DRAFT).upper() == STATUS_DRAFT


def _has_builder(user_role: str, user_domain_role: str) -> bool:
    return user_role == ROLE_ADMIN or user_domain_role in (
        ROLE_BUILDER,
        ROLE_ADMIN,
    )


def _has_admin(user_role: str, user_domain_role: str) -> bool:
    return user_role == ROLE_ADMIN


def check_status_transition(
    current: str,
    new: str,
    *,
    user_role: str,
    user_domain_role: str,
    last_build: str,
) -> None:
    """Validate a lifecycle transition.

    Raises :class:`ValidationError` for an unknown / illegal transition or
    an unmet precondition, and :class:`AuthorizationError` when the caller
    lacks the required role. Returns ``None`` when the transition is
    allowed.
    """
    current = (current or STATUS_DRAFT).upper()
    new = (new or "").upper()

    if new not in VALID_STATUSES:
        raise ValidationError(
            f"Invalid status '{new}'. Expected one of: "
            f"{', '.join(VALID_STATUSES)}"
        )
    if new == current:
        raise ValidationError(f"Version is already {current}")

    tier = ALLOWED_TRANSITIONS.get((current, new))
    if tier is None:
        raise ValidationError(
            f"Illegal transition {current} -> {new}"
        )

    if tier == "admin":
        if not _has_admin(user_role, user_domain_role):
            raise AuthorizationError(
                f"Only an administrator can change status {current} -> {new}"
            )
    else:  # builder tier
        if not _has_builder(user_role, user_domain_role):
            raise AuthorizationError(
                "Only an administrator or builder can change status "
                f"{current} -> {new}"
            )

    if (current, new) == (STATUS_DRAFT, STATUS_IN_REVIEW) and not last_build:
        raise ValidationError(
            "Cannot submit for review: this version has never been built. "
            "Run a Knowledge Graph build first."
        )
