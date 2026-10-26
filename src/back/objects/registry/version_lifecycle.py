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

from typing import Any

from back.core.errors import AuthorizationError, ConflictError, ValidationError
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

TRANSITION_LABELS = {
    STATUS_IN_REVIEW: "Submit for Review",
    STATUS_PUBLISHED: "Publish",
    STATUS_DRAFT: "Return to Draft",
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
    has_ontology: bool = False,
) -> None:
    """Validate a lifecycle transition.

    Raises :class:`ValidationError` for an unknown / illegal transition or
    an unmet precondition, and :class:`AuthorizationError` when the caller
    lacks the required role. Returns ``None`` when the transition is
    allowed.

    The ``DRAFT -> IN-REVIEW`` precondition accepts *either* a Knowledge
    Graph build (``last_build``) *or* a valid ontology (``has_ontology``):
    an ontology-only domain (no mapping, no graph) is publishable, but a
    truly empty version — neither build nor ontology — is not.
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

    if (
        (current, new) == (STATUS_DRAFT, STATUS_IN_REVIEW)
        and not last_build
        and not has_ontology
    ):
        raise ValidationError(
            "Cannot submit for review: this version has neither a Knowledge "
            "Graph build nor a valid ontology. Define an ontology (or run a "
            "Knowledge Graph build) first."
        )


def transition_capabilities(
    current: str,
    *,
    user_role: str,
    user_domain_role: str,
    last_build: str,
    has_ontology: bool,
) -> list[dict[str, Any]]:
    current = (current or STATUS_DRAFT).upper()
    capabilities: list[dict[str, Any]] = []
    for (source, target), _tier in ALLOWED_TRANSITIONS.items():
        if source != current:
            continue
        label = (
            "Reopen as Draft"
            if source == STATUS_PUBLISHED and target == STATUS_DRAFT
            else TRANSITION_LABELS[target]
        )
        try:
            check_status_transition(
                source,
                target,
                user_role=user_role,
                user_domain_role=user_domain_role,
                last_build=last_build,
                has_ontology=has_ontology,
            )
        except AuthorizationError:
            continue
        except ValidationError as exc:
            capabilities.append(
                {
                    "target_status": target,
                    "label": label,
                    "enabled": False,
                    "blocked_reason": str(exc),
                }
            )
        else:
            capabilities.append(
                {
                    "target_status": target,
                    "label": label,
                    "enabled": True,
                    "blocked_reason": "",
                }
            )
    return capabilities


def check_version_deletion(
    *,
    user_role: str,
    status: str,
    is_loaded: bool,
    version_count: int,
    is_latest: bool = False,
) -> None:
    if user_role != ROLE_ADMIN:
        raise AuthorizationError("Only an application administrator can delete versions")
    _ = is_latest
    if (status or STATUS_DRAFT).upper() != STATUS_DRAFT:
        raise ConflictError("Reopen this version as Draft before deleting it")
    # The last remaining version may be deleted (it removes the domain).
    # A loaded version may be deleted only when it is that last version.
    if is_loaded and version_count > 1:
        raise ConflictError(
            "Load another version before deleting this loaded version"
        )


def version_deletion_capability(
    *,
    user_role: str,
    status: str,
    is_loaded: bool,
    version_count: int,
    is_latest: bool = False,
) -> dict[str, Any]:
    visible = user_role == ROLE_ADMIN
    deletes_domain = version_count <= 1
    try:
        check_version_deletion(
            user_role=user_role,
            status=status,
            is_loaded=is_loaded,
            version_count=version_count,
            is_latest=is_latest,
        )
    except (AuthorizationError, ConflictError) as exc:
        reason = str(exc)
        if "Load another" in reason:
            reason = "Load another version before deleting this one."
        elif "Draft" in reason:
            reason = "Reopen this version as Draft before deleting it."
        return {
            "delete_control_visible": visible,
            "can_delete": False,
            "delete_block_reason": reason,
            "deletes_domain": deletes_domain,
        }
    return {
        "delete_control_visible": True,
        "can_delete": True,
        "delete_block_reason": "",
        "deletes_domain": deletes_domain,
    }
