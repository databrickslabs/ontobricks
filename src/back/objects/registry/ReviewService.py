"""Ontology review / validation workflow.

Layers a business-user-oriented review workflow on top of the existing
domain-version lifecycle (:mod:`version_lifecycle`):

    DRAFT  --submit-->  IN-REVIEW  --publish-->  PUBLISHED
      ^___ request changes ___|

Every decision and lifecycle change is persisted as an append-only row
in the ``domain_review_events`` table (the registry audit log), so the
full history of *who validated what, when* is durable and queryable.

Roles (see plan ``Ontology Validation & Review``):

* **Submit for review** / **Publish** stay builder/admin (the publish
  transition is additionally gated by a quorum of business sign-offs).
* **Sign-off** (approve / request changes) is a business-user audit
  step open to any principal with a domain role (viewer or above).
  ``request_changes`` also reopens the version for editing
  (IN-REVIEW -> DRAFT).

The lifecycle ``status`` on ``domain_versions`` remains the single
source of truth for the current state; this service only adds the
review/audit layer and the quorum gate.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from back.core.errors import (
    AuthorizationError,
    ConflictError,
    InfrastructureError,
    NotFoundError,
    OntoBricksError,
    ValidationError,
)
from back.core.logging import get_logger
from back.objects.registry.RegistryService import RegistryCfg, RegistryService
from back.objects.registry.PermissionService import (
    ASSIGNABLE_ROLES,
    ROLE_ADMIN,
    ROLE_NONE,
    ROLE_VIEWER,
    permission_service,
    role_level,
)
from back.objects.registry.registry_cache import invalidate_registry_cache
from back.objects.registry.version_lifecycle import (
    STATUS_DRAFT,
    STATUS_IN_REVIEW,
    STATUS_PUBLISHED,
)
from back.objects.session import SessionManager, get_domain

logger = get_logger(__name__)

# Workflow event/action tags (mirror the schema CHECK constraint).
ACTION_SUBMITTED = "submitted"
ACTION_APPROVED = "approved"
ACTION_CHANGES_REQUESTED = "changes_requested"
ACTION_PUBLISHED = "published"
ACTION_REOPENED = "reopened"
ACTION_COMMENTED = "commented"

# Sign-off decisions accepted by :meth:`ReviewService.signoff`.
DECISION_APPROVE = "approve"
DECISION_REQUEST_CHANGES = "request_changes"

_DEFAULT_QUORUM = 1


class ReviewService:
    """Stateless orchestrator for the review/validation workflow."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @staticmethod
    def my_tasks(
        request,
        session_mgr: SessionManager,
        settings,
    ) -> Dict[str, Any]:
        """Cross-domain worklist of versions with a pending action for the
        current user.

        Returns ``{success, tasks: [...]}`` where each task is a
        ``(domain, version, status, approvals, required, actions)`` row.
        ``required`` is the per-domain sign-off quorum.
        """
        try:
            domain = get_domain(session_mgr)
            svc = RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                return {"success": True, "tasks": []}

            email = ReviewService._email(request)
            app_role = getattr(request.state, "user_role", "") or ""

            ok, domains, msg = svc.list_domain_details_cached()
            if not ok:
                raise InfrastructureError(
                    "Failed to list registry domains", detail=msg
                )

            events_by_key = ReviewService._group_events(
                svc.list_all_review_events()
            )
            roles = ReviewService._resolve_roles(
                request,
                domain,
                settings,
                [d.get("name", "") for d in domains],
                app_role,
            )

            tasks: List[Dict[str, Any]] = []
            for d in domains:
                folder = d.get("name", "")
                role = roles.get(folder, ROLE_NONE)
                if role_level(role) <= 0:
                    continue
                quorum = max(1, int(d.get("review_quorum") or _DEFAULT_QUORUM))
                for v in d.get("versions", []) or []:
                    version = v.get("version", "")
                    status = (v.get("status") or STATUS_DRAFT).upper()
                    last_build = v.get("last_build", "") or ""
                    summary = ReviewService._summarize(
                        events_by_key.get((folder, version), [])
                    )
                    actions = ReviewService._pending_actions(
                        status, role, email, summary, quorum, last_build
                    )
                    if not actions:
                        continue
                    tasks.append(
                        {
                            "domain": folder,
                            "version": version,
                            "status": status,
                            "approvals": summary["approvals"],
                            "required": quorum,
                            "your_role": role,
                            "actions": actions,
                            "last_activity": summary["last_activity"],
                        }
                    )

            # Newest activity first; versions never reviewed (no activity)
            # sort to the bottom.
            tasks.sort(key=lambda t: t["last_activity"] or "", reverse=True)
            return {
                "success": True,
                "tasks": tasks,
                "assigned_tasks": ReviewService._assigned_tasks(svc, email),
            }
        except OntoBricksError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("ReviewService.my_tasks failed")
            raise InfrastructureError(
                "Failed to build review worklist", detail=str(exc)
            ) from exc

    @staticmethod
    def review_detail(
        request,
        session_mgr: SessionManager,
        settings,
        folder: str,
        version: str,
        *,
        user_role: str,
        user_domain_role: str,
    ) -> Dict[str, Any]:
        """Full review state + audit trail + allowed actions for a version."""
        try:
            svc, info = ReviewService._load(session_mgr, settings, folder, version)
            status = (info.get("status") or STATUS_DRAFT).upper()
            last_build = info.get("last_build", "") or ""
            quorum = ReviewService._quorum(svc, folder)
            events = svc.list_review_events(folder, version)
            summary = ReviewService._summarize(events)
            email = ReviewService._email(request)
            is_member = (
                user_role == ROLE_ADMIN
                or role_level(user_domain_role) >= role_level(ROLE_VIEWER)
            )
            is_builder = ReviewService._is_builder(user_role, user_domain_role)
            is_admin = ReviewService._is_admin(user_role, user_domain_role)
            already = email.lower() in {a.lower() for a in summary["approvers"]}
            quorum_met = summary["approvals"] >= quorum

            return {
                "success": True,
                "domain": folder,
                "version": version,
                "status": status,
                "last_build": last_build,
                "quorum": quorum,
                "approvals": summary["approvals"],
                "approvers": summary["approvers"],
                "quorum_met": quorum_met,
                "publish_override": (
                    status == STATUS_IN_REVIEW and is_admin and not quorum_met
                ),
                "already_approved": already,
                "events": events,
                "actions": {
                    "can_submit": (
                        status == STATUS_DRAFT and is_builder and bool(last_build)
                    ),
                    "submit_blocked_reason": (
                        ""
                        if last_build
                        else "This version has never been built. "
                        "Run a Knowledge Graph build first."
                    ),
                    "can_approve": (
                        status == STATUS_IN_REVIEW and is_member and not already
                    ),
                    "can_request_changes": (
                        status == STATUS_IN_REVIEW and is_member
                    ),
                    "can_publish": (
                        status == STATUS_IN_REVIEW
                        and is_builder
                        and (quorum_met or is_admin)
                    ),
                    "can_reopen": (
                        status in (STATUS_IN_REVIEW, STATUS_PUBLISHED)
                        and is_admin
                    ),
                },
            }
        except OntoBricksError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("ReviewService.review_detail failed")
            raise InfrastructureError(
                "Failed to load review detail", detail=str(exc)
            ) from exc

    @staticmethod
    def review_team(
        request,
        session_mgr: SessionManager,
        settings,
        folder: str,
    ) -> Dict[str, Any]:
        """Domain access list — principals and their role on *folder*.

        Read-only summary surfaced on the Validation page so reviewers can
        see who can view / edit / build the domain.

        Uses the **same source as the Registry → Teams matrix**: rows come
        from the Databricks App principals (``list_app_principals``) and the
        role from the per-domain registry permissions
        (``list_domain_entries``). A principal appears here only if it is a
        known app principal *and* has an assignable role on this domain —
        i.e. exactly the "filled cells" of the domain's column in the Teams
        matrix. This avoids surfacing orphan ``.domain_permissions.json``
        entries (principals no longer in the App ACL) that the Teams page
        does not show. Members are returned most-privileged first.
        """
        _ = request  # identity is not needed; the list is the same for any member
        if not folder:
            raise ValidationError("domain is required")
        try:
            from back.core.helpers import get_databricks_host_and_token

            domain = get_domain(session_mgr)
            host, token = get_databricks_host_and_token(domain, settings)
            registry_cfg = RegistryCfg.from_domain(domain, settings).as_dict()
            app_name = settings.ontobricks_app_name
            app_principals = permission_service.list_app_principals(
                host, token, app_name
            )
            entries = permission_service.list_domain_entries(
                host, token, registry_cfg, folder
            )
        except OntoBricksError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("ReviewService.review_team failed")
            raise InfrastructureError(
                "Failed to load domain access list", detail=str(exc)
            ) from exc

        # Role lookup keyed by the stored principal string (mirrors the
        # Teams matrix cell mapping).
        roles = {
            e.get("principal", ""): e.get("role", "")
            for e in entries
            if e.get("principal")
        }

        members: List[Dict[str, Any]] = []
        for u in app_principals.get("users", []):
            email = u.get("email") or ""
            role = roles.get(email, "")
            if email and role in ASSIGNABLE_ROLES:
                members.append(
                    {
                        "principal": email,
                        "principal_type": "user",
                        "display_name": u.get("display_name") or email,
                        "role": role,
                    }
                )
        for g in app_principals.get("groups", []):
            name = g.get("display_name") or g.get("id") or ""
            role = roles.get(name, "")
            if name and role in ASSIGNABLE_ROLES:
                members.append(
                    {
                        "principal": name,
                        "principal_type": "group",
                        "display_name": name,
                        "role": role,
                    }
                )

        members.sort(
            key=lambda m: (-role_level(m["role"]), m["display_name"].lower())
        )
        return {"success": True, "domain": folder, "members": members}

    @staticmethod
    def submit(
        request,
        session_mgr: SessionManager,
        settings,
        folder: str,
        version: str,
        *,
        comment: str,
        user_role: str,
        user_domain_role: str,
    ) -> Dict[str, Any]:
        """Submit a DRAFT version for review (DRAFT -> IN-REVIEW)."""
        svc, info = ReviewService._load(session_mgr, settings, folder, version)
        status = (info.get("status") or STATUS_DRAFT).upper()
        last_build = info.get("last_build", "") or ""
        if not ReviewService._is_builder(user_role, user_domain_role):
            raise AuthorizationError(
                "Only an administrator or builder can submit for review"
            )
        if status != STATUS_DRAFT:
            raise ConflictError(f"Version is {status}, expected DRAFT")
        if not last_build:
            raise ValidationError(
                "Cannot submit for review: this version has never been "
                "built. Run a Knowledge Graph build first."
            )

        ReviewService._set_status(
            svc, session_mgr, folder, version, STATUS_IN_REVIEW
        )
        svc.record_review_event(
            folder,
            version,
            ReviewService._email(request),
            ACTION_SUBMITTED,
            from_status=STATUS_DRAFT,
            to_status=STATUS_IN_REVIEW,
            comment=comment,
        )
        return ReviewService.review_detail(
            request,
            session_mgr,
            settings,
            folder,
            version,
            user_role=user_role,
            user_domain_role=user_domain_role,
        )

    @staticmethod
    def signoff(
        request,
        session_mgr: SessionManager,
        settings,
        folder: str,
        version: str,
        *,
        decision: str,
        comment: str,
        user_role: str,
        user_domain_role: str,
    ) -> Dict[str, Any]:
        """Record a business-user sign-off on an IN-REVIEW version.

        ``decision`` is ``approve`` or ``request_changes``. Requesting
        changes additionally reopens the version (IN-REVIEW -> DRAFT).
        """
        decision = (decision or "").strip().lower()
        if decision not in (DECISION_APPROVE, DECISION_REQUEST_CHANGES):
            raise ValidationError(
                "decision must be 'approve' or 'request_changes'"
            )
        svc, info = ReviewService._load(session_mgr, settings, folder, version)
        status = (info.get("status") or STATUS_DRAFT).upper()
        is_member = (
            user_role == ROLE_ADMIN
            or role_level(user_domain_role) >= role_level(ROLE_VIEWER)
        )
        if not is_member:
            raise AuthorizationError(
                "You need a role on this domain to review it"
            )
        if status != STATUS_IN_REVIEW:
            raise ConflictError(
                f"Version is {status}, expected IN-REVIEW"
            )

        email = ReviewService._email(request)
        if decision == DECISION_APPROVE:
            events = svc.list_review_events(folder, version)
            summary = ReviewService._summarize(events)
            if email.lower() in {a.lower() for a in summary["approvers"]}:
                raise ConflictError("You have already approved this version")
            svc.record_review_event(
                folder,
                version,
                email,
                ACTION_APPROVED,
                comment=comment,
            )
        else:  # request_changes — reopen for editing
            ReviewService._set_status(
                svc, session_mgr, folder, version, STATUS_DRAFT
            )
            svc.record_review_event(
                folder,
                version,
                email,
                ACTION_CHANGES_REQUESTED,
                from_status=STATUS_IN_REVIEW,
                to_status=STATUS_DRAFT,
                comment=comment,
            )

        return ReviewService.review_detail(
            request,
            session_mgr,
            settings,
            folder,
            version,
            user_role=user_role,
            user_domain_role=user_domain_role,
        )

    @staticmethod
    def publish(
        request,
        session_mgr: SessionManager,
        settings,
        folder: str,
        version: str,
        *,
        comment: str,
        user_role: str,
        user_domain_role: str,
    ) -> Dict[str, Any]:
        """Publish an IN-REVIEW version once the sign-off quorum is met.

        Admins (app-level or domain-level) may publish regardless of the
        quorum; the override is recorded in the audit event meta.
        """
        svc, info = ReviewService._load(session_mgr, settings, folder, version)
        status = (info.get("status") or STATUS_DRAFT).upper()
        if not ReviewService._is_builder(user_role, user_domain_role):
            raise AuthorizationError(
                "Only an administrator or builder can publish"
            )
        if status != STATUS_IN_REVIEW:
            raise ConflictError(f"Version is {status}, expected IN-REVIEW")

        quorum = ReviewService._quorum(svc, folder)
        summary = ReviewService._summarize(svc.list_review_events(folder, version))
        is_admin = ReviewService._is_admin(user_role, user_domain_role)
        quorum_override = is_admin and summary["approvals"] < quorum
        if summary["approvals"] < quorum and not is_admin:
            raise ConflictError(
                f"Cannot publish: {summary['approvals']} of {quorum} "
                f"required sign-offs collected"
            )

        ReviewService._set_status(
            svc, session_mgr, folder, version, STATUS_PUBLISHED
        )
        svc.record_review_event(
            folder,
            version,
            ReviewService._email(request),
            ACTION_PUBLISHED,
            from_status=STATUS_IN_REVIEW,
            to_status=STATUS_PUBLISHED,
            comment=comment,
            meta={
                "approvals": summary["approvals"],
                "quorum": quorum,
                "quorum_override": quorum_override,
            },
        )
        return ReviewService.review_detail(
            request,
            session_mgr,
            settings,
            folder,
            version,
            user_role=user_role,
            user_domain_role=user_domain_role,
        )

    @staticmethod
    def reopen(
        request,
        session_mgr: SessionManager,
        settings,
        folder: str,
        version: str,
        *,
        comment: str,
        user_role: str,
        user_domain_role: str,
    ) -> Dict[str, Any]:
        """Send an IN-REVIEW or PUBLISHED version back to DRAFT (admin only)."""
        svc, info = ReviewService._load(session_mgr, settings, folder, version)
        status = (info.get("status") or STATUS_DRAFT).upper()
        if not ReviewService._is_admin(user_role, user_domain_role):
            raise AuthorizationError("Only an administrator can reopen")
        if status not in (STATUS_IN_REVIEW, STATUS_PUBLISHED):
            raise ConflictError(
                f"Version is {status}, expected IN-REVIEW or PUBLISHED"
            )

        ReviewService._set_status(
            svc, session_mgr, folder, version, STATUS_DRAFT
        )
        svc.record_review_event(
            folder,
            version,
            ReviewService._email(request),
            ACTION_REOPENED,
            from_status=status,
            to_status=STATUS_DRAFT,
            comment=comment,
        )
        return ReviewService.review_detail(
            request,
            session_mgr,
            settings,
            folder,
            version,
            user_role=user_role,
            user_domain_role=user_domain_role,
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _load(
        session_mgr: SessionManager, settings, folder: str, version: str
    ) -> Tuple[RegistryService, Dict[str, Any]]:
        """Resolve the registry service + the version ``info`` blob."""
        if not folder or not version:
            raise ValidationError("domain and version are required")
        domain = get_domain(session_mgr)
        svc = RegistryService.from_context(domain, settings)
        if not svc.cfg.is_configured:
            raise ValidationError("Registry not configured")
        if version not in svc.list_versions_sorted(folder):
            raise NotFoundError(f'Version {version} not found in "{folder}"')
        ok, data, msg = svc.read_version(folder, version)
        if not ok:
            raise InfrastructureError(
                "Failed to read registry version", detail=msg
            )
        return svc, data.get("info", {}) or {}

    @staticmethod
    def _set_status(
        svc: RegistryService,
        session_mgr: SessionManager,
        folder: str,
        version: str,
        new_status: str,
    ) -> None:
        """Persist a lifecycle status change + sync session/caches."""
        ok, msg = svc.set_version_status(folder, version, new_status)
        if not ok:
            raise InfrastructureError(
                "Failed to update version status", detail=msg
            )
        invalidate_registry_cache()
        try:
            from back.objects.domain.version_status import (
                clear_version_status_cache,
            )

            clear_version_status_cache()
        except Exception as exc:  # noqa: BLE001
            logger.debug("clear_version_status_cache failed: %s", exc)

        try:
            domain = get_domain(session_mgr)
            if (
                domain.domain_folder == folder
                and domain.current_version == version
            ):
                domain.info["status"] = new_status
                domain.save()
        except Exception as exc:  # noqa: BLE001
            logger.debug("session status sync skipped: %s", exc)

    @staticmethod
    def _quorum(svc: RegistryService, folder: str) -> int:
        """Per-domain review sign-off quorum (>= 1)."""
        try:
            return max(1, int(svc.store.get_domain_quorum(folder)))
        except Exception:  # noqa: BLE001
            return _DEFAULT_QUORUM

    @staticmethod
    def _email(request) -> str:
        return (
            getattr(request.state, "user_email", "")
            or request.headers.get("x-forwarded-email", "")
            or ""
        )

    @staticmethod
    def _is_builder(user_role: str, user_domain_role: str) -> bool:
        from back.objects.registry.PermissionService import ROLE_BUILDER

        return user_role == ROLE_ADMIN or user_domain_role in (
            ROLE_BUILDER,
            ROLE_ADMIN,
        )

    @staticmethod
    def _is_admin(user_role: str, user_domain_role: str) -> bool:
        """An admin (app-level or domain-level) may drive any lifecycle
        transition, including publishing regardless of the sign-off quorum.
        """
        return user_role == ROLE_ADMIN or user_domain_role == ROLE_ADMIN

    @staticmethod
    def _assigned_tasks(svc, email: str) -> List[Dict[str, Any]]:
        """Open / in-progress collaborative tasks assigned to *email*.

        Best-effort: the worklist must still render review actions even if
        the tasks backend is mid-migration or unavailable.
        """
        if not email:
            return []
        try:
            rows = svc.list_tasks_for_assignee(email)
            return [
                r
                for r in rows
                if (r.get("status") or "").lower() in ("open", "in_progress")
            ]
        except Exception as exc:  # noqa: BLE001
            logger.debug("_assigned_tasks(%s) failed: %s", email, exc)
            return []

    @staticmethod
    def _group_events(
        events: List[Dict[str, Any]],
    ) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
        grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for e in events:
            key = (e.get("folder", ""), e.get("version", ""))
            grouped.setdefault(key, []).append(e)
        return grouped

    @staticmethod
    def _summarize(events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Reduce an oldest-first event list to the current review state.

        Approvals reset on every (re)submission, on a change request and
        on publish — so ``approvers`` only ever counts distinct sign-offs
        for the *current* review round.
        """
        approvers: List[str] = []
        seen = set()
        last_activity = ""
        for e in events:
            action = e.get("action", "")
            ts = e.get("created_at", "") or ""
            if ts:
                last_activity = ts
            if action in (
                ACTION_SUBMITTED,
                ACTION_CHANGES_REQUESTED,
                ACTION_PUBLISHED,
                ACTION_REOPENED,
            ):
                approvers = []
                seen = set()
            elif action == ACTION_APPROVED:
                actor = e.get("actor", "") or ""
                key = actor.lower()
                if key and key not in seen:
                    seen.add(key)
                    approvers.append(actor)
        return {
            "approvers": approvers,
            "approvals": len(approvers),
            "last_activity": last_activity,
        }

    @staticmethod
    def _pending_actions(
        status: str,
        role: str,
        email: str,
        summary: Dict[str, Any],
        quorum: int,
        last_build: str,
    ) -> List[Dict[str, str]]:
        """Actionable items for the My Tasks worklist (only pending ones)."""
        actions: List[Dict[str, str]] = []
        is_builder = role in ("builder", ROLE_ADMIN)
        is_admin = role == ROLE_ADMIN
        is_member = role_level(role) >= role_level(ROLE_VIEWER)
        already = email.lower() in {a.lower() for a in summary["approvers"]}

        if status == STATUS_DRAFT:
            if is_builder and last_build:
                actions.append({"id": "submit", "label": "Submit for review"})
        elif status == STATUS_IN_REVIEW:
            if is_member and not already:
                actions.append({"id": "review", "label": "Review & sign off"})
            # Builders publish once quorum is met; admins may override the
            # quorum and publish at any time.
            if is_builder and (summary["approvals"] >= quorum or is_admin):
                actions.append({"id": "publish", "label": "Publish"})
        return actions

    @staticmethod
    def _resolve_roles(
        request,
        domain,
        settings,
        folders: List[str],
        app_role: str,
    ) -> Dict[str, str]:
        """Resolve the caller's role on each folder (admin short-circuit).

        Mirrors ``_permissions.filter_visible_domains``: admins (and the
        local-dev case where the app role is unresolved) get full access
        to every domain; otherwise the per-domain role is read from the
        registry permissions.
        """
        if not app_role or app_role == ROLE_ADMIN:
            return {f: ROLE_ADMIN for f in folders}

        email = ReviewService._email(request)
        if not email:
            return {f: ROLE_NONE for f in folders}

        try:
            from back.core.helpers import get_databricks_host_and_token

            host, token = get_databricks_host_and_token(domain, settings)
            user_token = request.headers.get("x-forwarded-access-token", "") or ""
            registry_cfg = RegistryCfg.from_domain(domain, settings).as_dict()
        except Exception as exc:  # noqa: BLE001
            logger.debug("_resolve_roles setup failed: %s", exc)
            return {f: ROLE_NONE for f in folders}

        roles: Dict[str, str] = {}
        for folder in folders:
            try:
                roles[folder] = permission_service.get_domain_role(
                    email,
                    host,
                    token,
                    registry_cfg,
                    settings.ontobricks_app_name,
                    folder,
                    user_token=user_token,
                    app_role=app_role,
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug("get_domain_role(%s) failed: %s", folder, exc)
                roles[folder] = ROLE_NONE
        return roles
