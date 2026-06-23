"""Collaborative comments & tasks workflow.

Adds contextual, threaded discussion on top of a domain version and lets
a comment become a personalised task assigned to a teammate. Comments are
*anchored* to a canonical reference so the same thread component opens
from any surface:

* ``ontology_class`` / ``ontology_property`` — the class/property URI
* ``mapping``                                — the mapping ontology URI
* ``graph_node`` / ``graph_edge``           — the node subject URI /
                                              ``source|predicate|target``
* ``domain``                                — the whole (domain, version)

Lifecycle coupling (mirrors the edit lock): comments and tasks are
*written* only while a version is DRAFT or IN-REVIEW — PUBLISHED versions
are read-only here too. Reads are open to any domain member.

Authorization reuses the per-domain role resolved by the router (the same
``_roles()`` pattern as :mod:`api.routers.internal.review`):

* **Read** (list comments / tasks) — any member (viewer or above).
* **Comment / create task** — any member (a reviewer can delegate work).
* **Resolve a comment** — its author, an editor (or above), or an admin.
* **Update a task** — its assignee, its creator, an editor (or above),
  or an admin.

Every task create / completion also appends a ``commented`` row to the
``domain_review_events`` audit log (with ``meta`` linking the task and
comment) so the Validation timeline stays unified.
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
    ROLE_EDITOR,
    ROLE_VIEWER,
    permission_service,
    role_level,
)
from back.objects.registry.version_lifecycle import (
    STATUS_DRAFT,
    STATUS_IN_REVIEW,
)
from back.objects.session import SessionManager, get_domain

logger = get_logger(__name__)

# Task status values (mirror the schema CHECK constraint).
TASK_STATUSES = frozenset({"open", "in_progress", "done", "cancelled"})

# Versions whose discussion is still open for writing. PUBLISHED is
# read-only, matching the ontology edit lock.
_WRITABLE_STATUSES = frozenset({STATUS_DRAFT, STATUS_IN_REVIEW})

_MAX_BODY = 8000
_MAX_TITLE = 300


class CommentService:
    """Stateless orchestrator for collaborative comments + tasks."""

    # ------------------------------------------------------------------
    # Comments
    # ------------------------------------------------------------------

    @staticmethod
    def list_comments(
        request,
        session_mgr: SessionManager,
        settings,
        folder: str,
        version: str,
        *,
        user_role: str,
        user_domain_role: str,
    ) -> Dict[str, Any]:
        """All comments for the domain-wide ``(folder, version)`` thread."""
        try:
            svc, _ = CommentService._load(session_mgr, settings, folder, version)
            CommentService._require_member(user_role, user_domain_role)
            comments = svc.list_comments(folder, version)
            return {"success": True, "comments": comments}
        except OntoBricksError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("CommentService.list_comments failed")
            raise InfrastructureError(
                "Failed to load comments", detail=str(exc)
            ) from exc

    @staticmethod
    def add_comment(
        request,
        session_mgr: SessionManager,
        settings,
        folder: str,
        version: str,
        *,
        body: str,
        parent_id: Optional[str],
        user_role: str,
        user_domain_role: str,
    ) -> Dict[str, Any]:
        """Add a comment (or a reply) to the domain discussion."""
        body = (body or "").strip()
        if not body:
            raise ValidationError("comment body is required")
        if len(body) > _MAX_BODY:
            raise ValidationError(
                f"comment is too long (max {_MAX_BODY} characters)"
            )

        svc, status = CommentService._load(session_mgr, settings, folder, version)
        CommentService._require_member(user_role, user_domain_role)
        CommentService._require_writable(status)

        created = svc.insert_comment(
            folder,
            version,
            author=CommentService._email(request),
            body=body,
            parent_id=(parent_id or None),
        )
        if not created:
            raise InfrastructureError("Failed to save comment")
        return {"success": True, "comment": created}

    @staticmethod
    def resolve_comment(
        request,
        session_mgr: SessionManager,
        settings,
        folder: str,
        version: str,
        comment_id: str,
        *,
        resolved: bool,
        user_role: str,
        user_domain_role: str,
    ) -> Dict[str, Any]:
        """Mark a comment thread resolved (or reopen it)."""
        if not comment_id:
            raise ValidationError("comment_id is required")
        svc, status = CommentService._load(session_mgr, settings, folder, version)
        CommentService._require_member(user_role, user_domain_role)
        CommentService._require_writable(status)

        # Author may always resolve their own thread; otherwise editor+.
        email = CommentService._email(request)
        is_author = CommentService._is_comment_author(
            svc, folder, version, comment_id, email
        )
        if not is_author and not CommentService._is_editor(
            user_role, user_domain_role
        ):
            raise AuthorizationError(
                "Only the comment author, an editor or an admin can "
                "resolve a comment"
            )

        ok, msg = svc.resolve_comment(folder, comment_id, resolved=resolved)
        if not ok:
            raise NotFoundError(msg or "Comment not found")
        return {"success": True, "resolved": resolved}

    # ------------------------------------------------------------------
    # Tasks
    # ------------------------------------------------------------------

    @staticmethod
    def list_tasks(
        request,
        session_mgr: SessionManager,
        settings,
        folder: str,
        version: str,
        *,
        user_role: str,
        user_domain_role: str,
    ) -> Dict[str, Any]:
        """All tasks for ``(folder, version)``."""
        try:
            svc, _ = CommentService._load(session_mgr, settings, folder, version)
            CommentService._require_member(user_role, user_domain_role)
            return {"success": True, "tasks": svc.list_tasks(folder, version)}
        except OntoBricksError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("CommentService.list_tasks failed")
            raise InfrastructureError(
                "Failed to load tasks", detail=str(exc)
            ) from exc

    @staticmethod
    def list_assignees(
        request,
        session_mgr: SessionManager,
        settings,
        folder: str,
        *,
        user_role: str,
        user_domain_role: str,
    ) -> Dict[str, Any]:
        """People with an assignable role on *folder* (task assignee picker).

        Sourced directly from the domain permission entries — i.e. everyone
        granted a role on the domain (viewer/editor/builder) — so it lists
        who can actually be assigned work, independent of the Databricks App
        ACL intersection used by the Validation "Team" view. Any member may
        read it. Returns most-privileged first.
        """
        del request  # identity not needed; same list for any member
        CommentService._require_member(user_role, user_domain_role)
        if not folder:
            raise ValidationError("domain is required")
        try:
            from back.core.helpers import get_databricks_host_and_token

            domain = get_domain(session_mgr)
            host, token = get_databricks_host_and_token(domain, settings)
            registry_cfg = RegistryCfg.from_domain(domain, settings).as_dict()
            entries = permission_service.list_domain_entries(
                host, token, registry_cfg, folder
            )
        except OntoBricksError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("CommentService.list_assignees failed: %s", exc)
            return {"success": True, "domain": folder, "members": []}

        members: List[Dict[str, Any]] = []
        for entry in entries:
            principal = entry.get("principal") or ""
            role = entry.get("role") or ""
            if principal and role in ASSIGNABLE_ROLES:
                members.append(
                    {
                        "principal": principal,
                        "principal_type": entry.get("principal_type") or "user",
                        "display_name": entry.get("display_name") or principal,
                        "role": role,
                    }
                )
        members.sort(
            key=lambda m: (-role_level(m["role"]), m["display_name"].lower())
        )
        return {"success": True, "domain": folder, "members": members}

    @staticmethod
    def create_task(
        request,
        session_mgr: SessionManager,
        settings,
        folder: str,
        version: str,
        *,
        assignee: str,
        title: str,
        description: str,
        due_date: Optional[str],
        comment_id: Optional[str],
        user_role: str,
        user_domain_role: str,
    ) -> Dict[str, Any]:
        """Create a task (optionally born from a comment)."""
        assignee = (assignee or "").strip()
        title = (title or "").strip()
        if not assignee:
            raise ValidationError("assignee is required")
        if not title:
            raise ValidationError("title is required")
        if len(title) > _MAX_TITLE:
            raise ValidationError(
                f"title is too long (max {_MAX_TITLE} characters)"
            )

        svc, status = CommentService._load(session_mgr, settings, folder, version)
        CommentService._require_member(user_role, user_domain_role)
        CommentService._require_writable(status)

        effective_comment_id = comment_id or None
        created = svc.insert_task(
            folder,
            version,
            assignee=assignee,
            created_by=CommentService._email(request),
            title=title,
            description=(description or "").strip(),
            due_date=(due_date or None),
            comment_id=effective_comment_id,
        )
        if not created:
            raise InfrastructureError("Failed to create task")

        CommentService._audit(
            svc,
            folder,
            version,
            CommentService._email(request),
            comment=f"Task assigned to {assignee}: {title}",
            meta={
                "task_id": created.get("id", ""),
                "comment_id": effective_comment_id or "",
                "event": "task_created",
            },
        )

        return {"success": True, "task": created}

    @staticmethod
    def update_task_status(
        request,
        session_mgr: SessionManager,
        settings,
        folder: str,
        version: str,
        task_id: str,
        *,
        status: str,
        user_role: str,
        user_domain_role: str,
    ) -> Dict[str, Any]:
        """Advance a task's status (open/in_progress/done/cancelled)."""
        if not task_id:
            raise ValidationError("task_id is required")
        status = (status or "").strip().lower()
        if status not in TASK_STATUSES:
            raise ValidationError(
                "status must be one of: " + ", ".join(sorted(TASK_STATUSES))
            )

        svc, lifecycle = CommentService._load(
            session_mgr, settings, folder, version
        )
        CommentService._require_member(user_role, user_domain_role)
        CommentService._require_writable(lifecycle)

        email = CommentService._email(request)
        task = CommentService._find_task(svc, folder, version, task_id)
        if task is None:
            raise NotFoundError("Task not found")
        is_owner = email.lower() in {
            (task.get("assignee") or "").lower(),
            (task.get("created_by") or "").lower(),
        }
        if not is_owner and not CommentService._is_editor(
            user_role, user_domain_role
        ):
            raise AuthorizationError(
                "Only the assignee, the creator, an editor or an admin "
                "can update this task"
            )

        ok, msg = svc.update_task_status(folder, task_id, status)
        if not ok:
            raise NotFoundError(msg or "Task not found")

        if status == "done":
            CommentService._audit(
                svc,
                folder,
                version,
                email,
                comment=f"Task completed: {task.get('title', '')}",
                meta={
                    "task_id": task_id,
                    "comment_id": task.get("comment_id", ""),
                    "event": "task_done",
                },
            )
        return {"success": True, "status": status}

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _load(
        session_mgr: SessionManager, settings, folder: str, version: str
    ) -> Tuple[RegistryService, str]:
        """Resolve the registry service + the version lifecycle status."""
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
        info = data.get("info", {}) or {}
        return svc, (info.get("status") or STATUS_DRAFT).upper()

    @staticmethod
    def _audit(
        svc: RegistryService,
        folder: str,
        version: str,
        actor: str,
        *,
        comment: str,
        meta: Dict[str, Any],
    ) -> None:
        """Append a best-effort ``commented`` row to the review audit log."""
        try:
            svc.record_review_event(
                folder, version, actor, "commented", comment=comment, meta=meta
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("collab audit append skipped: %s", exc)

    @staticmethod
    def _is_comment_author(
        svc: RegistryService,
        folder: str,
        version: str,
        comment_id: str,
        email: str,
    ) -> bool:
        try:
            for c in svc.list_comments(folder, version):
                if str(c.get("id")) == str(comment_id):
                    return (c.get("author") or "").lower() == email.lower()
        except Exception:  # noqa: BLE001
            return False
        return False

    @staticmethod
    def _find_task(
        svc: RegistryService, folder: str, version: str, task_id: str
    ) -> Optional[Dict[str, Any]]:
        try:
            for t in svc.list_tasks(folder, version):
                if str(t.get("id")) == str(task_id):
                    return t
        except Exception:  # noqa: BLE001
            return None
        return None

    @staticmethod
    def _require_member(user_role: str, user_domain_role: str) -> None:
        is_member = user_role == ROLE_ADMIN or role_level(
            user_domain_role
        ) >= role_level(ROLE_VIEWER)
        if not is_member:
            raise AuthorizationError(
                "You need a role on this domain to view or discuss it"
            )

    @staticmethod
    def _require_writable(status: str) -> None:
        if (status or "").upper() not in _WRITABLE_STATUSES:
            raise ConflictError(
                "Comments and tasks are read-only once a version is "
                "published; reopen it to DRAFT to continue the discussion"
            )

    @staticmethod
    def _is_editor(user_role: str, user_domain_role: str) -> bool:
        return user_role == ROLE_ADMIN or role_level(
            user_domain_role
        ) >= role_level(ROLE_EDITOR)

    @staticmethod
    def _email(request) -> str:
        return (
            getattr(request.state, "user_email", "")
            or request.headers.get("x-forwarded-email", "")
            or ""
        )
