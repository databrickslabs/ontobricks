"""App ACL, domain permissions, and Teams matrix Settings.

Extracted from :class:`SettingsService` (Fowler Extract Class).
``SettingsService`` keeps one-line delegators for callers.
"""

from __future__ import annotations

import importlib
import time
from typing import Any, Dict, List

from back.core.errors import InfrastructureError, ValidationError
from shared.config.settings import Settings
from back.core.logging import get_logger
from back.objects.session import SessionManager
from back.objects.domain.settings.SettingsService import SettingsService

# Package ``__init__`` binds ``SettingsService`` as the class, which shadows
# the submodule. Tests patch that module's globals (``permission_service``).
_ss = importlib.import_module("back.objects.domain.settings.SettingsService")

logger = get_logger(__name__)


class PermissionSettings:
    """permissions/me, domain ACL CRUD, and the Teams matrix."""

    @staticmethod
    def build_permissions_me(
        email: str,
        display_name: str,
        user_token: str,
        user_role: str,
        user_domain_role: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        if not _ss.is_databricks_app():
            return {
                "email": email or "local-user",
                "display_name": display_name or "Local User",
                "role": "admin",
                "is_app_mode": False,
            }

        role = "none"
        is_app_admin = False
        domain_role = user_domain_role or ""
        domain_folder = ""
        try:
            domain, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )
            domain_folder = getattr(domain, "domain_folder", "") or ""

            _ss.permission_service.clear_admin_cache(email)
            is_app_admin = _ss.permission_service.is_admin(
                email,
                host,
                token,
                settings.ontobricks_app_name,
                user_token=user_token,
            )
            role = _ss.permission_service.get_user_role(
                email,
                host,
                token,
                registry_cfg,
                settings.ontobricks_app_name,
                user_token=user_token,
            )
            # Re-resolve domain role fresh so it matches what the
            # middleware sees on the next request (useful for debugging
            # why a viewer can/can't write).
            domain_role = _ss.permission_service.get_domain_role(
                email,
                host,
                token,
                registry_cfg,
                settings.ontobricks_app_name,
                domain_folder,
                user_token=user_token,
                app_role=role,
            )
        except Exception as e:
            logger.error(
                "permissions/me: error resolving role for %s (middleware app/domain role=%r/%r): %s",
                email,
                user_role,
                user_domain_role,
                e,
                exc_info=True,
            )

        return {
            "email": email,
            "display_name": display_name,
            "role": role,
            "is_app_admin": is_app_admin,
            "is_app_mode": True,
            "domain_folder": domain_folder,
            "domain_role": domain_role,
        }

    @staticmethod
    def build_permissions_diag(
        email: str,
        display_name: str,
        user_token: str,
        user_role: str,
        user_domain_role: str,
        settings: Settings,
    ) -> Dict[str, Any]:
        from databricks.sdk import WorkspaceClient
        import requests as _req

        app_name = settings.ontobricks_app_name
        diag: dict = {
            "email": email,
            "app_name": app_name,
            "is_app_mode": _ss.is_databricks_app(),
            "user_token_present": bool(user_token),
            "display_name": display_name,
            "state_user_role": user_role,
            "state_user_domain_role": user_domain_role,
        }

        # ── SDK path (SP token) ──
        try:
            w = WorkspaceClient()
            diag["sdk_host"] = str(getattr(w.config, "host", ""))
            diag["sdk_auth_type"] = str(getattr(w.config, "auth_type", ""))
            raw = w.api_client.do("GET", f"{_ss.PERMISSIONS_APPS_PATH}/{app_name}")
            diag["sdk_can_manage"] = _ss.permission_service._extract_can_manage(raw)
            diag["sdk_error"] = None
        except Exception as e:
            diag["sdk_error"] = f"{type(e).__name__}: {e}"
            diag["sdk_can_manage"] = []

        # ── User-token path (preferred at runtime) ──
        managers = diag["sdk_can_manage"]
        if user_token:
            try:
                host = diag.get("sdk_host", "").rstrip("/")
                resp = _req.get(
                    f"{host}{_ss.PERMISSIONS_APPS_PATH}/{app_name}",
                    headers={"Authorization": f"Bearer {user_token}", "User-Agent": _ss.HTTP_USER_AGENT},
                    timeout=5,
                )
                resp.raise_for_status()
                managers = _ss.permission_service._extract_can_manage(resp.json())
                diag["user_token_can_manage"] = managers
                diag["user_token_error"] = None
            except Exception as e:
                diag["user_token_error"] = f"{type(e).__name__}: {e}"
                diag["user_token_can_manage"] = []
                managers = diag["sdk_can_manage"]

        # Admin can be granted to the e-mail directly or to any group the
        # caller belongs to, so report both and why.
        wanted = {m.lower() for m in managers if m}
        user_groups = _ss.permission_service._get_user_groups(
            email,
            diag.get("sdk_host", ""),
            "",
            user_token=user_token,
        )
        diag["user_groups"] = user_groups
        diag["email_is_manager"] = email.lower() in wanted
        diag["group_is_manager"] = sorted(
            g for g in user_groups if g.lower() in wanted
        )

        diag["admin_cache"] = {
            k: {"result": v[0], "age_s": round(time.time() - v[1], 1)}
            for k, v in _ss.permission_service._admin_cache.items()
        }

        return diag

    @staticmethod
    def list_app_principals_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Return the Databricks App principals (users + groups).

        Used as the row source for the Settings → Admin → Teams matrix picker.
        """
        _, host, token, _ = SettingsService._resolve_context(session_mgr, settings)
        app_name = settings.ontobricks_app_name
        _ss.permission_service.clear_principals_cache()
        result = _ss.permission_service.list_app_principals(host, token, app_name)
        return {
            "success": True,
            "users": result.get("users", []),
            "groups": result.get("groups", []),
        }

    @staticmethod
    def list_principals_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Alias kept for the Teams picker dropdown."""
        return SettingsService.list_app_principals_result(session_mgr, settings)

    @staticmethod
    def search_workspace_principals(
        query: str,
        principal_type: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Search users or groups that have access to the Databricks App.

        Fetches the full app-permission principal list (cached by
        ``PermissionService``) and applies a case-insensitive *contains*
        filter on the client side.  This avoids SCIM calls that the app
        service-principal typically cannot perform and ensures only
        app-visible principals are returned.
        """
        _, host, token, _ = SettingsService._resolve_context(session_mgr, settings)
        app_name = settings.ontobricks_app_name
        all_principals = _ss.permission_service.list_app_principals(host, token, app_name)

        q = query.lower()

        if principal_type == "group":
            groups = [
                g
                for g in all_principals.get("groups", [])
                if q in (g.get("display_name") or "").lower()
            ]
            return {"success": True, "results": groups}

        users = [
            u
            for u in all_principals.get("users", [])
            if q in (u.get("email") or "").lower()
            or q in (u.get("display_name") or "").lower()
        ]
        return {"success": True, "results": users}

    @staticmethod
    def list_domain_permissions_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        entries = _ss.permission_service.list_domain_entries(
            host, token, registry_cfg, domain_name
        )
        return {"success": True, "domain": domain_name, "permissions": entries}

    @staticmethod
    def add_domain_permission_result(
        domain_name: str,
        data: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        principal = data.get("principal", "").strip()
        principal_type = data.get("principal_type", "user")
        display_name = data.get("display_name", principal)
        role = data.get("role", "viewer")

        if not principal:
            raise ValidationError("Principal (email or group name) is required")
        if role not in _ss.ASSIGNABLE_ROLES:
            raise ValidationError('Role must be "viewer", "editor", or "builder"')
        if not domain_name:
            raise ValidationError("Domain name is required")

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        if not registry_cfg.get("catalog") or not registry_cfg.get("schema"):
            raise ValidationError("Registry not configured")

        ok, msg = _ss.permission_service.add_or_update_domain_entry(
            host,
            token,
            registry_cfg,
            domain_name,
            principal,
            principal_type,
            display_name,
            role,
        )
        if not ok:
            raise InfrastructureError(
                "Failed to add or update domain permission", detail=msg
            )
        return {"success": ok, "message": msg}

    @staticmethod
    def delete_domain_permission_result(
        domain_name: str,
        principal: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        if not registry_cfg.get("catalog") or not registry_cfg.get("schema"):
            raise ValidationError("Registry not configured")

        ok, msg = _ss.permission_service.remove_domain_entry(
            host,
            token,
            registry_cfg,
            domain_name,
            principal,
        )
        if not ok:
            raise InfrastructureError("Failed to remove domain permission", detail=msg)
        return {"success": ok, "message": msg}

    @staticmethod
    def build_teams_matrix_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Return the Teams matrix payload: domains, principals, assignments.

        Payload shape::

            {
              "success": true,
              "domains": ["acme", "beta", ...],
              "principals": [
                {"principal": "alice@acme", "principal_type": "user",
                 "display_name": "Alice"},
                {"principal": "data-eng", "principal_type": "group",
                 "display_name": "data-eng"}
              ],
              "assignments": {
                "acme": {"alice@acme": "editor"},
                "beta": {"data-eng": "viewer"}
              }
            }
        """
        domain_obj, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        app_name = settings.ontobricks_app_name

        # Domains
        domains: List[str] = []
        try:
            svc = _ss.RegistryService.from_context(domain_obj, settings)
            ok, names, _msg = svc.list_domains_cached()
            if ok:
                domains = sorted(names)
        except Exception as exc:
            logger.warning("Teams matrix: failed to list domains: %s", exc)

        # Principals from Databricks App ACL
        _ss.permission_service.clear_principals_cache()
        app_principals = _ss.permission_service.list_app_principals(host, token, app_name)

        principals: List[Dict[str, Any]] = []
        for u in app_principals.get("users", []):
            email = u.get("email") or ""
            if not email:
                continue
            principals.append(
                {
                    "principal": email,
                    "principal_type": "user",
                    "display_name": u.get("display_name") or email,
                }
            )
        for g in app_principals.get("groups", []):
            name = g.get("display_name") or g.get("id") or ""
            if not name:
                continue
            principals.append(
                {
                    "principal": name,
                    "principal_type": "group",
                    "display_name": name,
                }
            )

        # Assignments per domain (key: domain -> {principal: role})
        assignments: Dict[str, Dict[str, str]] = {}
        for domain_name in domains:
            try:
                entries = _ss.permission_service.list_domain_entries(
                    host, token, registry_cfg, domain_name
                )
                row: Dict[str, str] = {}
                for e in entries:
                    principal = e.get("principal", "")
                    role = e.get("role", "")
                    if principal and role:
                        row[principal] = role
                if row:
                    assignments[domain_name] = row
            except Exception as exc:
                logger.warning(
                    "Teams matrix: failed to read team for %s: %s", domain_name, exc
                )

        return {
            "success": True,
            "domains": domains,
            "principals": principals,
            "assignments": assignments,
        }

    @staticmethod
    def save_teams_batch_result(
        data: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Persist a batch of team changes across multiple domains.

        Body shape::

            {
              "changes": [
                {"domain_folder": "acme",
                 "principal": "alice@acme",
                 "principal_type": "user",
                 "display_name": "Alice",
                 "role": "editor"},
                {"domain_folder": "beta",
                 "principal": "bob@acme",
                 "principal_type": "user",
                 "display_name": "Bob",
                 "role": null}           # null = remove
              ]
            }
        """
        changes = data.get("changes") or []
        if not isinstance(changes, list):
            raise ValidationError("Body must include a 'changes' array")

        validated: List[Dict[str, Any]] = []
        for idx, ch in enumerate(changes):
            if not isinstance(ch, dict):
                raise ValidationError(f"Change #{idx} is not an object")
            domain_folder = (ch.get("domain_folder") or "").strip()
            principal = (ch.get("principal") or "").strip()
            principal_type = ch.get("principal_type") or "user"
            display_name = ch.get("display_name") or principal
            role = ch.get("role")

            if not domain_folder:
                raise ValidationError(
                    f"Change #{idx}: 'domain_folder' is required"
                )
            if not principal:
                raise ValidationError(f"Change #{idx}: 'principal' is required")
            if principal_type not in ("user", "group"):
                raise ValidationError(
                    f"Change #{idx}: 'principal_type' must be 'user' or 'group'"
                )
            if role is not None and role not in _ss.ASSIGNABLE_ROLES:
                raise ValidationError(
                    f"Change #{idx}: 'role' must be one of "
                    f"{list(_ss.ASSIGNABLE_ROLES)} or null"
                )

            validated.append(
                {
                    "domain_folder": domain_folder,
                    "principal": principal,
                    "principal_type": principal_type,
                    "display_name": display_name,
                    "role": role,
                }
            )

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        if not registry_cfg.get("catalog") or not registry_cfg.get("schema"):
            raise ValidationError("Registry not configured")

        saved, failed = _ss.permission_service.save_domain_permissions_batch(
            host, token, registry_cfg, validated
        )

        return {
            "success": len(failed) == 0,
            "saved": saved,
            "failed": failed,
            "total_changes": len(validated),
        }
