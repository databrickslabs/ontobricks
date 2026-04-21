"""Databricks Workspace service (SCIM users/groups, app permissions).

Wraps the SCIM v2 and Permissions REST APIs so that workspace-level
operations are isolated from SQL / UC / file concerns.
"""

from typing import Any, Dict, List

from back.core.logging import get_logger
from .DatabricksAuth import DatabricksAuth
from .constants import (
    SCIM_ME_PATH,
    SCIM_USERS_PATH,
    SCIM_GROUPS_PATH,
    PERMISSIONS_APPS_PATH,
)

logger = get_logger(__name__)


class WorkspaceService:
    """Workspace-level operations: users, groups, and app permissions."""

    def __init__(self, auth: DatabricksAuth) -> None:
        self._auth = auth
        self._last_app_permissions_status: int = 0

    def get_current_user_email(self) -> str:
        """Return the authenticated user's e-mail via SCIM ``/Me``."""
        import requests as req

        if not self._auth.host or not self._auth.has_valid_auth():
            return ""

        host = self._auth.host.rstrip("/")
        headers = self._auth.get_auth_headers()
        try:
            resp = req.get(f"{host}{SCIM_ME_PATH}", headers=headers)
            resp.raise_for_status()
            email = resp.json().get("userName", "")
            logger.debug("Current user email: %s", email)
            return email
        except Exception as exc:
            logger.warning("Could not get current user email: %s", exc)
            return ""

    def list_users(self, max_results: int = 500) -> List[Dict[str, Any]]:
        """List workspace users via SCIM.

        Returns dicts with ``email``, ``display_name``, ``active`` keys.
        """
        import requests as req

        if not self._auth.host or not self._auth.has_valid_auth():
            return []

        host = self._auth.host.rstrip("/")
        headers = self._auth.get_auth_headers()
        users: List[Dict[str, Any]] = []
        start_index = 1

        try:
            while True:
                resp = req.get(
                    f"{host}{SCIM_USERS_PATH}",
                    headers=headers,
                    params={"startIndex": start_index, "count": 100},
                )
                resp.raise_for_status()
                data = resp.json()
                for u in data.get("Resources", []):
                    email = u.get("userName", "")
                    if email:
                        users.append(
                            {
                                "email": email,
                                "display_name": u.get("displayName", email),
                                "active": u.get("active", True),
                            }
                        )
                total = data.get("totalResults", 0)
                start_index += data.get("itemsPerPage", 100)
                if start_index > total or start_index > max_results:
                    break
            logger.info("Listed %d workspace users via SCIM", len(users))
            return users
        except Exception as exc:
            logger.warning("Error listing workspace users: %s", exc)
            return []

    def list_groups(self) -> List[Dict[str, Any]]:
        """List workspace groups via SCIM.

        Returns dicts with ``display_name`` and ``id`` keys.
        """
        import requests as req

        if not self._auth.host or not self._auth.has_valid_auth():
            return []

        host = self._auth.host.rstrip("/")
        headers = self._auth.get_auth_headers()
        try:
            resp = req.get(
                f"{host}{SCIM_GROUPS_PATH}",
                headers=headers,
                params={"count": 500},
            )
            resp.raise_for_status()
            groups = []
            for g in resp.json().get("Resources", []):
                name = g.get("displayName", "")
                if name:
                    groups.append({"display_name": name, "id": g.get("id", "")})
            logger.info("Listed %d workspace groups via SCIM", len(groups))
            return groups
        except Exception as exc:
            logger.warning("Error listing workspace groups: %s", exc)
            return []

    def search_users(self, query: str, max_results: int = 500) -> List[Dict[str, Any]]:
        """Search workspace users via SCIM ``filter``.

        Uses a server-side ``co`` (contains) filter on ``userName`` and
        ``displayName``.  Paginates through all matching SCIM pages so
        every workspace user matching *query* is returned.
        """
        import requests as req

        if not self._auth.host or not self._auth.has_valid_auth() or not query:
            return []

        host = self._auth.host.rstrip("/")
        headers = self._auth.get_auth_headers()
        scim_filter = f'userName co "{query}" or displayName co "{query}"'
        users: List[Dict[str, Any]] = []
        start_index = 1
        page_size = 100

        try:
            while True:
                resp = req.get(
                    f"{host}{SCIM_USERS_PATH}",
                    headers=headers,
                    params={
                        "filter": scim_filter,
                        "startIndex": start_index,
                        "count": page_size,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                for u in data.get("Resources", []):
                    email = u.get("userName", "")
                    if email:
                        users.append(
                            {
                                "email": email,
                                "display_name": u.get("displayName", email),
                                "active": u.get("active", True),
                            }
                        )
                total = data.get("totalResults", 0)
                start_index += data.get("itemsPerPage", page_size)
                if start_index > total or len(users) >= max_results:
                    break
            logger.info("SCIM user search '%s': %d results", query, len(users))
            return users
        except Exception as exc:
            logger.warning("Error searching workspace users: %s", exc)
            return []

    def search_groups(self, query: str, max_results: int = 500) -> List[Dict[str, Any]]:
        """Search workspace groups via SCIM ``filter``.

        Paginates through all matching SCIM pages so every workspace
        group matching *query* is returned.
        """
        import requests as req

        if not self._auth.host or not self._auth.has_valid_auth() or not query:
            return []

        host = self._auth.host.rstrip("/")
        headers = self._auth.get_auth_headers()
        scim_filter = f'displayName co "{query}"'
        groups: List[Dict[str, Any]] = []
        start_index = 1
        page_size = 100

        try:
            while True:
                resp = req.get(
                    f"{host}{SCIM_GROUPS_PATH}",
                    headers=headers,
                    params={
                        "filter": scim_filter,
                        "startIndex": start_index,
                        "count": page_size,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                for g in data.get("Resources", []):
                    name = g.get("displayName", "")
                    if name:
                        groups.append({"display_name": name, "id": g.get("id", "")})
                total = data.get("totalResults", 0)
                start_index += data.get("itemsPerPage", page_size)
                if start_index > total or len(groups) >= max_results:
                    break
            logger.info("SCIM group search '%s': %d results", query, len(groups))
            return groups
        except Exception as exc:
            logger.warning("Error searching workspace groups: %s", exc)
            return []

    def get_app_permissions(self, app_name: str) -> List[Dict[str, Any]]:
        """Return permission ACLs for a Databricks App."""
        import requests as req

        if not self._auth.host or not self._auth.has_valid_auth() or not app_name:
            return []

        host = self._auth.host.rstrip("/")
        headers = self._auth.get_auth_headers()
        try:
            resp = req.get(
                f"{host}{PERMISSIONS_APPS_PATH}/{app_name}",
                headers=headers,
                timeout=5,
            )
            resp.raise_for_status()
            results = []
            for acl in resp.json().get("access_control_list", []):
                principal = (
                    acl.get("user_name", "")
                    or acl.get("group_name", "")
                    or acl.get("service_principal_name", "")
                )
                for perm in acl.get("all_permissions", []):
                    results.append(
                        {
                            "principal": principal,
                            "permission_level": perm.get("permission_level", ""),
                            "inherited": perm.get("inherited", False),
                        }
                    )
            logger.debug("App '%s' has %d permission entries", app_name, len(results))
            return results
        except Exception as exc:
            logger.warning("Error getting app permissions for '%s': %s", app_name, exc)
            return []

    @property
    def last_app_permissions_status(self) -> int:
        """HTTP status of the most recent ``list_app_principals`` call.

        ``0`` means "never called or unknown error", ``200`` means success,
        ``403`` indicates the caller lacks ``CAN_VIEW_PERMISSIONS`` on the
        app (typical first-deploy bootstrap failure).
        """
        return self._last_app_permissions_status

    def list_app_principals(self, app_name: str) -> Dict[str, List[Dict[str, Any]]]:
        """Return ``{'users': [...], 'groups': [...]}`` for a Databricks App.

        Service principals are excluded.  On HTTP failure the call is
        swallowed and an empty result is returned; inspect
        :attr:`last_app_permissions_status` to distinguish ``403``
        (bootstrap / permission) from other failure modes.
        """
        import requests as req

        self._last_app_permissions_status = 0

        if not self._auth.host or not self._auth.has_valid_auth() or not app_name:
            return {"users": [], "groups": []}

        host = self._auth.host.rstrip("/")
        headers = self._auth.get_auth_headers()
        try:
            resp = req.get(
                f"{host}{PERMISSIONS_APPS_PATH}/{app_name}",
                headers=headers,
                timeout=5,
            )
            self._last_app_permissions_status = resp.status_code
            resp.raise_for_status()

            users: List[Dict[str, Any]] = []
            groups: List[Dict[str, Any]] = []

            for acl in resp.json().get("access_control_list", []):
                levels = [
                    p.get("permission_level", "")
                    for p in acl.get("all_permissions", [])
                    if not p.get("inherited", False)
                ]
                level = levels[0] if levels else ""

                if acl.get("user_name"):
                    email = acl["user_name"]
                    users.append(
                        {
                            "email": email,
                            "display_name": email,
                            "active": True,
                            "permission_level": level,
                        }
                    )
                elif acl.get("group_name"):
                    groups.append(
                        {
                            "display_name": acl["group_name"],
                            "id": acl.get("group_name", ""),
                            "permission_level": level,
                        }
                    )

            logger.info(
                "App '%s' principals: %d users, %d groups",
                app_name,
                len(users),
                len(groups),
            )
            return {"users": users, "groups": groups}
        except Exception as exc:
            logger.warning("Error listing app principals for '%s': %s", app_name, exc)
            return {"users": [], "groups": []}
