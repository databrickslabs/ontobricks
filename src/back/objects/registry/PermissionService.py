"""
Permission Service for OntoBricks.

New permission model:

- **App-level access** is driven entirely by the Databricks App permissions
  (``list_app_principals``).  A user either has CAN_MANAGE (admin) or appears
  (directly or via a group) in the App's ACL (``ROLE_APP_USER``).  There is
  no local ``.permissions.json`` file anymore.
- **Domain-level access** (Viewer / Editor / Builder) is managed per-domain
  in ``.domain_permissions.json`` files inside each domain folder.  Non-admin
  users with no entry on a given domain have **no access** to it.

Active only in Databricks App mode (DATABRICKS_APP_PORT is set).  In local
mode every user has unrestricted access.
"""

import json
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from back.core.logging import get_logger
from back.core.databricks.DatabricksClient import DatabricksClient
from back.core.databricks import VolumeFileService

logger = get_logger(__name__)

ROLE_ADMIN = "admin"
ROLE_BUILDER = "builder"
ROLE_EDITOR = "editor"
ROLE_VIEWER = "viewer"
ROLE_APP_USER = "app_user"
ROLE_NONE = "none"

ROLE_HIERARCHY: Dict[str, int] = {
    ROLE_NONE: 0,
    ROLE_VIEWER: 1,
    ROLE_EDITOR: 2,
    ROLE_BUILDER: 3,
    ROLE_ADMIN: 4,
}

ASSIGNABLE_ROLES = (ROLE_VIEWER, ROLE_EDITOR, ROLE_BUILDER)


def role_level(role: str) -> int:
    """Return the numeric level for *role* (0 for unknown)."""
    return ROLE_HIERARCHY.get(role, 0)


def min_role(a: str, b: str) -> str:
    """Return the less-privileged of two roles."""
    return a if role_level(a) <= role_level(b) else b


_DOMAIN_PERMISSIONS_FILENAME = ".domain_permissions.json"
_CACHE_TTL_DOMAIN_PERMS = 120  # 2 min – per-domain permission cache
_CACHE_TTL_ADMIN = 60  # 1 min – keep short to pick up permission changes quickly
_CACHE_TTL_PRINCIPALS = 600  # 10 min
_CACHE_TTL_USER_GROUPS = 300  # 5 min – SCIM group membership


class PermissionService:
    """Resolve App-level access and per-domain roles, plus manage team files."""

    def __init__(self):
        self._admin_cache: Dict[str, Tuple[bool, float]] = {}

        self._users_cache: Optional[List[Dict[str, Any]]] = None
        self._users_cache_ts: float = 0.0

        self._groups_cache: Optional[List[Dict[str, Any]]] = None
        self._groups_cache_ts: float = 0.0

        # Per-domain permission cache: keyed by domain folder name
        self._domain_perm_cache: Dict[str, Tuple[Dict[str, Any], float]] = {}

        # Per-user SCIM group membership cache
        self._user_groups_cache: Dict[str, Tuple[List[str], float]] = {}

        # First-deploy bootstrap signal: True when the last call to
        # list_app_principals came back 403, meaning the caller (usually the
        # app's own service principal) lacks CAN_VIEW_PERMISSIONS on the app.
        self._app_principals_forbidden: bool = False

    # ------------------------------------------------------------------
    # Role resolution
    # ------------------------------------------------------------------

    def get_user_role(
        self,
        email: str,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],  # kept for signature compat
        app_name: str,
        *,
        user_token: str = "",
    ) -> str:
        """Resolve the app-level access for *email*.

        Returns:
            ``ROLE_ADMIN``   -- user has CAN_MANAGE on the Databricks App.
            ``ROLE_APP_USER`` -- user (or a group they belong to) appears in
                                 the App's ACL.
            ``ROLE_NONE``    -- otherwise.

        ``registry_cfg`` is accepted for backward compatibility with the
        previous signature but is no longer used.
        """
        _ = registry_cfg  # unused; kept for signature compatibility

        if not email:
            return ROLE_NONE

        if self.is_admin(email, host, token, app_name, user_token=user_token):
            return ROLE_ADMIN

        if not app_name:
            return ROLE_NONE

        principals = self.list_app_principals(host, token, app_name)
        users = principals.get("users", [])
        email_l = email.lower()
        for u in users:
            if (u.get("email") or "").lower() == email_l:
                return ROLE_APP_USER

        groups = principals.get("groups", [])
        if groups:
            user_groups = self._get_user_groups(email, host, token)
            user_group_names_l = {g.lower() for g in user_groups}
            for g in groups:
                name = (g.get("display_name") or g.get("id") or "").lower()
                if name and name in user_group_names_l:
                    return ROLE_APP_USER

        return ROLE_NONE

    def _get_user_groups(self, email: str, host: str, token: str) -> List[str]:
        """Return group display-names that *email* belongs to (via SCIM)."""
        import requests as req

        if not host or not token:
            return []

        now = time.time()
        cached = self._user_groups_cache.get(email.lower())
        if cached and (now - cached[1]) < _CACHE_TTL_USER_GROUPS:
            return cached[0]

        try:
            h = host.rstrip("/")
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
            resp = req.get(
                f"{h}/api/2.0/preview/scim/v2/Users",
                headers=headers,
                params={"filter": f'userName eq "{email}"', "count": 1},
            )
            resp.raise_for_status()
            resources = resp.json().get("Resources", [])
            if not resources:
                self._user_groups_cache[email.lower()] = ([], now)
                return []
            groups = resources[0].get("groups", [])
            names = [g.get("display", "") for g in groups if g.get("display")]
            self._user_groups_cache[email.lower()] = (names, now)
            return names
        except Exception as e:
            logger.debug("Could not resolve groups for %s: %s", email, e)
            return []

    def clear_user_groups_cache(self, email: str = ""):
        if email:
            self._user_groups_cache.pop(email.lower(), None)
        else:
            self._user_groups_cache.clear()

    # ------------------------------------------------------------------
    # Admin detection via Databricks App Permissions API
    # ------------------------------------------------------------------

    def is_admin(
        self,
        email: str,
        host: str,
        token: str,
        app_name: str,
        *,
        user_token: str = "",
    ) -> bool:
        """Check if *email* has CAN_MANAGE on the Databricks App.

        Tries every available auth path until one gives a definitive
        answer (``True`` or ``False``).  A ``None`` return from a check
        means "could not determine" (timeout, 403, network error) and
        the next path is attempted.

        Order: user token REST → SDK (SP) → SP token REST.
        """
        if not email or not app_name:
            logger.debug("is_admin: skipped (email=%r, app_name=%r)", email, app_name)
            return False

        now = time.time()
        cached = self._admin_cache.get(email)
        if cached and (now - cached[1]) < _CACHE_TTL_ADMIN:
            return cached[0]

        result: bool | None = None

        if user_token and result is None:
            check = self._check_admin_rest(email, host, user_token, app_name)
            if check is not None:
                result = check

        if result is None:
            sdk_result = self._check_admin_sdk(email, app_name)
            if sdk_result is not None:
                result = sdk_result

        if result is None and token:
            check = self._check_admin_rest(email, host, token, app_name)
            if check is not None:
                result = check

        final = bool(result)
        self._admin_cache[email] = (final, now)
        logger.info("Admin check for %s: %s", email, final)
        return final

    def _check_admin_sdk(self, email: str, app_name: str) -> Optional[bool]:
        """Try the Databricks SDK to read app permissions."""
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout

        def _do():
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            logger.info(
                "SDK admin check: calling GET /api/2.0/permissions/apps/%s",
                app_name,
            )
            raw = w.api_client.do("GET", f"/api/2.0/permissions/apps/{app_name}")
            acl_list = raw.get("access_control_list", [])
            managers = []
            for acl in acl_list:
                principal = acl.get("user_name") or acl.get("group_name") or ""
                for p in acl.get("all_permissions", []):
                    if p.get("permission_level") == "CAN_MANAGE":
                        managers.append(principal)
                        if principal.lower() == email.lower():
                            logger.info(
                                "SDK admin check: MATCH %s == %s",
                                principal,
                                email,
                            )
                            return True
            logger.info(
                "SDK admin check: CAN_MANAGE principals=%s, "
                "looking for=%s → not found",
                managers,
                email,
            )
            return False

        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                result = pool.submit(_do).result(timeout=5)
            logger.info("SDK admin check for %s: %s", email, result)
            return result
        except FutTimeout:
            logger.warning("SDK admin check timed out for %s", email)
            return None
        except Exception as e:
            logger.warning(
                "SDK admin check failed for %s: %s (%s)",
                email,
                e,
                type(e).__name__,
            )
            return None

    def _check_admin_rest(
        self, email: str, host: str, token: str, app_name: str
    ) -> Optional[bool]:
        """Call the Permissions REST API. Returns True/False or None on error."""
        import requests as req

        if not host or not token or not app_name:
            return None
        try:
            h = host.rstrip("/")
            headers = {"Authorization": f"Bearer {token}"}
            resp = req.get(
                f"{h}/api/2.0/permissions/apps/{app_name}",
                headers=headers,
                timeout=5,
            )
            resp.raise_for_status()
            data = resp.json()
            for acl_entry in data.get("access_control_list", []):
                principal = (
                    acl_entry.get("user_name") or acl_entry.get("group_name") or ""
                )
                for p in acl_entry.get("all_permissions", []):
                    if (
                        p.get("permission_level") == "CAN_MANAGE"
                        and principal.lower() == email.lower()
                    ):
                        return True
            return False
        except Exception as e:
            logger.warning("REST admin check failed for %s: %s", email, e)
            return None

    # ------------------------------------------------------------------
    # Domain-level permission file I/O
    # ------------------------------------------------------------------

    def _new_uc(self, host: str, token: str) -> VolumeFileService:
        return VolumeFileService(host=host, token=token)

    def _domain_permissions_path(
        self,
        registry_cfg: Dict[str, str],
        domain_folder: str,
    ) -> str:
        """Path to .domain_permissions.json inside a domain folder."""
        from back.objects.registry.RegistryService import RegistryCfg

        c = RegistryCfg.from_dict(registry_cfg)
        return (
            f"/Volumes/{c.catalog}/{c.schema}/{c.volume}/domains/"
            f"{domain_folder}/{_DOMAIN_PERMISSIONS_FILENAME}"
        )

    def load_domain_permissions(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        domain_folder: str,
        *,
        force: bool = False,
    ) -> Dict[str, Any]:
        """Load and cache per-domain permission file."""
        now = time.time()
        if not force:
            cached = self._domain_perm_cache.get(domain_folder)
            if cached and (now - cached[1]) < _CACHE_TTL_DOMAIN_PERMS:
                return cached[0]

        path = self._domain_permissions_path(registry_cfg, domain_folder)
        try:
            uc = self._new_uc(host, token)
            ok, content, msg = uc.read_file(path)
            if ok and content:
                data = json.loads(content)
                self._domain_perm_cache[domain_folder] = (data, now)
                logger.info(
                    "Loaded %d domain permission entries for %s",
                    len(data.get("permissions", [])),
                    domain_folder,
                )
                return data
            logger.debug(
                "Domain permission file not found for %s: %s", domain_folder, msg
            )
        except Exception as e:
            logger.warning(
                "Error loading domain permissions for %s: %s", domain_folder, e
            )

        empty: Dict[str, Any] = {"version": 1, "permissions": []}
        self._domain_perm_cache[domain_folder] = (empty, now)
        return empty

    def save_domain_permissions(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        domain_folder: str,
        data: Dict[str, Any],
    ) -> Tuple[bool, str]:
        """Write the per-domain permission file."""
        if not registry_cfg.get("catalog") or not registry_cfg.get("schema"):
            return False, "Registry not configured"
        path = self._domain_permissions_path(registry_cfg, domain_folder)
        try:
            uc = self._new_uc(host, token)
            ok, msg = uc.write_file(path, json.dumps(data, indent=2), overwrite=True)
            if not ok:
                logger.error(
                    "Failed to write domain permissions for %s: %s", domain_folder, msg
                )
                return False, f"Failed to save domain permissions: {msg}"
            self._domain_perm_cache[domain_folder] = (data, time.time())
            logger.info(
                "Saved %d domain permission entries for %s",
                len(data.get("permissions", [])),
                domain_folder,
            )
            return True, "Domain permissions saved"
        except Exception as e:
            logger.error("Error saving domain permissions for %s: %s", domain_folder, e)
            return False, str(e)

    # ------------------------------------------------------------------
    # Domain-level role resolution
    # ------------------------------------------------------------------

    def _resolve_domain_entry_role(
        self,
        email: str,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        domain_folder: str,
    ) -> Optional[str]:
        """Resolve the domain-level entry for *email* (None = no entry)."""
        if not domain_folder:
            return None

        data = self.load_domain_permissions(host, token, registry_cfg, domain_folder)
        entries = data.get("permissions", [])
        if not entries:
            return None

        for entry in entries:
            if (
                entry.get("principal_type") == "user"
                and entry.get("principal", "").lower() == email.lower()
            ):
                return entry.get("role", ROLE_VIEWER)

        user_groups = self._get_user_groups(email, host, token)
        for entry in entries:
            if entry.get("principal_type") == "group" and entry.get(
                "principal", ""
            ).lower() in (g.lower() for g in user_groups):
                return entry.get("role", ROLE_VIEWER)

        return None

    def get_domain_role(
        self,
        email: str,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        app_name: str,
        domain_folder: str,
        *,
        user_token: str = "",
        app_role: str = "",
    ) -> str:
        """Resolve the effective role for *email* on a specific domain.

        Strict model:

        - Admins → ``ROLE_ADMIN``.
        - Otherwise the domain role comes *only* from
          ``.domain_permissions.json``. No entry → ``ROLE_NONE``.

        ``app_role`` is honoured as an optimization: the caller may pass the
        already-resolved app role to avoid a redundant admin lookup.
        """
        if app_role == ROLE_ADMIN:
            return ROLE_ADMIN

        if not app_role:
            app_role = self.get_user_role(
                email,
                host,
                token,
                registry_cfg,
                app_name,
                user_token=user_token,
            )
            if app_role == ROLE_ADMIN:
                return ROLE_ADMIN

        if not domain_folder:
            return ROLE_NONE

        domain_entry = self._resolve_domain_entry_role(
            email,
            host,
            token,
            registry_cfg,
            domain_folder,
        )
        if domain_entry is None:
            return ROLE_NONE
        return domain_entry

    # ------------------------------------------------------------------
    # Domain-level CRUD helpers
    # ------------------------------------------------------------------

    def list_domain_entries(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        domain_folder: str,
    ) -> List[Dict[str, Any]]:
        data = self.load_domain_permissions(host, token, registry_cfg, domain_folder)
        return data.get("permissions", [])

    def add_or_update_domain_entry(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        domain_folder: str,
        principal: str,
        principal_type: str,
        display_name: str,
        role: str,
    ) -> Tuple[bool, str]:
        data = self.load_domain_permissions(
            host,
            token,
            registry_cfg,
            domain_folder,
            force=True,
        )
        entries = data.get("permissions", [])

        for entry in entries:
            if entry["principal"].lower() == principal.lower():
                entry["role"] = role
                entry["display_name"] = display_name
                entry["principal_type"] = principal_type
                return self.save_domain_permissions(
                    host,
                    token,
                    registry_cfg,
                    domain_folder,
                    data,
                )

        entries.append(
            {
                "principal": principal,
                "principal_type": principal_type,
                "display_name": display_name,
                "role": role,
            }
        )
        data["permissions"] = entries
        return self.save_domain_permissions(
            host,
            token,
            registry_cfg,
            domain_folder,
            data,
        )

    def remove_domain_entry(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        domain_folder: str,
        principal: str,
    ) -> Tuple[bool, str]:
        data = self.load_domain_permissions(
            host,
            token,
            registry_cfg,
            domain_folder,
            force=True,
        )
        before = len(data.get("permissions", []))
        data["permissions"] = [
            e
            for e in data.get("permissions", [])
            if e["principal"].lower() != principal.lower()
        ]
        if len(data["permissions"]) == before:
            return False, f"Principal '{principal}' not found in domain permissions"
        return self.save_domain_permissions(
            host,
            token,
            registry_cfg,
            domain_folder,
            data,
        )

    def clear_domain_perm_cache(self, domain_folder: str = ""):
        """Drop cached domain permission data."""
        if domain_folder:
            self._domain_perm_cache.pop(domain_folder, None)
        else:
            self._domain_perm_cache.clear()

    # ------------------------------------------------------------------
    # Visibility filter for domain lists
    # ------------------------------------------------------------------

    def filter_accessible_domains(
        self,
        email: str,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        app_name: str,
        entries: List[Any],
        *,
        user_token: str = "",
        app_role: str = "",
        key: Optional[Callable[[Any], str]] = None,
    ) -> List[Any]:
        """Return only the entries the user has a role != ``ROLE_NONE`` on.

        Admins (either by passed-in *app_role* or via live lookup) get the
        full list back unchanged.  When *key* is ``None`` each entry is
        treated as a folder name string, or ``entry["name"]`` for dicts.
        """
        if not entries:
            return []

        if app_role == ROLE_ADMIN or self.is_admin(
            email, host, token, app_name, user_token=user_token
        ):
            return list(entries)

        def _folder(e: Any) -> str:
            if key is not None:
                return key(e)
            if isinstance(e, str):
                return e
            return e.get("name", "") if isinstance(e, dict) else ""

        # Use the already-resolved app role to skip a redundant admin
        # lookup inside get_domain_role. Fall back to ROLE_APP_USER so
        # admin-level resolution is not triggered.
        effective_app_role = app_role or ROLE_APP_USER
        out: List[Any] = []
        for entry in entries:
            folder = _folder(entry)
            if not folder:
                continue
            role = self.get_domain_role(
                email,
                host,
                token,
                registry_cfg,
                app_name,
                folder,
                user_token=user_token,
                app_role=effective_app_role,
            )
            if role != ROLE_NONE:
                out.append(entry)
        return out

    # ------------------------------------------------------------------
    # Batch domain-permission save (matrix UI)
    # ------------------------------------------------------------------

    def save_domain_permissions_batch(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        changes: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Apply a batch of team changes across several domains in one pass.

        Each change is a dict::

            {
              "domain_folder": "acme",
              "principal": "alice@example.com",
              "principal_type": "user" | "group",
              "display_name": "Alice",
              "role": "viewer" | "editor" | "builder" | None,  # None == remove
            }

        The function groups changes by domain, re-reads each file fresh,
        applies all operations, and writes the file back once per domain.

        Returns ``(saved, failed)`` where each element is a dict like
        ``{"domain": str, "count": int, "message": str}``.
        """
        if not registry_cfg.get("catalog") or not registry_cfg.get("schema"):
            return [], [{"domain": "", "count": 0, "message": "Registry not configured"}]

        saved: List[Dict[str, Any]] = []
        failed: List[Dict[str, Any]] = []

        by_domain: Dict[str, List[Dict[str, Any]]] = {}
        for ch in changes:
            df = (ch.get("domain_folder") or "").strip()
            if not df:
                failed.append(
                    {"domain": "", "count": 0, "message": "missing domain_folder"}
                )
                continue
            by_domain.setdefault(df, []).append(ch)

        for domain_folder, ops in by_domain.items():
            try:
                data = self.load_domain_permissions(
                    host, token, registry_cfg, domain_folder, force=True
                )
                entries: List[Dict[str, Any]] = data.get("permissions", [])

                for op in ops:
                    principal = (op.get("principal") or "").strip()
                    if not principal:
                        continue
                    role = op.get("role")
                    pl = principal.lower()

                    if role is None:
                        entries = [e for e in entries if e["principal"].lower() != pl]
                        continue

                    found = False
                    for e in entries:
                        if e["principal"].lower() == pl:
                            e["role"] = role
                            e["display_name"] = op.get("display_name", principal)
                            e["principal_type"] = op.get("principal_type", "user")
                            found = True
                            break
                    if not found:
                        entries.append(
                            {
                                "principal": principal,
                                "principal_type": op.get("principal_type", "user"),
                                "display_name": op.get("display_name", principal),
                                "role": role,
                            }
                        )

                data["permissions"] = entries
                ok, msg = self.save_domain_permissions(
                    host, token, registry_cfg, domain_folder, data
                )
                if ok:
                    saved.append(
                        {"domain": domain_folder, "count": len(ops), "message": msg}
                    )
                else:
                    failed.append(
                        {"domain": domain_folder, "count": len(ops), "message": msg}
                    )
            except Exception as exc:
                logger.error(
                    "Batch save failed for domain %s: %s", domain_folder, exc
                )
                failed.append(
                    {
                        "domain": domain_folder,
                        "count": len(ops),
                        "message": f"{type(exc).__name__}: {exc}",
                    }
                )

        return saved, failed

    # ------------------------------------------------------------------
    # App principal listing (cached)
    # ------------------------------------------------------------------

    def clear_admin_cache(self, email: str = ""):
        """Drop cached admin result so the next call hits the API.

        If *email* is given, only that entry is removed; otherwise the
        entire admin cache is cleared.
        """
        if email:
            self._admin_cache.pop(email, None)
        else:
            self._admin_cache.clear()

    def clear_principals_cache(self):  # noqa: D401
        """Invalidate the cached users/groups so the next call fetches fresh data."""
        self._users_cache = None
        self._groups_cache = None
        self._app_principals_forbidden = False

    def list_app_principals(
        self, host: str, token: str, app_name: str
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Return users and groups that have permissions on the Databricks App.

        Also captures whether the underlying REST call came back 403, which
        indicates the app's own service principal is not allowed to read its
        own ACL (first-deploy bootstrap).  The flag is exposed via
        :meth:`is_app_principals_forbidden` and consumed by the permission
        middleware to render a targeted access-denied page.
        """
        now = time.time()
        if (
            self._users_cache is not None
            and self._groups_cache is not None
            and (now - self._users_cache_ts) < _CACHE_TTL_PRINCIPALS
        ):
            return {"users": self._users_cache, "groups": self._groups_cache}

        client = DatabricksClient(host=host, token=token)
        result = client.list_app_principals(app_name)
        status = getattr(client, "last_app_permissions_status", 0)
        self._app_principals_forbidden = status == 403
        self._users_cache = result.get("users", [])
        self._groups_cache = result.get("groups", [])
        self._users_cache_ts = now
        self._groups_cache_ts = now
        return result

    def is_app_principals_forbidden(self) -> bool:
        """Return True if the last ``list_app_principals`` call was denied.

        Used by the middleware to detect the first-deploy chicken-and-egg:
        the app service principal has no permission on its own app, so the
        ACL can't be read and every user ends up as ``ROLE_NONE``.
        """
        return self._app_principals_forbidden

    def list_users(self, host: str, token: str) -> List[Dict[str, Any]]:
        now = time.time()
        if (
            self._users_cache is not None
            and (now - self._users_cache_ts) < _CACHE_TTL_PRINCIPALS
        ):
            return self._users_cache

        client = DatabricksClient(host=host, token=token)
        users = client.list_workspace_users()
        self._users_cache = users
        self._users_cache_ts = now
        return users

    def list_groups(self, host: str, token: str) -> List[Dict[str, Any]]:
        now = time.time()
        if (
            self._groups_cache is not None
            and (now - self._groups_cache_ts) < _CACHE_TTL_PRINCIPALS
        ):
            return self._groups_cache

        client = DatabricksClient(host=host, token=token)
        groups = client.list_workspace_groups()
        self._groups_cache = groups
        self._groups_cache_ts = now
        return groups


# Singleton instance shared across the application
permission_service = PermissionService()
