"""End-to-end provisioning of a Lakebase graph database from the Settings UI.

This is the in-app Python port of the two operator scripts that used to be
the only way to stand up the graph store:

- ``scripts/setup-lakebase.sh``           -- create the Lakebase
  instance/project + Postgres database (via the *old*
  ``/api/2.0/database/instances`` API so the project stays compatible with
  the Synced Tables API used by ``managed_synced`` builds).
- ``scripts/bootstrap-lakebase-perms.sh`` -- grant ``CAN_USE`` on the
  project and ``USAGE``/``CREATE``/DML on the graph schema to the app +
  MCP service principals.

The whole flow runs in a worker thread and reports progress through the
shared :class:`~back.core.task_manager.TaskManager`, so the UI can poll
``GET /tasks/{id}`` exactly like the Knowledge Graph build.

Permission model (unchanged from the scripts -- only automated):

- The button runs as the **app's own service principal**, not a human.
  Creating a Lakebase *instance* therefore needs the SP to be allowed to
  create instances at the workspace/account level. When it is not, the
  flow fails on the first step with a clear message and the operator
  scripts remain the documented fallback.
- The SP that *creates* the project is its owner, so it can grant
  ``CAN_USE`` to itself and to the MCP SP.
- The SP that *creates* the schema owns it, so it can run the ``GRANT``
  statements. Granting the MCP SP requires its Postgres role to exist;
  ``CAN_USE`` is applied first (step 6) to provision identity federation,
  and the schema grants (step 7) are best-effort per SP with explicit
  warnings -- the same tolerance the bash script has.
"""

from __future__ import annotations

import re
import time
from typing import Any, Callable, Dict, List, Optional

from back.core.logging import get_logger

logger = get_logger(__name__)

# Lakebase/Postgres resource names: force lowercase and restrict to letters,
# digits, hyphen and underscore. Any run of other characters collapses to a
# single underscore; leading/trailing separators are trimmed.
_NAME_CLEAN_RE = re.compile(r"[^a-z0-9_-]+")


def _normalize_name(raw: str) -> str:
    """Lowercase ``raw`` and keep only ``[a-z0-9_-]`` characters."""
    cleaned = _NAME_CLEAN_RE.sub("_", (raw or "").strip().lower())
    return cleaned.strip("-_")

# Lakebase Autoscaling compute capacities accepted by the instances API.
ALLOWED_CAPACITIES = ("CU_1", "CU_2", "CU_4", "CU_8")
DEFAULT_CAPACITY = "CU_2"
DEFAULT_BRANCH = "production"

# How long to wait for a freshly created instance to reach AVAILABLE.
_AVAILABLE_TIMEOUT_S = 600.0
_AVAILABLE_POLL_S = 5.0


class ProvisionError(Exception):
    """Raised for any hard failure that should abort the provision flow."""


def provision_steps(*, grant_uc: bool) -> List[Dict[str, str]]:
    """Return the seeded TaskManager step list for the provision flow."""
    steps = [
        {"name": "instance", "description": "Creating Lakebase instance"},
        {"name": "available", "description": "Waiting for instance to become available"},
        {"name": "endpoint", "description": "Resolving branch endpoint"},
        {"name": "database", "description": "Creating Postgres database"},
        {"name": "schema", "description": "Creating graph schema"},
        {"name": "can_use", "description": "Granting CAN_USE on the project"},
        {"name": "grants", "description": "Granting schema privileges"},
        {"name": "superusers", "description": "Granting Postgres superuser to CAN_MANAGE users"},
    ]
    if grant_uc:
        steps.append(
            {"name": "uc", "description": "Granting Unity Catalog privileges"}
        )
    return steps


class LakebaseGraphProvisioner:
    """Drive the instance -> database -> schema -> grants flow.

    Parameters
    ----------
    tm, task_id:
        Task manager + task id used to report progress.
    name:
        Lakebase instance/project name to create (or adopt if present).
    capacity:
        Compute capacity (``CU_1`` / ``CU_2`` / ``CU_4`` / ``CU_8``).
    branch:
        Branch within the project (default ``production``).
    database:
        Postgres database to create inside the branch.
    schema:
        Graph schema to create (already validated by the caller).
    app_names:
        Databricks App names whose service principals receive the grants
        (the running app first, then the MCP app).
    sync_mode:
        ``app_managed`` or ``managed_synced`` (controls the UC grant).
    uc_catalog:
        Unity Catalog catalog to grant ``ALL_PRIVILEGES`` on
        (``managed_synced`` only).
    pg_user:
        Postgres role to authenticate as (the app SP -- normally ``PGUSER``).
    on_success:
        Optional callback invoked with the result dict just before the
        task is marked complete (used to persist the new coordinates into
        ``graph_engine_config``). Best-effort -- failures are logged, not
        fatal.
    """

    def __init__(
        self,
        *,
        tm: Any,
        task_id: str,
        name: str,
        capacity: str,
        branch: str,
        database: str,
        schema: str,
        app_names: List[str],
        sync_mode: str = "app_managed",
        uc_catalog: str = "",
        pg_user: str = "",
        operator_email: str = "",
        on_success: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        self._tm = tm
        self._task_id = task_id
        # All resource names are forced to lowercase and restricted to
        # ``[a-z0-9_-]`` so the values we create, look up, and grant on match
        # the canonical resources the platform registers. The raw inputs are
        # kept so we can warn the operator when normalisation changed them.
        self._requested = {
            "instance": (name or "").strip(),
            "branch": (branch or DEFAULT_BRANCH).strip(),
            "database": (database or "").strip(),
            "schema": (schema or "").strip(),
        }
        self._name = _normalize_name(name)
        self._capacity = capacity if capacity in ALLOWED_CAPACITIES else DEFAULT_CAPACITY
        self._branch = _normalize_name(branch or DEFAULT_BRANCH)
        self._database = _normalize_name(database)
        self._schema = _normalize_name(schema)
        self._app_names = [a for a in app_names if a]
        self._sync_mode = sync_mode or "app_managed"
        self._uc_catalog = (uc_catalog or "").strip()
        self._pg_user = (pg_user or "").strip()
        self._operator_email = (operator_email or "").strip()
        self._on_success = on_success

        # Resolved canonical resource paths (filled in during the endpoint
        # step by listing /postgres/projects rather than assuming the path).
        self._project_path = f"projects/{self._name}"
        self._project_short = self._name
        self._branch_path = f"projects/{self._name}/branches/{self._branch}"
        self._endpoint_resource = ""
        self._host = ""
        self._warnings: List[str] = []
        self._granted: List[str] = []

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Execute every step, reporting progress to the task manager."""
        tm = self._tm
        try:
            self._validate_inputs()
            for label, norm in (
                ("instance", self._name),
                ("branch", self._branch),
                ("database", self._database),
                ("schema", self._schema),
            ):
                raw = self._requested.get(label, "")
                if raw and raw != norm:
                    self._warnings.append(
                        f"{label} name normalised to lowercase {norm!r} "
                        f"(from {raw!r})"
                    )
            api = self._api()

            tm.start_task(self._task_id, "Creating Lakebase instance...")
            self._step_create_instance(api)

            tm.advance_step(self._task_id)
            self._step_wait_available(api)

            tm.advance_step(self._task_id)
            self._step_resolve_endpoint(api)

            tm.advance_step(self._task_id)
            self._step_create_database(api)

            tm.advance_step(self._task_id)
            self._step_create_schema(api)

            tm.advance_step(self._task_id)
            sp_ids = self._resolve_service_principals(api)
            self._step_grant_can_use(api, sp_ids)

            tm.advance_step(self._task_id)
            self._step_grant_schema(api, sp_ids)

            tm.advance_step(self._task_id)
            self._step_grant_superuser_to_managers(api)

            if self._uc_catalog and self._sync_mode == "managed_synced":
                tm.advance_step(self._task_id)
                self._step_grant_uc(api, sp_ids)

            result = {
                "instance": self._name,
                "branch": self._branch,
                "branch_path": self._branch_path,
                "database": self._database,
                "schema": self._schema,
                "capacity": self._capacity,
                "granted": self._granted,
                "warnings": self._warnings,
            }
            if self._on_success:
                try:
                    self._on_success(result)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("provision on_success callback failed: %s", exc)
                    self._warnings.append(f"Could not persist config: {exc}")

            msg = (
                f"Graph DB ready: {self._name}/{self._branch}/{self._database} "
                f"(schema {self._schema})"
            )
            if self._warnings:
                msg += f" — {len(self._warnings)} warning(s)"
            tm.complete_task(self._task_id, result=result, message=msg)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Lakebase graph DB provisioning failed")
            tm.fail_task(self._task_id, str(exc))

    # ------------------------------------------------------------------
    # Steps
    # ------------------------------------------------------------------

    def _step_create_instance(self, api: Any) -> None:
        existing = self._get_instance(api)
        if existing and existing.get("state"):
            logger.info(
                "Lakebase instance %r already exists (state=%s); adopting",
                self._name,
                existing.get("state"),
            )
            self._tm.update_progress(
                self._task_id, 5, f"Instance {self._name} already exists"
            )
            return
        # Old instances API (NOT /api/2.0/postgres/projects) so the project
        # stays compatible with the Synced Tables API used by managed_synced.
        api.do(
            "POST",
            "/api/2.0/database/instances",
            body={
                "name": self._name,
                "capacity": self._capacity,
                "enable_pg_native_login": True,
            },
        )
        self._tm.update_progress(
            self._task_id, 5, f"Instance {self._name} creation requested"
        )

    def _step_wait_available(self, api: Any) -> None:
        deadline = time.monotonic() + _AVAILABLE_TIMEOUT_S
        while True:
            self._check_cancelled()
            info = self._get_instance(api) or {}
            state = (info.get("state") or "").upper()
            self._tm.update_progress(
                self._task_id, 12, f"Instance state: {state or '?'}"
            )
            if state == "AVAILABLE":
                return
            if state in ("FAILED", "DELETING", "DELETED"):
                raise ProvisionError(
                    f"Instance {self._name!r} entered unexpected state {state!r}"
                )
            if time.monotonic() >= deadline:
                raise ProvisionError(
                    f"Timed out after {int(_AVAILABLE_TIMEOUT_S)}s waiting for "
                    f"instance {self._name!r} to become AVAILABLE (state={state!r})"
                )
            time.sleep(_AVAILABLE_POLL_S)

    def _step_resolve_endpoint(self, api: Any) -> None:
        # Resolve the *canonical* project + branch resource paths rather than
        # assuming ``projects/<name>`` — the platform may register the project
        # under a normalised (lowercase) name or an opaque id, and a freshly
        # created instance can take a moment to surface in the Postgres API.
        self._resolve_project_path(api)
        self._resolve_branch_path(api)

        last_err = ""
        for _ in range(12):
            self._check_cancelled()
            try:
                endpoints = (
                    api.do("GET", f"/api/2.0/postgres/{self._branch_path}/endpoints")
                    or {}
                ).get("endpoints") or []
            except Exception as exc:  # noqa: BLE001
                last_err = str(exc)
                endpoints = []
            for ep in endpoints:
                host = (
                    ((ep.get("status") or {}).get("hosts") or {})
                    .get("host", "")
                    .strip()
                )
                if host:
                    self._host = host
                    self._endpoint_resource = ep.get("name") or ""
                    self._tm.update_progress(
                        self._task_id, 30, f"Endpoint resolved: {host}"
                    )
                    return
            time.sleep(5.0)
        detail = f" ({last_err})" if last_err else ""
        raise ProvisionError(
            f"No active endpoint found for branch {self._branch_path!r}{detail}"
        )

    def _resolve_project_path(self, api: Any) -> None:
        """Find the canonical ``projects/<id>`` path for the instance name."""
        target = self._name.lower()
        last_err = ""
        for _ in range(10):
            self._check_cancelled()
            try:
                projects = (
                    api.do("GET", "/api/2.0/postgres/projects") or {}
                ).get("projects") or []
            except Exception as exc:  # noqa: BLE001
                last_err = str(exc)
                projects = []
            for p in projects:
                path = p.get("name") or ""
                short = path.rsplit("/", 1)[-1]
                if short.lower() == target:
                    self._project_path = path
                    self._project_short = short
                    return
            time.sleep(3.0)
        detail = f" ({last_err})" if last_err else ""
        raise ProvisionError(
            f"Lakebase project {self._name!r} did not appear under "
            f"/api/2.0/postgres/projects{detail}"
        )

    def _resolve_branch_path(self, api: Any) -> None:
        """Find the canonical branch resource path under the project.

        Prefer an exact (case-insensitive) match on the requested branch
        name, but a freshly created instance auto-creates a single default
        branch whose name the caller does not control — so when no exact
        match exists and there is exactly one branch, use it (and warn).
        """
        target = self._branch.lower()
        last_err = ""
        last_branches: List[Dict[str, Any]] = []
        for _ in range(10):
            self._check_cancelled()
            try:
                branches = (
                    api.do("GET", f"/api/2.0/postgres/{self._project_path}/branches")
                    or {}
                ).get("branches") or []
            except Exception as exc:  # noqa: BLE001
                last_err = str(exc)
                branches = []
            last_branches = branches
            for b in branches:
                path = b.get("name") or ""
                short = path.rsplit("/", 1)[-1]
                if short.lower() == target:
                    self._branch_path = path
                    return
            # Fall back to the sole default branch of a new instance.
            if len(branches) == 1:
                path = branches[0].get("name") or ""
                short = path.rsplit("/", 1)[-1]
                if path:
                    self._branch_path = path
                    if short.lower() != target:
                        self._warnings.append(
                            f"Requested branch {self._branch!r} not present; "
                            f"using the project's default branch {short!r}"
                        )
                        self._branch = short
                    return
            time.sleep(3.0)
        available = ", ".join(
            (b.get("name") or "").rsplit("/", 1)[-1] for b in last_branches
        ) or "none"
        detail = f" ({last_err})" if last_err else ""
        raise ProvisionError(
            f"Branch {self._branch!r} not found in project "
            f"{self._project_short!r} (available: {available}){detail}"
        )

    def _resolve_role_path(self, api: Any) -> str:
        """Return the full resource path of an owner role for the new database.

        ``spec.role`` on the database create call must be the full
        ``projects/<p>/branches/<b>/roles/<role-id>`` path, where ``<role-id>``
        is the *resource id* (e.g. ``benoit-cayla``) — not the Postgres role
        name (``status.postgres_role``, e.g. ``benoit.cayla@databricks.com``).
        We list the branch roles and match PGUSER against ``postgres_role``
        (or the resource id). When PGUSER's role does not yet exist on a brand
        new branch — its service-principal role is only federated later, by
        the CAN_USE step — we fall back to an existing ``USER`` role (the
        instance owner) so the database still gets a valid owner.
        """
        target = self._pg_user.lower()
        try:
            roles = (
                api.do("GET", f"/api/2.0/postgres/{self._branch_path}/roles") or {}
            ).get("roles") or []
        except Exception as exc:  # noqa: BLE001
            logger.debug("list roles failed: %s", exc)
            roles = []

        # 1. Exact match on the Postgres role (email/UUID) or the resource id.
        for r in roles:
            path = r.get("name") or ""
            if not path:
                continue
            status = r.get("status") or {}
            pg_role = str(status.get("postgres_role") or "").lower()
            short = path.rsplit("/", 1)[-1].lower()
            if target in (pg_role, short):
                return path

        # 2. Fall back to an existing USER (human owner) role.
        for r in roles:
            path = r.get("name") or ""
            status = r.get("status") or {}
            if path and status.get("identity_type") == "USER":
                self._warnings.append(
                    f"PGUSER role {self._pg_user!r} not found on the new branch; "
                    f"using {path.rsplit('/', 1)[-1]!r} as the database owner"
                )
                return path

        # 3. Last resort: any role at all.
        if roles and roles[0].get("name"):
            path = roles[0]["name"]
            self._warnings.append(
                f"PGUSER role {self._pg_user!r} not found; using "
                f"{path.rsplit('/', 1)[-1]!r} as the database owner"
            )
            return path

        raise ProvisionError(
            f"No Postgres role found on branch {self._branch_path!r} to own the "
            f"new database (PGUSER={self._pg_user!r})"
        )

    def _step_create_database(self, api: Any) -> None:
        if self._database_exists(api):
            self._tm.update_progress(
                self._task_id, 45, f"Database {self._database} already exists"
            )
            return
        # ``spec.role`` is the owner Postgres role for the new database and must
        # be the *full* resource path
        # (``projects/<p>/branches/<b>/roles/<role>``); use the connecting
        # service-principal role (PGUSER) so it owns the DB and can create the
        # graph schema next.
        role_path = self._resolve_role_path(api)
        self._tm.update_progress(
            self._task_id, 40, f"Creating database {self._database} (owner {self._pg_user})"
        )
        api.do(
            "POST",
            f"/api/2.0/postgres/{self._branch_path}/databases",
            body={
                "spec": {
                    "postgres_database": self._database,
                    "role": role_path,
                }
            },
        )
        # Phase 1: wait for the control-plane API to list the new database.
        for _ in range(10):
            self._check_cancelled()
            if self._database_exists(api):
                break
            time.sleep(3.0)

        # Phase 2: wait for the database to accept Postgres connections.
        # The API may report the DB as existing before the Postgres layer
        # is ready, causing "database does not exist" on the first connect.
        self._tm.update_progress(
            self._task_id, 43, f"Waiting for {self._database} to become reachable…"
        )
        self._wait_for_db_reachable()
        self._tm.update_progress(
            self._task_id, 45, f"Database {self._database} created"
        )

    def _step_create_schema(self, api: Any) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{self._schema}"')
        self._tm.update_progress(
            self._task_id, 60, f"Schema {self._schema} created"
        )

    def _step_grant_can_use(self, api: Any, sp_ids: Dict[str, str]) -> None:
        for app_name, sp_id in sp_ids.items():
            ok = False
            for securable in ("database-projects", "database-instances"):
                try:
                    api.do(
                        "PATCH",
                        f"/api/2.0/permissions/{securable}/{self._project_short}",
                        body={
                            "access_control_list": [
                                {
                                    "service_principal_name": sp_id,
                                    "permission_level": "CAN_USE",
                                }
                            ]
                        },
                    )
                    ok = True
                except Exception as exc:  # noqa: BLE001
                    logger.debug(
                        "CAN_USE grant via %s for %s failed: %s",
                        securable,
                        app_name,
                        exc,
                    )
            if ok:
                self._granted.append(f"{app_name}: CAN_USE on project")
            else:
                self._warnings.append(
                    f"{app_name}: could not grant CAN_USE on project (need "
                    f"manage permission on the Lakebase project)"
                )
        self._tm.update_progress(self._task_id, 75, "CAN_USE grants applied")

    def _step_grant_schema(self, api: Any, sp_ids: Dict[str, str]) -> None:
        sch = self._schema
        with self._connect() as conn:
            for app_name, sp_id in sp_ids.items():
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            f'GRANT USAGE, CREATE ON SCHEMA "{sch}" TO "{sp_id}"'
                        )
                        cur.execute(
                            f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES "
                            f'IN SCHEMA "{sch}" TO "{sp_id}"'
                        )
                        cur.execute(
                            f"GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES "
                            f'IN SCHEMA "{sch}" TO "{sp_id}"'
                        )
                        cur.execute(
                            f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{sch}" '
                            f'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES '
                            f'TO "{sp_id}"'
                        )
                        cur.execute(
                            f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{sch}" '
                            f'GRANT USAGE, SELECT, UPDATE ON SEQUENCES '
                            f'TO "{sp_id}"'
                        )
                    self._granted.append(f"{app_name}: USAGE + DML on schema {sch}")
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "Schema grant for %s (%s) failed: %s", app_name, sp_id, exc
                    )
                    self._warnings.append(
                        f"{app_name}: schema grant failed ({exc}). The Postgres "
                        f"role may not exist yet — re-run after the app has "
                        f"connected once, or use scripts/bootstrap-lakebase-perms.sh."
                    )
        self._tm.update_progress(self._task_id, 90, "Schema grants applied")

    def _step_grant_uc(self, api: Any, sp_ids: Dict[str, str]) -> None:
        for app_name, sp_id in sp_ids.items():
            try:
                api.do(
                    "PATCH",
                    f"/api/2.1/unity-catalog/permissions/catalog/{self._uc_catalog}",
                    body={
                        "changes": [
                            {"principal": sp_id, "add": ["ALL_PRIVILEGES"]}
                        ]
                    },
                )
                self._granted.append(
                    f"{app_name}: ALL_PRIVILEGES on catalog {self._uc_catalog}"
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "UC catalog grant for %s failed: %s", app_name, exc
                )
                self._warnings.append(
                    f"{app_name}: UC catalog grant on {self._uc_catalog} failed "
                    f"({exc}). You may lack MANAGE on the catalog."
                )
        self._tm.update_progress(self._task_id, 98, "Unity Catalog grants applied")

    def _step_grant_superuser_to_managers(self, api: Any) -> None:
        """Provision Postgres roles + DATABRICKS_SUPERUSER for every workspace admin.

        Workspace admins hold ``CAN_MANAGE`` on every Lakebase project via the
        *admins* group, but that group membership is never expanded to individual
        ``user_name`` rows in the project ACL — so reading the project ACL alone
        is insufficient.  Instead we resolve admins via the SCIM ``/Groups``
        endpoint (``displayName eq admins``) and act on ``Users/`` members only
        (service-principal members already have DATABRICKS_SUPERUSER as the
        project creator).

        For each admin user we ensure:

        1. A ``LAKEBASE_OAUTH_V1`` Postgres role (created if absent).
        2. ``DATABRICKS_SUPERUSER`` group membership (patched if absent).

        Idempotent and best-effort — users who already hold the membership are
        skipped; individual failures produce warnings but do not abort the flow.
        """
        manager_users = self._resolve_admin_emails(api)
        if not manager_users:
            self._tm.update_progress(
                self._task_id, 93, "No workspace admin users found — superuser step skipped"
            )
            return

        try:
            existing = (
                api.do("GET", f"/api/2.0/postgres/{self._branch_path}/roles") or {}
            ).get("roles") or []
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not list branch roles: %s", exc)
            existing = []

        # email → {role_id, has_superuser}
        role_map: Dict[str, Dict[str, Any]] = {}
        for r in existing:
            status = r.get("status") or {}
            pg_role = str(status.get("postgres_role") or "").lower()
            if not pg_role:
                continue
            role_map[pg_role] = {
                "role_id": (r.get("name") or "").rsplit("/", 1)[-1],
                "has_superuser": "DATABRICKS_SUPERUSER" in (status.get("membership_roles") or []),
            }

        for user_email in manager_users:
            self._ensure_superuser_role(api, user_email, role_map)

        self._tm.update_progress(
            self._task_id,
            93,
            f"Superuser grants applied to {len(manager_users)} CAN_MANAGE user(s)",
        )

    def _resolve_admin_emails(self, api: Any) -> List[str]:
        """Return emails that should receive DATABRICKS_SUPERUSER.

        Priority order:

        1. ``operator_email`` — the human who clicked the button.  Always
           included when set.  This is the only reliable source when the
           provisioner runs as a service principal that lacks SCIM read
           access.

        2. Workspace admins group via SCIM (best-effort).  May return an
           empty supplemental list when called as a non-admin SP — that is
           not treated as an error.
        """
        seen: set = set()
        emails: List[str] = []

        if self._operator_email:
            emails.append(self._operator_email)
            seen.add(self._operator_email.lower())

        try:
            resources = (
                api.do(
                    "GET",
                    "/api/2.0/preview/scim/v2/Groups?filter=displayName+eq+admins",
                )
                or {}
            ).get("Resources") or []
            for group in resources:
                for member in (group.get("members") or []):
                    ref = member.get("$ref") or ""
                    if not ref.startswith("Users/"):
                        continue
                    user_id = member.get("value") or ref.split("/", 1)[-1]
                    if not user_id:
                        continue
                    try:
                        user = (
                            api.do(
                                "GET",
                                f"/api/2.0/preview/scim/v2/Users/{user_id}",
                            )
                            or {}
                        )
                        email = user.get("userName") or ""
                        if email and email.lower() not in seen:
                            emails.append(email)
                            seen.add(email.lower())
                    except Exception as exc:  # noqa: BLE001
                        logger.debug(
                            "Could not resolve admin user %s: %s", user_id, exc
                        )
        except Exception as exc:  # noqa: BLE001
            # Expected when the SP doesn't have SCIM read access — not fatal.
            logger.debug("SCIM admin group lookup skipped: %s", exc)

        return emails

    def _ensure_superuser_role(
        self, api: Any, user_email: str, role_map: Dict[str, Dict[str, Any]]
    ) -> None:
        """Create (if absent) and promote *user_email* to DATABRICKS_SUPERUSER."""
        email_lower = user_email.lower()
        existing = role_map.get(email_lower)

        if existing and existing.get("has_superuser"):
            logger.debug("%s already has DATABRICKS_SUPERUSER — skipping", user_email)
            return

        role_id: str = (existing or {}).get("role_id", "")

        if not role_id:
            try:
                op = (
                    api.do(
                        "POST",
                        f"/api/2.0/postgres/{self._branch_path}/roles",
                        body={
                            "spec": {
                                "identity_type": "USER",
                                "postgres_role": user_email,
                                "auth_method": "LAKEBASE_OAUTH_V1",
                            }
                        },
                    )
                    or {}
                )
                # Extract role_id from the LRO name:
                # ".../roles/<role_id>/operations/<op_id>"
                op_name = op.get("name") or ""
                parts = op_name.split("/")
                if "roles" in parts:
                    idx = parts.index("roles")
                    if idx + 1 < len(parts) and parts[idx + 1] != "operations":
                        role_id = parts[idx + 1]
                if not role_id:
                    raise ProvisionError(
                        f"Could not extract role_id from operation: {op_name!r}"
                    )
                time.sleep(3.0)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Postgres role creation failed for %s: %s", user_email, exc
                )
                self._warnings.append(
                    f"{user_email}: Postgres role creation failed ({exc})"
                )
                return

        try:
            api.do(
                "PATCH",
                (
                    f"/api/2.0/postgres/{self._branch_path}/roles/{role_id}"
                    f"?update_mask=spec.membership_roles"
                ),
                body={"spec": {"membership_roles": ["DATABRICKS_SUPERUSER"]}},
            )
            self._granted.append(
                f"{user_email}: DATABRICKS_SUPERUSER on {self._project_short}"
            )
            logger.info(
                "Granted DATABRICKS_SUPERUSER to %s on %s",
                user_email,
                self._project_short,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "DATABRICKS_SUPERUSER grant failed for %s: %s", user_email, exc
            )
            self._warnings.append(
                f"{user_email}: DATABRICKS_SUPERUSER grant failed ({exc})"
            )

    # ------------------------------------------------------------------
    # Service principal resolution
    # ------------------------------------------------------------------

    def _resolve_service_principals(self, api: Any) -> Dict[str, str]:
        """Resolve each app's ``service_principal_client_id``.

        Missing apps are skipped with a warning (mirrors the bash ``SKIP``
        path). Returns an ordered ``{app_name: sp_client_id}`` mapping.
        """
        out: Dict[str, str] = {}
        for app_name in self._app_names:
            sp_id = self._app_service_principal(api, app_name)
            if sp_id:
                out[app_name] = sp_id
            else:
                self._warnings.append(
                    f"{app_name}: could not resolve service principal "
                    f"(app may not exist) — grants skipped"
                )
        if not out:
            raise ProvisionError(
                "Could not resolve any app service principal; nothing to grant. "
                "Check the app names."
            )
        return out

    @staticmethod
    def _app_service_principal(api: Any, app_name: str) -> str:
        try:
            resp = api.do("GET", f"/api/2.0/apps/{app_name}") or {}
            return resp.get("service_principal_client_id") or ""
        except Exception as exc:  # noqa: BLE001
            logger.debug("apps get %s failed: %s", app_name, exc)
            return ""

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _validate_inputs(self) -> None:
        if not self._name:
            raise ProvisionError("Instance/project name is required")
        if not self._database:
            raise ProvisionError("Postgres database name is required")
        if not self._schema:
            raise ProvisionError("Graph schema name is required")
        if not self._pg_user:
            raise ProvisionError(
                "PGUSER is not set — required to connect to the new Lakebase "
                "instance as the app service principal"
            )
        if not self._app_names:
            raise ProvisionError("At least one app name is required for grants")

    def _api(self) -> Any:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        api = getattr(w, "api_client", None)
        if api is None or not hasattr(api, "do"):
            raise ProvisionError("Databricks SDK api_client unavailable")
        return api

    def _get_instance(self, api: Any) -> Optional[Dict[str, Any]]:
        try:
            return api.do("GET", f"/api/2.0/database/instances/{self._name}") or {}
        except Exception as exc:  # noqa: BLE001
            logger.debug("get instance %s failed: %s", self._name, exc)
            return None

    def _wait_for_db_reachable(self, max_retries: int = 15, interval_s: float = 5.0) -> None:
        """Poll until a Postgres connection to the new database succeeds.

        After the control-plane API reports the database as created, the
        Postgres layer may still be propagating it.  We retry up to
        ``max_retries`` times (default ~75 s total) before giving up and
        letting the next step surface the real error.
        """
        from back.core.graphdb.lakebase.pool import _require_psycopg

        psycopg, _ = _require_psycopg()
        for attempt in range(max_retries):
            self._check_cancelled()
            try:
                token = self._mint_token(self._api())
                kwargs = {
                    "host": self._host,
                    "port": 5432,
                    "user": self._pg_user,
                    "password": token,
                    "dbname": self._database,
                    "sslmode": "require",
                    "connect_timeout": 10,
                    "application_name": "ontobricks-provision-probe",
                }
                with psycopg.connect(autocommit=True, **kwargs):
                    return  # connection succeeded — database is ready
            except Exception as exc:  # noqa: BLE001
                msg = str(exc).lower()
                if "does not exist" in msg or "database" in msg:
                    logger.debug(
                        "DB reachability probe attempt %d/%d — not ready yet: %s",
                        attempt + 1,
                        max_retries,
                        exc,
                    )
                    time.sleep(interval_s)
                else:
                    # Unexpected error (auth, network) — stop retrying.
                    raise
        logger.warning(
            "DB %r did not become reachable after %d attempts; continuing anyway",
            self._database,
            max_retries,
        )

    def _database_exists(self, api: Any) -> bool:
        try:
            raw = (
                api.do("GET", f"/api/2.0/postgres/{self._branch_path}/databases")
                or {}
            ).get("databases") or []
        except Exception as exc:  # noqa: BLE001
            logger.debug("list databases failed: %s", exc)
            return False
        for db in raw:
            status = db.get("status") or {}
            seg = (db.get("name") or "").rsplit("/", 1)[-1]
            if status.get("postgres_database") == self._database or seg == self._database:
                return True
        return False

    def _mint_token(self, api: Any) -> str:
        resp = (
            api.do(
                "POST",
                "/api/2.0/postgres/credentials",
                body={"endpoint": self._endpoint_resource},
            )
            or {}
        )
        token = resp.get("token") or ""
        if not token:
            raise ProvisionError(
                f"Failed to mint Lakebase JWT for endpoint "
                f"{self._endpoint_resource!r}"
            )
        return token

    def _connect(self):
        """Open an autocommit psycopg connection to the new database."""
        from back.core.graphdb.lakebase.pool import _require_psycopg

        psycopg, _ = _require_psycopg()
        token = self._mint_token(self._api())
        kwargs = {
            "host": self._host,
            "port": 5432,
            "user": self._pg_user,
            "password": token,
            "dbname": self._database,
            "sslmode": "require",
            "connect_timeout": 15,
            "application_name": "ontobricks-provision",
        }
        return psycopg.connect(autocommit=True, **kwargs)

    def _check_cancelled(self) -> None:
        try:
            if self._tm.is_cancelled(self._task_id):
                raise ProvisionError("Provisioning cancelled")
        except ProvisionError:
            raise
        except Exception:  # noqa: BLE001
            pass
