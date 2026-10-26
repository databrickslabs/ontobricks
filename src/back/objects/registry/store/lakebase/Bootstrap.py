"""Lakebase registry initialize, grants, and permission diagnostics.

Extracted from :class:`LakebaseRegistryStore` (Fowler Extract Class / mixin).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


from back.core.logging import get_logger

logger = get_logger("back.objects.registry.store.lakebase.store")


def _known_tables():
    from back.objects.registry.store.lakebase.store import _KNOWN_TABLES

    return _KNOWN_TABLES


def _require_psycopg():
    """Defer to the store module so tests can monkeypatch that name."""
    from back.objects.registry.store.lakebase.store import _require_psycopg as _rp

    return _rp()


class Bootstrap:
    """Schema bootstrap, USAGE diagnostics, and app GRANTs."""

    def is_initialized(self) -> bool:
        """Cheap boolean probe — silent on errors (matches base contract).

        Most callers only need a yes/no answer (e.g. *Initialize*
        button gating). Use :meth:`init_status` when you also want
        the *reason* an initialised schema looks empty (missing
        ``USAGE`` on the schema, no registry row, …) — that's what
        the admin Registry Location panel surfaces to operators.
        """
        return self.init_status()["initialized"]

    def init_status(self) -> Dict[str, Any]:
        """Detailed initialise-probe with explicit failure reasons.

        Returns ``{initialized: bool, reason: str, error: Optional[str]}``.
        ``reason`` is a short stable token (``"ok"``, ``"no_usage"``,
        ``"no_registries_table"``, ``"no_registry_row"``,
        ``"connect_failed"``) suitable for log filtering; ``error``
        is a human-readable explanation suitable for the admin UI.

        The reason ``no_usage`` is the most common silent-failure
        mode: when the app's service principal lacks ``USAGE`` on
        the registry schema, ``to_regclass`` returns NULL even
        though the tables exist and hold data — turning the panel
        into a misleading "not initialised, 0 rows everywhere"
        screen. Surfacing the explicit reason lets the operator
        run ``scripts/bootstrap-lakebase-perms.sh`` and move on
        instead of hunting for a phantom data loss.
        """
        try:
            with self._connect() as conn, conn.cursor() as cur:
                # Probe the live session context so the error message can
                # tell the operator exactly which (database, role, schema)
                # the check ran against — this is the only reliable way
                # to spot grants that landed on a different database
                # than the one the Apps ``postgres`` resource binds.
                cur.execute(
                    "SELECT current_database(), current_user, "
                    "       has_schema_privilege(current_user, %s, 'USAGE'), "
                    "       EXISTS (SELECT 1 FROM pg_namespace "
                    "               WHERE nspname = %s)",
                    (self._schema, self._schema),
                )
                row = cur.fetchone()
                if not row:
                    has_usage = False
                    cur_db = self._effective_database
                    cur_user = "?"
                    schema_exists = False
                else:
                    cur_db, cur_user, has_usage_raw, schema_exists = row
                    has_usage = bool(has_usage_raw)
                if not has_usage:
                    if schema_exists:
                        msg = (
                            f"Role '{cur_user}' lacks USAGE on schema "
                            f"'{self._schema}' in database '{cur_db}'. "
                            f"Run scripts/bootstrap-lakebase-perms.sh "
                            f"-i <instance> -d {cur_db} -s {self._schema} "
                            f"-a <app-name>, or GRANT USAGE ON SCHEMA "
                            f"\"{self._schema}\" TO \"{cur_user}\" "
                            f"directly in database '{cur_db}'."
                        )
                    else:
                        msg = (
                            f"Schema '{self._schema}' does not exist in "
                            f"database '{cur_db}' (role '{cur_user}'). "
                            f"Either initialize it from Settings > "
                            f"Registry, or check that bundle "
                            f"``lakebase_*`` Postgres binding points at "
                            f"the database where the schema actually "
                            f"lives."
                        )
                    logger.warning("Lakebase init probe: %s", msg)
                    return {
                        "initialized": False,
                        "reason": "no_usage",
                        "error": msg,
                    }
                cur.execute(
                    "SELECT to_regclass(%s) IS NOT NULL",
                    (f"{self._schema}.registries",),
                )
                ok = bool(cur.fetchone()[0])
            if not ok:
                return {
                    "initialized": False,
                    "reason": "no_registries_table",
                    "error": (
                        f"Schema '{self._schema}' has no 'registries' "
                        f"table — run *Initialize* to create it."
                    ),
                }
            if self._registry_id is None:
                self._registry_id = self._fetch_registry_id()
            if self._registry_id is None:
                return {
                    "initialized": False,
                    "reason": "no_registry_row",
                    "error": (
                        f"Schema '{self._schema}' has no registry row "
                        f"yet — run *Initialize* to seed it."
                    ),
                }
            return {"initialized": True, "reason": "ok", "error": None}
        except Exception as exc:  # noqa: BLE001
            logger.warning("Lakebase init probe failed: %s", exc)
            return {
                "initialized": False,
                "reason": "connect_failed",
                "error": f"Lakebase probe failed: {exc}",
            }

    def check_permissions(self) -> Dict[str, Any]:
        """Run a comprehensive permission diagnostic against the Lakebase registry.

        Executes two lightweight queries in a single connection:

        1. **Context + schema probe** — confirms the connection works and
           checks ``USAGE`` + ``CREATE`` on the registry schema.
        2. **Per-table privilege scan** — for every table that *exists* in
           the schema, checks ``SELECT``, ``INSERT``, ``UPDATE``, ``DELETE``.
           Tables from :data:`_KNOWN_TABLES` that are absent from the catalog
           are reported as ``"missing"`` (expected before initialization, not
           an error).

        Return shape::

            {
              "success": True,
              "database": str,
              "user": str,
              "schema": str,
              "checks": [
                {
                  "id": str,          # stable token for the UI
                  "label": str,       # human-readable label
                  "status": "ok" | "warning" | "error" | "missing",
                  "detail": str | None,
                },
                ...
              ],
            }
        """
        _require_psycopg()
        checks: list = []

        def _chk(id_: str, label: str, status: str, detail: str | None = None):
            checks.append({"id": id_, "label": label, "status": status, "detail": detail})

        try:
            with self._connect() as conn, conn.cursor() as cur:
                # ── 1. Connection + database/user context ──────────────
                cur.execute("SELECT current_database(), current_user")
                row = cur.fetchone()
                cur_db, cur_user = (row[0], row[1]) if row else (self._effective_database, "?")
                _chk("connect", "Connect to Lakebase", "ok")

                # ── 2. Schema existence + privileges ───────────────────
                cur.execute(
                    """
                    SELECT
                        EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = %s),
                        has_schema_privilege(current_user, %s, 'USAGE'),
                        has_schema_privilege(current_user, %s, 'CREATE')
                    """,
                    (self._schema, self._schema, self._schema),
                )
                row2 = cur.fetchone()
                schema_exists = bool(row2[0]) if row2 else False
                has_usage     = bool(row2[1]) if row2 else False
                has_create    = bool(row2[2]) if row2 else False

                if not schema_exists:
                    _chk(
                        "schema_exists",
                        f"Schema '{self._schema}' exists",
                        "error",
                        f"Schema '{self._schema}' not found in database '{cur_db}'. "
                        "Run *Initialize* from Settings → Registry to create it.",
                    )
                    # No point checking table privileges if the schema is absent
                    for tbl in sorted(_known_tables()):
                        _chk(f"tbl_{tbl}", f"Table: {tbl}", "missing",
                             "Schema does not exist — run Initialize first.")
                    return {
                        "success": True,
                        "database": cur_db,
                        "user": cur_user,
                        "schema": self._schema,
                        "checks": checks,
                    }

                _chk("schema_exists", f"Schema '{self._schema}' exists", "ok")
                _chk(
                    "schema_usage",
                    f"USAGE on schema '{self._schema}'",
                    "ok" if has_usage else "error",
                    None if has_usage else (
                        f"Role '{cur_user}' lacks USAGE on schema '{self._schema}' "
                        f"in database '{cur_db}'. "
                        f"Run: GRANT USAGE ON SCHEMA \"{self._schema}\" TO \"{cur_user}\";"
                    ),
                )
                _chk(
                    "schema_create",
                    f"CREATE on schema '{self._schema}'",
                    "ok" if has_create else "warning",
                    None if has_create else (
                        f"Role '{cur_user}' lacks CREATE on schema '{self._schema}' "
                        "(needed to add new registry tables on upgrade). "
                        f"Run: GRANT CREATE ON SCHEMA \"{self._schema}\" TO \"{cur_user}\";"
                    ),
                )

                # ── 3. Per-table: existence + CRUD privileges ──────────
                # Fetch all tables that actually exist in the schema
                cur.execute(
                    "SELECT relname FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = %s AND c.relkind = 'r'",
                    (self._schema,),
                )
                existing_tables = {row[0] for row in cur.fetchall()}

                for tbl in sorted(_known_tables()):
                    full = f"{self._schema}.{tbl}"
                    if tbl not in existing_tables:
                        _chk(f"tbl_{tbl}", f"Table: {tbl}", "missing",
                             "Not yet created — run *Initialize* to create all registry tables.")
                        continue
                    # Check all four DML privileges at once
                    cur.execute(
                        """
                        SELECT
                            has_table_privilege(current_user, %s, 'SELECT'),
                            has_table_privilege(current_user, %s, 'INSERT'),
                            has_table_privilege(current_user, %s, 'UPDATE'),
                            has_table_privilege(current_user, %s, 'DELETE')
                        """,
                        (full, full, full, full),
                    )
                    tp = cur.fetchone()
                    missing_privs = []
                    if tp:
                        for priv, has in zip(["SELECT", "INSERT", "UPDATE", "DELETE"], tp):
                            if not has:
                                missing_privs.append(priv)

                    if not missing_privs:
                        _chk(f"tbl_{tbl}", f"Table: {tbl}", "ok")
                    else:
                        grants = ", ".join(missing_privs)
                        _chk(
                            f"tbl_{tbl}",
                            f"Table: {tbl}",
                            "error",
                            f"Missing: {grants}. "
                            f"Run: GRANT {grants} ON TABLE \"{self._schema}\".\"{tbl}\" "
                            f"TO \"{cur_user}\";",
                        )

        except Exception as exc:
            logger.warning("check_permissions failed: %s", exc)
            if not checks:
                _chk("connect", "Connect to Lakebase", "error", str(exc))
            return {
                "success": False,
                "error": str(exc),
                "database": self._effective_database,
                "user": "?",
                "schema": self._schema,
                "checks": checks,
            }

        return {
            "success": True,
            "database": cur_db,
            "user": cur_user,
            "schema": self._schema,
            "checks": checks,
        }

    def initialize(self, *, client: Any = None) -> Tuple[bool, str]:
        """Initialize or upgrade the Lakebase registry schema.

        Idempotent: safe to re-run on an already-initialized registry.
        Creates any tables that are missing (``IF NOT EXISTS``), applies
        pending column migrations, ensures the registry identity row, and
        scrubs legacy JSONB keys.  Use this both for first-time setup and
        as an in-app upgrade step when the app is updated to a new version
        that added columns to existing tables.

        When the caller is not the table owner (enterprise pattern: a
        provisioning identity owns the schema, the app SP is DML-only),
        ``schema.sql`` is skipped with a warning and the registry row is
        still pinned. A DML failure still fails Initialize.
        """
        del client  # not used: Lakebase instance is provisioned out of band
        try:
            try:
                self._apply_ddl()
            except Exception as ddl_exc:  # noqa: BLE001
                # Pre-provisioned schemas are owned by a dedicated deploy SP.
                # The app identity is DML-only, so schema.sql (ALTER / CREATE
                # INDEX) raises "must be owner of table". That must not abort
                # the DML that pins catalog/schema/volume on the registry row.
                logger.warning(
                    "Lakebase DDL skipped (schema already provisioned or "
                    "caller is not table owner): %s",
                    ddl_exc,
                )
            self._registry_id = self._ensure_registry_row()
            self._scrub_global_config_legacy_keys()
            # Apply pending column migrations eagerly so the admin gets
            # immediate feedback from the Initialize / Upgrade button rather
            # than waiting for the first runtime call that needs the column.
            self._ensure_domain_versions_status_column()
            self._ensure_domains_review_quorum_column()
            self._ensure_domains_mcp_policy_column()
            self._ensure_schedule_task_columns()
            self._import_legacy_cohort_schedules()
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT 1")  # wake probe
            logger.info(
                "Lakebase registry initialised/upgraded (schema=%s, host=%s)",
                self._schema,
                self._auth.host,
            )
            return True, (
                f"Lakebase registry initialized/upgraded at "
                f"{self._auth.host}/{self._effective_database} "
                f"(schema={self._schema})"
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Lakebase initialise failed")
            return False, f"Failed to initialise Lakebase registry: {exc}"

    def grant_app_permissions(
        self, *, app_names: List[str], uc_catalog: str = ""
    ) -> Dict[str, Any]:
        """In-app port of ``scripts/bootstrap-lakebase-perms.sh`` (registry schema).

        Runs as the app's own service principal, which **owns** the
        registry schema after *Initialize* and can therefore ``GRANT`` to
        the other app service principals (e.g. the MCP companion). Applies,
        idempotently and best-effort (mirroring the bash script):

        - ``CAN_USE`` on the Lakebase project (control-plane; needs manage
          on the project),
        - ``USAGE``/``CREATE``/DML + default privileges on the registry
          schema (data-plane; needs schema ownership — which the SP has),
        - ``ALL_PRIVILEGES`` on the Unity Catalog *uc_catalog* when set
          (needs ``MANAGE`` on the catalog).

        Returns ``{success, granted: [...], warnings: [...], error,
        schema, apps}``. Control-plane failures degrade to warnings rather
        than aborting, so the schema grants (the part the SP can always do)
        still apply.
        """
        from back.core.databricks.lakebase.grants import (  # noqa: PLC0415
            grant_can_use_on_project,
            grant_schema_privileges,
            grant_uc_catalog,
            resolve_app_service_principals,
        )

        try:
            from databricks.sdk import WorkspaceClient  # noqa: PLC0415

            api = getattr(WorkspaceClient(), "api_client", None)
        except Exception as exc:  # noqa: BLE001
            return {
                "success": False,
                "granted": [],
                "warnings": [],
                "error": f"Databricks SDK unavailable: {exc}",
            }
        if api is None or not hasattr(api, "do"):
            return {
                "success": False,
                "granted": [],
                "warnings": [],
                "error": "Databricks api_client unavailable",
            }

        sp_ids, warnings = resolve_app_service_principals(api, app_names)
        granted: List[str] = []
        if not sp_ids:
            return {
                "success": False,
                "granted": granted,
                "warnings": warnings,
                "error": (
                    "Could not resolve any app service principal to grant — "
                    "check the app name(s)."
                ),
            }

        # ── CAN_USE on the Lakebase project (control-plane) ──────────────
        try:
            project_short = self._auth.instance_name
        except Exception as exc:  # noqa: BLE001
            project_short = ""
            warnings.append(
                f"Could not resolve the Lakebase project for the CAN_USE "
                f"grant ({exc}); skipped."
            )
        if project_short:
            g, w = grant_can_use_on_project(api, project_short, sp_ids)
            granted.extend(g)
            warnings.extend(w)

        # ── Postgres schema grants (we own the schema) ───────────────────
        try:
            with self._connect() as conn:
                g, w = grant_schema_privileges(conn, self._schema, sp_ids)
            granted.extend(g)
            warnings.extend(w)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Registry schema grants failed to run: %s", exc)
            warnings.append(f"Schema grants could not run ({exc}).")

        # ── Unity Catalog ALL_PRIVILEGES (managed_synced readback) ───────
        if uc_catalog:
            g, w = grant_uc_catalog(api, uc_catalog, sp_ids)
            granted.extend(g)
            warnings.extend(w)

        return {
            "success": True,
            "granted": granted,
            "warnings": warnings,
            "error": None,
            "schema": self._schema,
            "apps": list(sp_ids.keys()),
        }

    def _ensure_registry_row(self) -> str:
        c = self._cfg
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._q(self._schema)}.registries
                    (name, catalog, schema, volume)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (name)
                DO UPDATE SET catalog    = EXCLUDED.catalog,
                              schema     = EXCLUDED.schema,
                              volume     = EXCLUDED.volume,
                              updated_at = now()
                RETURNING id
                """,
                (self._registry_name(), c.catalog, c.schema, c.volume),
            )
            row = cur.fetchone()
        return str(row[0])
