"""First-run setup of the registry location, run by an admin from Settings."""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List

from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.apps import AppAccessControlRequest, AppPermissionLevel
from databricks.sdk.service.sql import StatementState

from back.core.errors import AuthorizationError, InfrastructureError, ValidationError
from back.core.helpers import SQLHelpers
from back.core.logging import get_logger

logger = get_logger(__name__)

#: UC privileges the app's service principal needs on the registry schema:
#: Delta views and tables, the ``OntoBricksRegistry`` volume, and its files.
SCHEMA_PRIVILEGES = (
    "USE SCHEMA",
    "CREATE TABLE",
    "CREATE VOLUME",
    "SELECT",
    "MODIFY",
    "READ VOLUME",
    "WRITE VOLUME",
)


class RegistrySetupService:
    """Prepare (or dry-run check) a registry schema for the app's service principal, as the calling admin.

    A Marketplace install has no deploy step to run the bootstrap scripts, and
    the service principal cannot grant itself anything, so the first admin does
    it from Settings → Registry. Every call uses the admin's forwarded token:
    the ``apps`` user scope for the app grant, the ``sql`` scope for the DDL.
    """

    _POLL_INTERVAL_S = 2.0
    _POLL_LIMIT_S = 300.0

    def __init__(
        self,
        workspace_client: Any,
        *,
        warehouse_id: str,
        app_name: str,
        sp_client_id: str,
        user_email: str = "",
    ):
        self._w = workspace_client
        self._warehouse_id = warehouse_id
        self._app_name = app_name
        self._sp = sp_client_id
        self._me = user_email

    @classmethod
    def for_user(
        cls, host: str, user_token: str, warehouse_id: str, app_name: str, user_email: str = ""
    ) -> RegistrySetupService:
        """Build the service on the caller's token, for this app's service principal."""
        from databricks.sdk import WorkspaceClient

        sp_client_id = os.getenv("DATABRICKS_CLIENT_ID", "").strip()
        if not sp_client_id:
            raise InfrastructureError(
                "Cannot tell which service principal runs this app: DATABRICKS_CLIENT_ID is not set."
            )
        # auth_type="pat": the app's own DATABRICKS_CLIENT_ID/SECRET are in the
        # environment too, and the SDK refuses two auth methods at once.
        client = WorkspaceClient(host=host, token=user_token, auth_type="pat")
        return cls(
            client,
            warehouse_id=warehouse_id,
            app_name=app_name,
            sp_client_id=sp_client_id,
            user_email=user_email,
        )

    def prepare(self, catalog: str, schema: str) -> Dict[str, List[str]]:
        """Create ``catalog.schema`` and grant the service principal what it needs.

        Returns ``{"granted": [...], "warnings": [...]}``. A failed app grant
        is a warning (the admin can do it by hand); a failed SQL step raises.
        """
        catalog_q = SQLHelpers.quote_uc_identifier(catalog, role="catalog")
        schema_q = SQLHelpers.quote_uc_identifier(schema, role="schema")
        sp_q = SQLHelpers.quote_uc_identifier(self._sp, role="service principal")
        granted: List[str] = []
        warnings: List[str] = []
        self._grant_app_manage(granted, warnings)
        for statement in (
            f"CREATE SCHEMA IF NOT EXISTS {catalog_q}.{schema_q}",
            f"GRANT USE CATALOG ON CATALOG {catalog_q} TO {sp_q}",
            f"GRANT {', '.join(SCHEMA_PRIVILEGES)} ON SCHEMA {catalog_q}.{schema_q} TO {sp_q}",
        ):
            self._run_sql(statement)
        granted.append(f"Schema {catalog}.{schema} is ready")
        granted.append(f"App service principal: USE CATALOG on {catalog}, {', '.join(SCHEMA_PRIVILEGES)} on {catalog}.{schema}")
        return {"granted": granted, "warnings": warnings}

    # -- preflight (read-only) ------------------------------------------

    def preflight(self, catalog: str, schema: str) -> List[Dict[str, str]]:
        """Check, as the caller, everything :meth:`prepare` needs. Changes nothing.

        Each check is ``{name, label, status, detail, fix}`` with status
        ``ok`` / ``warning`` / ``error`` / ``skipped``.
        """
        checks = self._check_app_permissions()
        warehouse = self._check_warehouse()
        checks.append(warehouse)
        if warehouse["status"] != "ok":
            reason = "Skipped: you can't run SQL on the app's warehouse yet."
            checks.append(self._check("you.catalog", f"You can use catalog {catalog}", "skipped", reason))
            checks.append(self._check("you.schema", f"Schema {catalog}.{schema}", "skipped", reason))
            return checks
        try:
            catalog_q = SQLHelpers.quote_uc_identifier(catalog, role="catalog")
            schema_q = SQLHelpers.quote_uc_identifier(schema, role="schema")
        except ValidationError as exc:
            checks.append(self._check("you.catalog", "Catalog and schema names", "error", f"Invalid name: {exc}",
                                 "Use letters, digits, _ and - only."))
            checks.append(self._check("you.schema", f"Schema {catalog}.{schema}", "skipped", "Skipped: invalid name."))
            return checks
        catalog_check = self._check_catalog(catalog, catalog_q)
        checks.append(catalog_check)
        if catalog_check["status"] != "ok":
            checks.append(self._check("you.schema", f"Schema {catalog}.{schema}", "skipped",
                                      "Skipped: fix catalog access first."))
            return checks
        checks.append(self._check_schema(catalog, schema, f"{catalog_q}.{schema_q}"))
        return checks

    def _check_app_permissions(self) -> List[Dict[str, str]]:
        """Can the caller read the app's ACL, and does the SP have Can Manage on it?"""
        label = "You can read this app's permissions (`apps` user scope)"
        manage_label = "App service principal has Can Manage on this app"
        try:
            acl = self._w.apps.get_permissions(self._app_name)
        except DatabricksError as exc:
            return [
                self._check("you.app_permissions", label, "error", str(exc),
                       f"The app needs the `apps` user scope, and you need Can Manage on {self._app_name}. "
                       f"Or add the app's service principal ({self._sp}) yourself under Apps → "
                       f"{self._app_name} → Permissions → Can Manage."),
                self._check("app.can_manage", manage_label, "skipped", "Skipped: the app's permissions are unreadable."),
            ]
        has_manage = any(
            (entry.service_principal_name or "") == self._sp
            and any(getattr(p.permission_level, "value", p.permission_level) == "CAN_MANAGE"
                    for p in (entry.all_permissions or []))
            for entry in (acl.access_control_list or [])
        )
        manage = (
            self._check("app.can_manage", manage_label, "ok", f"{self._sp} has Can Manage.")
            if has_manage
            else self._check("app.can_manage", manage_label, "warning",
                        f"{self._sp} doesn't have Can Manage yet, so non-admin users are denied.",
                        f"Setup grants it. Or add it yourself under Apps → {self._app_name} → Permissions.")
        )
        return [self._check("you.app_permissions", label, "ok", "Readable with your token."), manage]

    def _check_warehouse(self) -> Dict[str, str]:
        label = "You can run SQL on the app's warehouse (`sql` user scope)"
        ok, message = self._probe("SELECT 1")
        if ok:
            return self._check("you.warehouse", label, "ok", f"Warehouse {self._warehouse_id} answered.")
        return self._check("you.warehouse", label, "error", message,
                      f"You need Can use on SQL warehouse {self._warehouse_id} (SQL warehouses → Permissions), "
                      "and the app needs the `sql` user scope.")

    def _check_catalog(self, catalog: str, catalog_q: str) -> Dict[str, str]:
        label = f"You can use catalog {catalog}"
        ok, message = self._probe(f"DESCRIBE CATALOG {catalog_q}")
        if ok:
            return self._check("you.catalog", label, "ok", "Visible to you. CREATE SCHEMA is checked when setup runs.")
        if self._is_not_found(message):
            return self._check("you.catalog", label, "error", message,
                          "Check the catalog name, or pick one you can see in Catalog Explorer.")
        return self._check("you.catalog", label, "error", message,
                      f"Ask the catalog's owner to run: GRANT USE CATALOG, CREATE SCHEMA ON CATALOG "
                      f"`{catalog}` TO `{self._me or '<your user>'}`")

    def _check_schema(self, catalog: str, schema: str, fqn: str) -> Dict[str, str]:
        label = f"Schema {catalog}.{schema}"
        ok, message, rows = self._probe_rows(f"DESCRIBE SCHEMA EXTENDED {fqn}")
        if not ok:
            if self._is_not_found(message):
                return self._check("you.schema", label, "ok",
                              "Doesn't exist yet: setup creates it (needs CREATE SCHEMA on the catalog).")
            return self._check("you.schema", label, "error", message,
                          f"Ask the schema's owner to grant you MANAGE on {catalog}.{schema}, or choose a new name.")
        owner = next((str(r[1]) for r in rows if r and str(r[0]).strip().lower() == "owner"), "")
        if owner and self._me and owner.lower() != self._me.lower():
            return self._check("you.schema", label, "warning", f"Exists, owned by {owner}.",
                          f"Granting on it needs ownership or MANAGE. Ask {owner}, or choose a new schema name.")
        return self._check("you.schema", label, "ok", f"Exists{', owned by ' + owner if owner else ''}.")

    def _probe(self, statement: str) -> tuple:
        ok, message, _rows = self._probe_rows(statement)
        return ok, message

    def _probe_rows(self, statement: str) -> tuple:
        """Run a read-only *statement*; return ``(ok, error message, rows)`` without raising."""
        try:
            resp = self._execute(statement)
        except (DatabricksError, InfrastructureError) as exc:
            return False, str(exc), []
        if resp.status.state == StatementState.SUCCEEDED:
            return True, "", list(getattr(getattr(resp, "result", None), "data_array", None) or [])
        return False, self._error_message(resp), []

    # -- statement execution ---------------------------------------------

    def _grant_app_manage(self, granted: List[str], warnings: List[str]) -> None:
        """Give the service principal Can Manage on its own app (PLAN.md Q4)."""
        try:
            self._w.apps.update_permissions(
                self._app_name,
                access_control_list=[
                    AppAccessControlRequest(
                        service_principal_name=self._sp,
                        permission_level=AppPermissionLevel.CAN_MANAGE,
                    )
                ],
            )
        except DatabricksError as exc:
            logger.warning("Could not grant the app service principal Can Manage on %s: %s", self._app_name, exc)
            warnings.append(
                f"Could not give the app's service principal Can Manage on {self._app_name} ({exc}). "
                "Add it under Apps → Permissions, or non-admin users will be denied."
            )
            return
        granted.append(f"App service principal: Can Manage on {self._app_name}")

    def _execute(self, statement: str) -> Any:
        """Run *statement* on the app's warehouse as the caller and wait for a final state."""
        sql = self._w.statement_execution
        resp = sql.execute_statement(statement=statement, warehouse_id=self._warehouse_id, wait_timeout="50s")
        deadline = time.monotonic() + self._POLL_LIMIT_S
        while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
            if time.monotonic() > deadline:
                sql.cancel_execution(resp.statement_id)
                raise InfrastructureError("The SQL warehouse did not finish in time", detail=statement)
            time.sleep(self._POLL_INTERVAL_S)
            resp = sql.get_statement(resp.statement_id)
        return resp

    def _run_sql(self, statement: str) -> None:
        """Run *statement* as the caller; raise unless it succeeds."""
        resp = self._execute(statement)
        if resp.status.state == StatementState.SUCCEEDED:
            return
        message = self._error_message(resp).rstrip(". ")
        hint = " Use Check prerequisites to see which privilege is missing."
        upper = message.upper()
        if any(word in upper for word in ("PERMISSION", "PRIVILEGE", "NOT AUTHORIZED")):
            raise AuthorizationError(f"You lack a privilege the registry setup needs ({statement}): {message}.{hint}")
        if self._is_not_found(message):
            raise ValidationError(f"Check the catalog name: {message}")
        raise InfrastructureError("Registry setup SQL failed", detail=f"{statement}: {message}")


    @staticmethod
    def _check(name: str, label: str, status: str, detail: str = "", fix: str = "") -> Dict[str, str]:
        return {"name": name, "label": label, "status": status, "detail": detail, "fix": fix}

    @staticmethod
    def _error_message(resp: Any) -> str:
        return (resp.status.error.message if resp.status.error else "") or str(resp.status.state)

    @staticmethod
    def _is_not_found(message: str) -> bool:
        upper = message.upper()
        return any(word in upper for word in ("NOT FOUND", "NOT_FOUND", "NO_SUCH", "DOES NOT EXIST"))
