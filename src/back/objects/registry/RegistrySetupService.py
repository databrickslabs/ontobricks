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
    """Prepare a registry schema for the app's service principal, as the calling admin.

    A Marketplace install has no deploy step to run the bootstrap scripts, and
    the service principal cannot grant itself anything, so the first admin does
    it from Settings → Registry. Every call uses the admin's forwarded token:
    the ``apps`` user scope for the app grant, the ``sql`` scope for the DDL.
    """

    _POLL_INTERVAL_S = 2.0
    _POLL_LIMIT_S = 300.0

    def __init__(self, workspace_client: Any, *, warehouse_id: str, app_name: str, sp_client_id: str):
        self._w = workspace_client
        self._warehouse_id = warehouse_id
        self._app_name = app_name
        self._sp = sp_client_id

    @classmethod
    def for_user(cls, host: str, user_token: str, warehouse_id: str, app_name: str) -> RegistrySetupService:
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
        return cls(client, warehouse_id=warehouse_id, app_name=app_name, sp_client_id=sp_client_id)

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

    def _run_sql(self, statement: str) -> None:
        """Run *statement* on the app's warehouse as the caller; raise unless it succeeds."""
        sql = self._w.statement_execution
        resp = sql.execute_statement(statement=statement, warehouse_id=self._warehouse_id, wait_timeout="50s")
        deadline = time.monotonic() + self._POLL_LIMIT_S
        while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
            if time.monotonic() > deadline:
                sql.cancel_execution(resp.statement_id)
                raise InfrastructureError("The SQL warehouse did not finish in time", detail=statement)
            time.sleep(self._POLL_INTERVAL_S)
            resp = sql.get_statement(resp.statement_id)
        if resp.status.state == StatementState.SUCCEEDED:
            return
        message = (resp.status.error.message if resp.status.error else "") or str(resp.status.state)
        upper = message.upper()
        if any(word in upper for word in ("PERMISSION", "PRIVILEGE", "NOT AUTHORIZED")):
            raise AuthorizationError(f"You lack a privilege the registry setup needs: {message}")
        if "NOT FOUND" in upper or "NO_SUCH" in upper or "DOES NOT EXIST" in upper:
            raise ValidationError(f"Check the catalog name: {message}")
        raise InfrastructureError("Registry setup SQL failed", detail=f"{statement}: {message}")
