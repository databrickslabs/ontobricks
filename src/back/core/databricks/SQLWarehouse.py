"""SQL Warehouse operations for Databricks.

Provides query execution, DDL operations, and warehouse management
through a SQL Warehouse endpoint.
"""
from databricks import sql
from typing import Any, Dict, List, Tuple

from back.core.logging import get_logger
from back.core.errors import ValidationError
from shared.config.constants import MSG_WAREHOUSE_ID_REQUIRED
from .DatabricksAuth import DatabricksAuth
from .constants import SQL_WAREHOUSES_PATH

logger = get_logger(__name__)


class SQLWarehouse:
    """Execute SQL against a Databricks SQL Warehouse.

    Requires a ``DatabricksAuth`` instance whose ``warehouse_id`` is set.
    """

    def __init__(self, auth: DatabricksAuth) -> None:
        self._auth = auth

    @property
    def warehouse_id(self) -> str:
        return self._auth.warehouse_id

    def _require_warehouse(self) -> None:
        if not self._auth.warehouse_id:
            raise ValidationError(MSG_WAREHOUSE_ID_REQUIRED)

    def test_connection(self) -> Tuple[bool, str]:
        """Test connectivity to the SQL Warehouse.

        Returns:
            ``(success, message)`` tuple.
        """
        if not self._auth.warehouse_id:
            return False, "Missing SQL Warehouse ID"

        if not self._auth.has_valid_auth():
            if self._auth.is_app_mode:
                return False, "Missing OAuth credentials (DATABRICKS_CLIENT_ID/SECRET)"
            return False, "Missing configuration: DATABRICKS_HOST or DATABRICKS_TOKEN"

        try:
            params = self._auth.get_sql_connection_params()
            with sql.connect(**params) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            auth_mode = "OAuth (Databricks App)" if self._auth.is_app_mode else "Personal Access Token"
            return True, f"Connection successful ({auth_mode})"
        except Exception as exc:
            return False, f"Connection failed: {exc}"

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute *query* and return rows as a list of dicts."""
        self._require_warehouse()
        try:
            params = self._auth.get_sql_connection_params()
            with sql.connect(**params) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    columns = [desc[0] for desc in cur.description]
                    return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Exception as exc:
            logger.exception("Error executing query: %s", exc)
            raise

    def execute_statement(self, statement: str) -> bool:
        """Execute a DDL/DML *statement* without returning results."""
        self._require_warehouse()
        try:
            params = self._auth.get_sql_connection_params()
            with sql.connect(**params) as conn:
                with conn.cursor() as cur:
                    cur.execute(statement)
            return True
        except Exception as exc:
            logger.exception("Error executing statement: %s", exc)
            raise

    def _create_or_replace(
        self, kind: str, catalog: str, schema: str, name: str, select_sql: str,
    ) -> Tuple[bool, str]:
        """Shared DDL wrapper for VIEW and TABLE creation."""
        fqn = f"`{catalog}`.`{schema}`.`{name}`"
        try:
            ddl = f"CREATE OR REPLACE {kind} {fqn} AS\n{select_sql}"
            logger.info("Creating %s: %s", kind.lower(), fqn)
            logger.debug("DDL length: %d chars", len(ddl))
            self.execute_statement(ddl)
            logger.info("SUCCESS: %s %s created", kind, fqn)
            return True, f"{kind} {fqn} created successfully"
        except Exception as exc:
            logger.exception("ERROR creating %s: %s", kind.lower(), exc)
            return False, f"Failed to create {kind.lower()}: {exc}"

    def create_or_replace_view(
        self, catalog: str, schema: str, view_name: str, select_sql: str
    ) -> Tuple[bool, str]:
        """``CREATE OR REPLACE VIEW`` wrapper."""
        return self._create_or_replace("VIEW", catalog, schema, view_name, select_sql)

    def create_or_replace_table_from_query(
        self, catalog: str, schema: str, table_name: str, select_sql: str
    ) -> Tuple[bool, str]:
        """``CREATE OR REPLACE TABLE ... AS SELECT`` (CTAS) wrapper."""
        return self._create_or_replace("TABLE", catalog, schema, table_name, select_sql)

    def get_warehouses(self) -> List[Dict[str, str]]:
        """List available SQL Warehouses.

        Uses the Databricks SDK in app mode, falling back to REST API.
        Returns list of dicts with ``id``, ``name``, ``state`` keys.
        """
        logger.debug("Host: %s, App mode: %s", self._auth.host, self._auth.is_app_mode)

        if self._auth.is_app_mode:
            try:
                from databricks.sdk import WorkspaceClient

                w = WorkspaceClient()
                if w is None or not hasattr(w, "warehouses"):
                    raise ValidationError("WorkspaceClient not properly initialized")
                warehouses = []
                for wh in w.warehouses.list():
                    warehouses.append({
                        "id": wh.id,
                        "name": wh.name,
                        "state": str(wh.state) if wh.state else "UNKNOWN",
                    })
                logger.info("Found %d warehouses via SDK", len(warehouses))
                return warehouses
            except AttributeError as exc:
                logger.warning("SDK HTTP client error (likely auth issue): %s", exc)
            except Exception as exc:
                logger.warning("SDK error: %s", exc)

        import requests

        if not self._auth.host:
            logger.warning("No host configured")
            return []
        if not self._auth.has_valid_auth():
            logger.warning("No valid auth")
            return []

        try:
            host = self._auth.host.rstrip("/")
            headers = self._auth.get_auth_headers()
            response = requests.get(f"{host}{SQL_WAREHOUSES_PATH}", headers=headers)
            response.raise_for_status()
            data = response.json()
            warehouses = []
            for wh in data.get("warehouses", []):
                warehouses.append({
                    "id": wh["id"],
                    "name": wh["name"],
                    "state": wh.get("state", "UNKNOWN"),
                })
            logger.info("Found %d warehouses via REST", len(warehouses))
            return warehouses
        except Exception as exc:
            logger.exception("Error fetching warehouses: %s", exc)
            if hasattr(exc, "response") and exc.response is not None:
                logger.error("Response: %s", exc.response.text)
            return []
