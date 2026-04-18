"""Unity Catalog metadata browsing.

Provides catalogue / schema / table / column / volume discovery using
both the SQL connector (for metadata queries) and the REST API (for
volume management).
"""

import requests
from databricks import sql
from typing import Dict, List

from back.core.logging import get_logger
from back.core.errors import ValidationError
from shared.config.constants import MSG_WAREHOUSE_ID_REQUIRED
from .DatabricksAuth import DatabricksAuth

logger = get_logger(__name__)


class UnityCatalog:
    """Browse Unity Catalog objects (catalogs, schemas, tables, columns, volumes).

    Metadata queries go through the SQL Warehouse; volume CRUD uses the
    Unity Catalog REST API.
    """

    def __init__(self, auth: DatabricksAuth) -> None:
        self._auth = auth

    def _require_warehouse(self) -> None:
        if not self._auth.warehouse_id:
            raise ValidationError(MSG_WAREHOUSE_ID_REQUIRED)

    def get_catalogs(self) -> List[str]:
        """Return the names of all accessible catalogs."""
        self._require_warehouse()
        try:
            logger.info(
                "Connecting — host=%s, warehouse=%s, app_mode=%s",
                self._auth.host,
                self._auth.warehouse_id,
                self._auth.is_app_mode,
            )
            params = self._auth.get_sql_connection_params()
            with sql.connect(**params) as conn:
                with conn.cursor() as cur:
                    cur.execute("SHOW CATALOGS")
                    catalogs = [row[0] for row in cur.fetchall()]
                    logger.info("Found %d catalogs", len(catalogs))
                    return catalogs
        except Exception as exc:
            logger.exception("Error fetching catalogs: %s", exc)
            raise

    def get_schemas(self, catalog: str) -> List[str]:
        """Return schema names within *catalog*."""
        self._require_warehouse()
        try:
            params = self._auth.get_sql_connection_params()
            with sql.connect(**params) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SHOW SCHEMAS IN {catalog}")
                    return [row[0] for row in cur.fetchall()]
        except Exception as exc:
            logger.exception("Error fetching schemas: %s", exc)
            raise

    def get_tables(self, catalog: str, schema: str) -> List[str]:
        """Return table names within *catalog*.*schema*.

        Returns an empty list on error (callers rely on this for graceful
        degradation).
        """
        try:
            params = self._auth.get_sql_connection_params()
            with sql.connect(**params) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SHOW TABLES IN {catalog}.{schema}")
                    return [row[1] for row in cur.fetchall()]
        except Exception as exc:
            logger.exception("Error fetching tables: %s", exc)
            return []

    def get_table_columns(
        self, catalog: str, schema: str, table: str
    ) -> List[Dict[str, str]]:
        """Return column metadata for *catalog*.*schema*.*table*.

        Each dict has ``name``, ``type``, and ``comment`` keys.
        Returns an empty list on error (callers rely on this for graceful
        degradation).
        """
        try:
            params = self._auth.get_sql_connection_params()
            with sql.connect(**params) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"DESCRIBE {catalog}.{schema}.{table}")
                    columns = []
                    for row in cur.fetchall():
                        columns.append(
                            {
                                "name": row[0],
                                "type": row[1],
                                "comment": row[2] if len(row) > 2 and row[2] else "",
                            }
                        )
                    return columns
        except Exception as exc:
            logger.exception("Error fetching table columns: %s", exc)
            return []

    def get_table_comment(self, catalog: str, schema: str, table: str) -> str:
        """Return the table-level comment (empty string if none)."""
        try:
            params = self._auth.get_sql_connection_params()
            with sql.connect(**params) as conn:
                with conn.cursor() as cur:
                    query = (
                        f"SELECT comment FROM {catalog}.information_schema.tables "
                        f"WHERE table_catalog = '{catalog}' "
                        f"AND table_schema = '{schema}' "
                        f"AND table_name = '{table}'"
                    )
                    cur.execute(query)
                    row = cur.fetchone()
                    return row[0] if row and row[0] else ""
        except Exception as exc:
            logger.exception("Error fetching table comment: %s", exc)
            return ""

    def get_volumes(self, catalog: str, schema: str) -> List[str]:
        """Return volume names via ``SHOW VOLUMES``."""
        self._require_warehouse()
        try:
            params = self._auth.get_sql_connection_params()
            with sql.connect(**params) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SHOW VOLUMES IN {catalog}.{schema}")
                    return [row[1] for row in cur.fetchall()]
        except Exception as exc:
            logger.exception("Error fetching volumes: %s", exc)
            raise

    def list_volumes(self, catalog: str, schema: str) -> List[str]:
        """Return volume names via the Unity Catalog REST API."""
        if not self._auth.host or not self._auth.has_valid_auth():
            return []

        try:
            host = self._auth.host.rstrip("/")
            headers = self._auth.get_auth_headers()
            url = f"{host}/api/2.1/unity-catalog/volumes"
            params = {"catalog_name": catalog, "schema_name": schema}
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            volumes = response.json().get("volumes", [])
            return [v.get("name") for v in volumes if v.get("name")]
        except Exception as exc:
            logger.exception("Error listing volumes: %s", exc)
            return []

    def create_volume(self, catalog: str, schema: str, volume_name: str) -> bool:
        """Create a managed volume via the Unity Catalog REST API."""
        if not self._auth.host or not self._auth.has_valid_auth():
            return False

        try:
            host = self._auth.host.rstrip("/")
            headers = self._auth.get_auth_headers()
            headers["Content-Type"] = "application/json"
            url = f"{host}/api/2.1/unity-catalog/volumes"
            payload = {
                "catalog_name": catalog,
                "schema_name": schema,
                "name": volume_name,
                "volume_type": "MANAGED",
                "comment": f"OntoBricks domain volume: {volume_name}",
            }
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            logger.info("Created volume: %s.%s.%s", catalog, schema, volume_name)
            return True
        except Exception as exc:
            logger.exception("Error creating volume: %s", exc)
            return False
