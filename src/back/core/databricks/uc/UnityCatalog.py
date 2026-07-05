"""Unity Catalog metadata browsing.

Provides catalogue / schema / table / column / volume discovery using
both the SQL connector (for metadata queries) and the REST API (for
volume management).
"""

import requests
from databricks import sql
from typing import Any, Dict, List

from back.core.logging import get_logger
from back.core.errors import ValidationError
from shared.config.constants import MSG_WAREHOUSE_ID_REQUIRED

from ..DatabricksAuth import DatabricksAuth
from .identifiers import (
    quote_uc_fqn,
    quote_uc_identifier,
    validate_uc_identifier,
)

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
        catalog_q = quote_uc_identifier(catalog, role="catalog")
        try:
            params = self._auth.get_sql_connection_params()
            with sql.connect(**params) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SHOW SCHEMAS IN {catalog_q}")
                    return [row[0] for row in cur.fetchall()]
        except Exception as exc:
            logger.exception("Error fetching schemas: %s", exc)
            raise

    def get_tables(self, catalog: str, schema: str) -> List[str]:
        """Return table names within *catalog*.*schema*.

        Returns an empty list on error (callers rely on this for graceful
        degradation).
        """
        fqn = quote_uc_fqn(catalog, schema)
        try:
            params = self._auth.get_sql_connection_params()
            with sql.connect(**params) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SHOW TABLES IN {fqn}")
                    return [row[1] for row in cur.fetchall()]
        except Exception as exc:
            logger.exception("Error fetching tables: %s", exc)
            return []

    def probe_schema_has_tables(self, catalog: str, schema: str) -> int:
        """Return the number of tables in *catalog*.*schema* via information_schema.

        Requires only USE SCHEMA — works even when SHOW TABLES returns empty
        due to missing SELECT grants on individual tables.  Returns -1 on error.
        """
        catalog_q = quote_uc_identifier(catalog, role="catalog")
        schema_val = validate_uc_identifier(schema, role="schema")
        try:
            params = self._auth.get_sql_connection_params()
            with sql.connect(**params) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT count(*) FROM {catalog_q}.information_schema.tables "
                        "WHERE table_schema = %s AND table_type = 'BASE TABLE'",
                        (schema_val,),
                    )
                    row = cur.fetchone()
                    return int(row[0]) if row else 0
        except Exception as exc:
            logger.warning("probe_schema_has_tables failed for %s.%s: %s", catalog, schema, exc)
            return -1

    def check_table_select_permission(
        self, catalog: str, schema: str, table: str
    ) -> Dict[str, Any]:
        """Probe whether the caller can SELECT from *catalog*.*schema*.*table*.

        Runs ``SELECT * … LIMIT 0`` — cheap, no data returned, but sufficient
        to confirm row-level read access.

        Returns:
        - ``can_select`` (bool): True when the query succeeds
        - ``error`` (str | None): human-readable reason when can_select is False
        """
        fqn = quote_uc_fqn(catalog, schema, table)
        try:
            params = self._auth.get_sql_connection_params()
            with sql.connect(**params) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT * FROM {fqn} LIMIT 0")
            return {"can_select": True, "error": None}
        except Exception as exc:
            logger.info(
                "SELECT probe failed for %s.%s.%s: %s", catalog, schema, table, exc
            )
            return {"can_select": False, "error": str(exc)}

    def get_table_columns(
        self, catalog: str, schema: str, table: str
    ) -> List[Dict[str, str]]:
        """Return column metadata for *catalog*.*schema*.*table*.

        Each dict has ``name``, ``type``, and ``comment`` keys.
        Returns an empty list on error (callers rely on this for graceful
        degradation).
        """
        fqn = quote_uc_fqn(catalog, schema, table)
        try:
            params = self._auth.get_sql_connection_params()
            with sql.connect(**params) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"DESCRIBE {fqn}")
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
        catalog_q = quote_uc_identifier(catalog, role="catalog")
        catalog_val = validate_uc_identifier(catalog, role="catalog")
        schema_val = validate_uc_identifier(schema, role="schema")
        table_val = validate_uc_identifier(table, role="table")
        try:
            params = self._auth.get_sql_connection_params()
            with sql.connect(**params) as conn:
                with conn.cursor() as cur:
                    query = (
                        f"SELECT comment FROM {catalog_q}.information_schema.tables "
                        "WHERE table_catalog = %s "
                        "AND table_schema = %s "
                        "AND table_name = %s"
                    )
                    cur.execute(query, (catalog_val, schema_val, table_val))
                    row = cur.fetchone()
                    return row[0] if row and row[0] else ""
        except Exception as exc:
            logger.exception("Error fetching table comment: %s", exc)
            return ""

    def get_volumes(self, catalog: str, schema: str) -> List[str]:
        """Return volume names via ``SHOW VOLUMES``."""
        self._require_warehouse()
        fqn = quote_uc_fqn(catalog, schema)
        try:
            params = self._auth.get_sql_connection_params()
            with sql.connect(**params) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SHOW VOLUMES IN {fqn}")
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

    def check_schema_access(self, catalog: str, schema: str) -> Dict[str, Any]:
        """Check whether *catalog*.*schema* exists and the caller has USE SCHEMA on it.

        Uses the Unity Catalog REST API — no warehouse required.

        Returns a dict with:
        - ``exists`` (bool | None): True = found, False = not found / auth issue, None = unknown
        - ``accessible`` (bool): True when the app has at least USE SCHEMA
        - ``error`` (str | None): human-readable reason when accessible is False
        """
        if not self._auth.host or not self._auth.has_valid_auth():
            return {"exists": None, "accessible": False, "error": "Not authenticated"}
        try:
            host = self._auth.host.rstrip("/")
            headers = self._auth.get_auth_headers()
            url = f"{host}/api/2.1/unity-catalog/schemas/{catalog}.{schema}"
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 404:
                return {"exists": False, "accessible": False, "error": "Schema not found in Unity Catalog"}
            if response.status_code == 403:
                return {"exists": True, "accessible": False, "error": "Insufficient privileges — grant USE SCHEMA to the app service principal"}
            response.raise_for_status()
            return {"exists": True, "accessible": True, "error": None}
        except requests.exceptions.RequestException as exc:
            logger.warning("check_schema_access failed for %s.%s: %s", catalog, schema, exc)
            return {"exists": None, "accessible": False, "error": str(exc)}

    def check_volume_access(self, catalog: str, schema: str, volume: str) -> Dict[str, Any]:
        """Check whether *catalog*.*schema*.*volume* exists and the caller can read it.

        Uses the Unity Catalog REST API — no warehouse required.

        Returns a dict with:
        - ``exists`` (bool | None): True = found, False = not found, None = unknown
        - ``accessible`` (bool): True when READ VOLUME is granted
        - ``error`` (str | None): human-readable reason when accessible is False
        - ``volume_type`` (str): MANAGED or EXTERNAL when found
        """
        if not self._auth.host or not self._auth.has_valid_auth():
            return {"exists": None, "accessible": False, "error": "Not authenticated"}
        try:
            host = self._auth.host.rstrip("/")
            headers = self._auth.get_auth_headers()
            url = f"{host}/api/2.1/unity-catalog/volumes/{catalog}.{schema}.{volume}"
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 404:
                return {"exists": False, "accessible": False, "error": "Volume not found — it may not have been created yet"}
            if response.status_code == 403:
                return {"exists": True, "accessible": False, "error": "Insufficient privileges — grant READ VOLUME (and WRITE VOLUME) to the app service principal"}
            response.raise_for_status()
            vol_info = response.json()
            return {
                "exists": True,
                "accessible": True,
                "error": None,
                "volume_type": vol_info.get("volume_type", "MANAGED"),
            }
        except requests.exceptions.RequestException as exc:
            logger.warning("check_volume_access failed for %s.%s.%s: %s", catalog, schema, volume, exc)
            return {"exists": None, "accessible": False, "error": str(exc)}

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
