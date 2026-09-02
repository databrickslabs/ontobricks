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

    @staticmethod
    def _normalize_privilege_name(value: Any) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        return raw.replace("_", " ").upper()

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

    def list_tables_and_views(
        self, catalog: str, schema: str
    ) -> List[Dict[str, str]]:
        """Return tables and views in *catalog*.*schema* via the UC REST API.

        Unlike ``get_tables`` (SQL ``SHOW TABLES``), the REST endpoint reports
        each asset's ``table_type`` so callers can distinguish views from
        tables. No warehouse required. Returns an empty list on error.

        Each dict has ``name``, ``full_name``, ``table_type`` and ``comment``.
        """
        if not self._auth.host or not self._auth.has_valid_auth():
            return []
        try:
            host = self._auth.host.rstrip("/")
            headers = self._auth.get_auth_headers()
            url = f"{host}/api/2.1/unity-catalog/tables"
            params = {"catalog_name": catalog, "schema_name": schema}
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            raw = response.json().get("tables", []) or []
            assets: List[Dict[str, str]] = []
            for tbl in raw:
                name = (tbl.get("name") or "").strip()
                if not name:
                    continue
                assets.append(
                    {
                        "name": name,
                        "full_name": (
                            tbl.get("full_name")
                            or f"{catalog}.{schema}.{name}"
                        ).strip(),
                        "table_type": str(tbl.get("table_type", "") or ""),
                        "comment": tbl.get("comment", "") or "",
                    }
                )
            return assets
        except Exception as exc:
            logger.exception("Error listing tables and views: %s", exc)
            return []

    def list_functions(self, catalog: str, schema: str) -> List[Dict[str, Any]]:
        """Return user-defined functions in *catalog*.*schema* with their parameters.

        Uses ``information_schema`` rather than the UC REST listing: the REST
        ``/functions`` collection endpoint omits ``input_params`` (it is only
        populated when fetching a single function by name), and callers need
        the parameter count to tell which functions are bindable as class
        actions. Returns an empty list on error.

        Each dict has ``name``, ``full_name``, ``comment``, ``input_params``
        (parameter names in declaration order), ``param_count``,
        ``returns_table`` (True for table-valued functions), ``return_type``
        and ``return_columns``: the ``RETURNS TABLE`` result columns in
        declaration order, each ``{"name", "data_type"}``. A scalar function
        has an empty ``return_columns`` and its type in ``return_type``.
        """
        catalog_q = quote_uc_identifier(catalog, role="catalog")
        try:
            validate_uc_identifier(schema, role="schema")
        except ValidationError:
            logger.warning("list_functions: invalid schema %r", schema)
            return []
        # parameter_mode = 'IN' excludes the RETURNS TABLE result columns,
        # which information_schema also reports as parameters.
        query = f"""
            SELECT r.routine_name, r.data_type, r.comment,
                   CONCAT_WS(',', ARRAY_SORT(COLLECT_LIST(p.parameter_name))) AS params
            FROM {catalog_q}.information_schema.routines r
            LEFT JOIN {catalog_q}.information_schema.parameters p
                   ON  p.specific_catalog = r.specific_catalog
                   AND p.specific_schema  = r.specific_schema
                   AND p.specific_name    = r.specific_name
                   AND p.parameter_mode   = 'IN'
            WHERE r.routine_schema = ?
            GROUP BY r.routine_name, r.data_type, r.comment
            ORDER BY r.routine_name
        """
        try:
            params = self._auth.get_sql_connection_params()
            with sql.connect(**params) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (schema,))
                    rows = cur.fetchall()
                    return_columns = self._fetch_return_columns(
                        cur, catalog_q, schema
                    )
        except Exception as exc:
            logger.exception("Error listing functions: %s", exc)
            return []

        functions: List[Dict[str, Any]] = []
        for row in rows:
            name = (row[0] or "").strip()
            if not name:
                continue
            input_params = [p for p in (row[3] or "").split(",") if p]
            data_type = str(row[1] or "").strip()
            returns_table = data_type.upper() == "TABLE_TYPE"
            functions.append(
                {
                    "name": name,
                    "full_name": f"{catalog}.{schema}.{name}",
                    "comment": row[2] or "",
                    "input_params": input_params,
                    "param_count": len(input_params),
                    "returns_table": returns_table,
                    "return_type": "TABLE" if returns_table else data_type,
                    "return_columns": (
                        return_columns.get(name, []) if returns_table else []
                    ),
                }
            )
        return functions

    @staticmethod
    def _fetch_return_columns(
        cur: Any, catalog_q: str, schema: str
    ) -> Dict[str, List[Dict[str, str]]]:
        """Return the ``RETURNS TABLE`` columns of each function in *schema*.

        Result columns live in ``routine_columns``, not in ``parameters``:
        Databricks only lists the declared arguments there, so a table function
        contributes no row for what it returns. Hence a second statement, which
        also avoids the row multiplication a single join of arguments and
        result columns would cause.

        Soft-fails to an empty mapping: the action picker only needs the input
        parameter count, so a metastore that cannot answer this must not cost
        the caller its function list.
        """
        query = f"""
            SELECT r.routine_name, c.column_name, c.full_data_type
            FROM {catalog_q}.information_schema.routines r
            JOIN {catalog_q}.information_schema.routine_columns c
                   ON  c.specific_catalog = r.specific_catalog
                   AND c.specific_schema  = r.specific_schema
                   AND c.specific_name    = r.specific_name
            WHERE r.routine_schema = ?
            ORDER BY r.routine_name, c.ordinal_position
        """
        try:
            cur.execute(query, (schema,))
            rows = cur.fetchall()
        except Exception as exc:  # noqa: BLE001 — soft-fail is the contract
            logger.warning(
                "list_functions: could not read return columns of %s: %s",
                schema,
                exc,
            )
            return {}

        columns: Dict[str, List[Dict[str, str]]] = {}
        for row in rows:
            routine = (row[0] or "").strip()
            col_name = (row[1] or "").strip()
            if not routine or not col_name:
                continue
            columns.setdefault(routine, []).append(
                {"name": col_name, "data_type": str(row[2] or "").strip()}
            )
        return columns

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
                        "WHERE table_schema = ? AND table_type = 'BASE TABLE'",
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
                    # Databricks SQL Connector (use_inline_params=False) only
                    # rewrites qmark ``?`` placeholders — pyformat ``%s`` is
                    # sent to the warehouse verbatim and raises PARSE_SYNTAX_ERROR.
                    query = (
                        f"SELECT comment FROM {catalog_q}.information_schema.tables "
                        "WHERE table_catalog = ? "
                        "AND table_schema = ? "
                        "AND table_name = ?"
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

    def get_effective_schema_permissions(
        self, catalog: str, schema: str, principal: str
    ) -> Dict[str, Any]:
        """Return effective schema privileges for one principal."""
        if not self._auth.host or not self._auth.has_valid_auth():
            return {
                "accessible": False,
                "assignments": [],
                "error": "Not authenticated",
            }

        host = self._auth.host.rstrip("/")
        headers = self._auth.get_auth_headers()
        url = (
            f"{host}/api/2.1/unity-catalog/effective-permissions/"
            f"SCHEMA/{catalog}.{schema}"
        )
        response = requests.get(
            url, headers=headers, params={"principal": principal}, timeout=10
        )
        if response.status_code == 404:
            return {
                "accessible": False,
                "assignments": [],
                "error": "Schema not found in Unity Catalog",
            }
        if response.status_code == 403:
            return {
                "accessible": False,
                "assignments": [],
                "error": "Insufficient privileges to inspect effective schema permissions",
            }
        response.raise_for_status()

        payload = response.json() if response.content else {}
        raw_assignments = payload.get("privilege_assignments", []) or []
        if isinstance(raw_assignments, dict):
            raw_assignments = [raw_assignments]

        assignments: List[Dict[str, str]] = []
        for entry in raw_assignments:
            if not isinstance(entry, dict):
                continue
            entry_principal = entry.get("principal")
            if entry_principal is not None and str(entry_principal) != principal:
                continue

            raw_privileges = entry.get("privileges", []) or []
            if isinstance(raw_privileges, (str, dict)):
                raw_privileges = [raw_privileges]

            for priv in raw_privileges:
                inherited_from = ""
                privilege_name = ""
                if isinstance(priv, dict):
                    privilege_name = self._normalize_privilege_name(
                        priv.get("privilege") or priv.get("privilege_name")
                    )
                    inherited_from = str(
                        priv.get("inherited_from_name")
                        or priv.get("inherited_from")
                        or ""
                    ).strip()
                elif isinstance(priv, str):
                    privilege_name = self._normalize_privilege_name(priv)
                if not privilege_name:
                    continue
                assignments.append(
                    {
                        "privilege": privilege_name,
                        "inherited_from": inherited_from,
                    }
                )

        return {"accessible": True, "assignments": assignments, "error": None}

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
