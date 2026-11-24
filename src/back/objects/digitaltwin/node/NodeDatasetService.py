"""Resolve class-linked Unity Catalog datasets for node context.

Tables/views use ``SELECT * … WHERE key = id``. Metric views use
``MEASURE()`` + ``GROUP BY`` via :func:`build_metric_view_entity_sql`.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from back.core.databricks.uc.metric_sql import build_metric_view_entity_sql
from back.core.errors import ValidationError
from back.core.helpers import (
    SAFE_COL_IDENT as _SAFE_COL_IDENT,
    SAFE_SQL_IDENT as _SAFE_SQL_IDENT,
    get_data_plane_client,
    run_blocking,
    sql_escape,
)
from back.core.logging import get_logger

logger = get_logger(__name__)


class NodeDatasetService:
    """Resolve one or more linked datasets for an entity node."""

    @staticmethod
    async def resolve_datasets(
        domain: Any,
        settings: Any,
        *,
        raw_datasets: List[Dict[str, Any]],
        local_id: str,
        entity_uri: str,
        fetch_dataset_rows: bool,
        dataset_row_limit: int,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Resolve every linked dataset; soft-fail per entry on row fetch."""
        if not raw_datasets:
            return [], None

        client_db = None
        if fetch_dataset_rows:
            client_db = get_data_plane_client(domain, settings)

        out: List[Dict[str, Any]] = []
        fetch_errors: List[str] = []
        for raw_dataset in raw_datasets:
            resolved, err = await NodeDatasetService.resolve_one(
                raw_dataset=raw_dataset,
                local_id=local_id,
                entity_uri=entity_uri,
                fetch_dataset_rows=fetch_dataset_rows,
                dataset_row_limit=dataset_row_limit,
                client_db=client_db,
            )
            if resolved is not None:
                out.append(resolved)
            if err:
                fetch_errors.append(err)
        return out, "; ".join(fetch_errors) if fetch_errors else None

    @staticmethod
    async def resolve_one(
        *,
        raw_dataset: Dict[str, Any],
        local_id: str,
        entity_uri: str,
        fetch_dataset_rows: bool,
        dataset_row_limit: int,
        client_db: Any = None,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        if not raw_dataset or not raw_dataset.get("fullName"):
            return None, None

        full_name = raw_dataset.get("fullName", "")
        if not full_name or not _SAFE_SQL_IDENT.match(full_name):
            raise ValidationError(
                f"Invalid dataset fullName in class config: {full_name!r}"
            )

        key_col = raw_dataset.get("key_column")
        if key_col and not _SAFE_COL_IDENT.match(key_col):
            raise ValidationError(
                f"Invalid key_column in class config: {key_col!r}"
            )

        ds_type = str(raw_dataset.get("type") or "TABLE").upper()
        rows = None
        key_col_missing = None
        fetch_error: Optional[str] = None

        if fetch_dataset_rows:
            if not key_col:
                key_col_missing = True
            else:
                try:
                    if client_db is None:
                        fetch_error = "Databricks client is not configured"
                        rows = []
                    elif ds_type == "METRIC_VIEW":
                        rows, fetch_error = await NodeDatasetService.fetch_metric_rows(
                            client_db,
                            raw_dataset=raw_dataset,
                            local_id=local_id,
                            dataset_row_limit=dataset_row_limit,
                        )
                    else:
                        sql = (
                            f"SELECT * FROM {raw_dataset['fullName']} "
                            f"WHERE {key_col} = '{sql_escape(local_id)}' "
                            f"LIMIT {dataset_row_limit}"
                        )
                        result = await run_blocking(client_db.execute_query, sql)
                        rows = result if isinstance(result, list) else []
                except Exception as exc:
                    fetch_error = str(exc)
                    rows = []
                    logger.warning(
                        "nodes/context: dataset row fetch failed for %s (%s): %s",
                        entity_uri,
                        full_name,
                        exc,
                    )

        return {
            "fullName": raw_dataset["fullName"],
            "type": ds_type,
            "key_column": key_col,
            "key_column_missing": key_col_missing,
            "description": (raw_dataset.get("description") or "").strip() or None,
            "rows": rows,
        }, fetch_error

    @staticmethod
    async def fetch_metric_rows(
        client_db: Any,
        *,
        raw_dataset: Dict[str, Any],
        local_id: str,
        dataset_row_limit: int,
    ) -> Tuple[List[Any], Optional[str]]:
        """Fetch entity-scoped rows from a UC metric view (MEASURE + GROUP BY)."""
        key_col = raw_dataset.get("key_column") or ""
        columns = list(raw_dataset.get("columns") or [])
        if not columns:
            catalog = raw_dataset.get("catalog") or ""
            schema = raw_dataset.get("schema") or ""
            asset = raw_dataset.get("asset") or ""
            if not (catalog and schema and asset) and raw_dataset.get("fullName"):
                parts = str(raw_dataset["fullName"]).split(".")
                if len(parts) == 3:
                    catalog, schema, asset = parts
            if catalog and schema and asset and hasattr(
                client_db, "get_metric_view_columns_with_roles"
            ):
                columns = await run_blocking(
                    client_db.get_metric_view_columns_with_roles,
                    catalog,
                    schema,
                    asset,
                )
            else:
                return [], (
                    "Metric view columns are unavailable; "
                    "cannot build MEASURE()/GROUP BY SQL"
                )

        sql = build_metric_view_entity_sql(
            {"full_name": raw_dataset["fullName"], "columns": columns},
            key_column=key_col,
            key_value=local_id,
            limit=dataset_row_limit,
        )
        result = await run_blocking(client_db.execute_query, sql)
        return (result if isinstance(result, list) else []), None
