"""Databricks SQL Warehouse wiring for the Delta graph engine."""

from __future__ import annotations

from typing import Any, Optional, Tuple

from back.core.databricks import is_databricks_app
from back.core.helpers import (
    get_databricks_host_and_token,
    resolve_delta_warehouse_id,
    resolve_lakehouse_use_sea,
)
from back.core.logging import get_logger

logger = get_logger(__name__)


def create_databricks_client(
    domain: Any,
    settings: Optional[Any] = None,
) -> Optional[Any]:
    """Return a :class:`DatabricksClient` or *None* if configuration is incomplete."""
    try:
        from back.core.databricks import DatabricksClient

        if settings is not None:
            host, token = get_databricks_host_and_token(domain, settings)
            warehouse_id = resolve_delta_warehouse_id(domain, settings)
            use_sea = resolve_lakehouse_use_sea(domain, settings)
        else:
            db = getattr(domain, "databricks", None) or {}
            host = db.get("host", "")
            token = db.get("token", "")
            warehouse_id = db.get("warehouse_id", "") or db.get("sql_warehouse_id", "")
            use_sea = bool(db.get("use_sea", False))

        if not host and not is_databricks_app():
            logger.warning("Delta graph engine: missing host")
            return None
        if not token and not is_databricks_app():
            logger.warning("Delta graph engine: missing token")
            return None
        if not warehouse_id:
            logger.warning("Delta graph engine: missing sql_warehouse_id")
            return None

        return DatabricksClient(
            host=host, token=token, warehouse_id=warehouse_id, use_sea=use_sea
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to create DatabricksClient for Delta engine: %s", exc)
        return None


def resolve_credentials(
    domain: Any, settings: Optional[Any] = None
) -> Tuple[str, str, str]:
    """Return ``(host, token, warehouse_id)`` for build tasks."""
    if settings is not None:
        host, token = get_databricks_host_and_token(domain, settings)
        warehouse_id = resolve_warehouse_id(domain, settings)
    else:
        db = getattr(domain, "databricks", None) or {}
        host = db.get("host", "")
        token = db.get("token", "")
        warehouse_id = db.get("warehouse_id", "") or db.get("sql_warehouse_id", "")
    return host, token, warehouse_id
