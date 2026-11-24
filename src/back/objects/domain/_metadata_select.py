"""Background SELECT probes for the Data Assets Rights column.

Kept outside :class:`Domain` so the asset list can paint from session metadata
first; the UI then calls ``POST /domain/metadata/check-select``.
"""

from __future__ import annotations

from typing import Any, Dict

from back.core.databricks import MetadataService
from back.core.exceptions import ValidationError
from back.core.helpers import (
    get_databricks_host_and_token,
    resolve_warehouse_id,
    run_blocking,
)


async def check_select_permissions(session, settings) -> Dict[str, Any]:
    """Probe SELECT on every loaded data asset and persist the Rights column."""
    metadata = session.catalog_metadata
    tables = list((metadata or {}).get("tables") or [])
    if not tables:
        return {"success": True, "tables": []}

    host, token = get_databricks_host_and_token(session, settings)
    warehouse_id = resolve_warehouse_id(session, settings)
    if not host or not warehouse_id:
        raise ValidationError(
            "Databricks not configured. Please configure connection in Settings.",
        )
    service = MetadataService(host=host, token=token, warehouse_id=warehouse_id)
    results = await run_blocking(service.probe_select_permissions, tables)
    session._data["domain"]["metadata"] = metadata
    session.save()
    return {"success": True, "tables": results}
