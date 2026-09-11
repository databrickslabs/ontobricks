"""Read-only probes for the Databricks Delta triple store."""

from __future__ import annotations

from typing import Any, Dict

from back.core.logging import get_logger
from back.core.graphdb.delta.DeltaFlatStore import DeltaFlatStore

logger = get_logger(__name__)

REQUIRED_SCHEMA_PERMISSIONS = (
    "USE CATALOG",
    "USE SCHEMA",
    "CREATE TABLE",
    "CREATE VIEW",
    "SELECT",
    "MODIFY",
)


def _normalize_permission_name(value: Any) -> str:
    """Normalize evaluator input from UC REST assignments.

    Intentionally duplicated with ``UnityCatalog._normalize_privilege_name``:
    this evaluator remains pure and boundary-agnostic, avoiding a dependency
    cycle between Databricks client code and domain health logic.
    """
    return str(value or "").strip().replace("_", " ").upper()


def schema_permission_summary(
    catalog: str, schema: str, principal: str, assignments: Any
) -> Dict[str, Any]:
    """Evaluate required schema permissions from normalized assignments."""
    registry_catalog = (catalog or "").strip()
    registry_schema = (schema or "").strip()
    storage_location = (
        f"{registry_catalog}.{registry_schema}"
        if registry_catalog and registry_schema
        else ""
    )
    raw_assignments = assignments if isinstance(assignments, list) else []
    grants: Dict[str, str] = {}

    for item in raw_assignments:
        if not isinstance(item, dict):
            continue
        privilege = _normalize_permission_name(item.get("privilege"))
        inherited_from = str(item.get("inherited_from") or "").strip()
        if not privilege:
            continue
        if privilege == "ALL PRIVILEGES":
            for required in REQUIRED_SCHEMA_PERMISSIONS:
                grants.setdefault(required, inherited_from)
            continue
        if privilege in REQUIRED_SCHEMA_PERMISSIONS:
            grants.setdefault(privilege, inherited_from)

    permissions = [
        {
            "name": required,
            "granted": required in grants,
            "inherited_from": grants.get(required, ""),
        }
        for required in REQUIRED_SCHEMA_PERMISSIONS
    ]
    return {
        "registry_catalog": registry_catalog,
        "registry_schema": registry_schema,
        "storage_location": storage_location,
        "principal": principal,
        "permissions": permissions,
        "operational": all(item["granted"] for item in permissions),
    }


def probe_table_status(store: DeltaFlatStore, table_fqn: str) -> Dict[str, Any]:
    """Return existence, count, and optional ``DESCRIBE DETAIL`` metadata."""
    out: Dict[str, Any] = {
        "table_fqn": table_fqn,
        "exists": False,
        "has_data": False,
        "count": 0,
        "last_modified": None,
        "path": None,
        "format": None,
        "error": None,
    }
    if not table_fqn:
        out["error"] = "No table FQN configured"
        return out
    try:
        exists = store.table_exists(table_fqn)
        out["exists"] = exists
        if not exists:
            return out
        status = store.get_status(table_fqn)
        count = int(status.get("count", 0) or 0)
        out["count"] = count
        out["has_data"] = count > 0
        out["last_modified"] = status.get("last_modified")
        out["path"] = status.get("path")
        out["format"] = status.get("format")
    except Exception as exc:  # noqa: BLE001
        logger.warning("probe_table_status failed for %s: %s", table_fqn, exc)
        out["error"] = str(exc)
    return out


def probe_from_client(client: Any, table_fqn: str) -> Dict[str, Any]:
    """Convenience wrapper when only a Databricks client is available."""
    if client is None:
        return {
            "table_fqn": table_fqn,
            "exists": False,
            "has_data": False,
            "count": 0,
            "error": "Databricks client not configured",
        }
    return probe_table_status(DeltaFlatStore(client), table_fqn)
