"""List and group Unity Catalog triple-store objects (Settings → Lakehouse)."""

from __future__ import annotations

from typing import Any, Dict, List

from back.core.errors import InfrastructureError

_TRIPLESTORE_PREFIX = "triplestore_"


def object_base(name: str) -> str:
    """Strip ``_data`` / ``_inferred`` / ``_graph`` suffix to get the domain group key."""
    if name.endswith("_graph"):
        return name[: -len("_graph")]
    if name.endswith("_inferred"):
        return name[: -len("_inferred")]
    if name.endswith("_data"):
        return name[: -len("_data")]
    return name


def uc_object_kind(table_type: str) -> str:
    """Map UC ``table_type`` to ``view`` or ``table`` for drop ordering."""
    return "view" if (table_type or "").upper() == "VIEW" else "table"


def _drop_sort_key(name: str, kind: str) -> tuple:
    """Views before tables; within views: ``_graph`` then R2RML; tables: ``_data`` then ``_inferred``."""
    if kind == "view":
        order = 0 if name.endswith("_graph") else 1
        return (0, order, name)
    order = 0 if name.endswith("_data") else 1
    return (1, order, name)


def group_triplestore_objects(
    raw_tables: List[Dict[str, Any]],
    catalog: str,
    schema: str,
) -> Dict[str, Dict[str, Any]]:
    """Group UC table entries whose names start with ``triplestore_`` by domain base."""
    groups: Dict[str, Dict[str, Any]] = {}
    for tbl in raw_tables:
        name = (tbl.get("name") or "").strip()
        if not name.startswith(_TRIPLESTORE_PREFIX):
            continue
        full_name = (tbl.get("full_name") or f"{catalog}.{schema}.{name}").strip()
        table_type = str(tbl.get("table_type", "") or "")
        kind = uc_object_kind(table_type)
        base = object_base(name)
        if base not in groups:
            groups[base] = {"base": base, "items": []}
        groups[base]["items"].append(
            {
                "kind": kind,
                "name": name,
                "full_name": full_name,
                "table_type": table_type,
            }
        )
    for grp in groups.values():
        grp["sorted_items"] = sorted(
            grp["items"],
            key=lambda o: _drop_sort_key(o["name"], o["kind"]),
        )
    return groups


def fetch_uc_schema_tables(catalog: str, schema: str) -> List[Dict[str, Any]]:
    """Enumerate tables and views in a UC schema via the REST API."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    api = getattr(w, "api_client", None)
    if api is None or not hasattr(api, "do"):
        raise InfrastructureError("Databricks SDK api_client unavailable")
    raw = (
        api.do(
            "GET",
            "/api/2.1/unity-catalog/tables",
            query={"catalog_name": catalog, "schema_name": schema},
        )
        or {}
    )
    return list(raw.get("tables", []) or [])
