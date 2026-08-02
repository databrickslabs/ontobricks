"""List and group Unity Catalog triple-store objects (Settings → Lakehouse)."""

from __future__ import annotations

import re
from typing import Any, Dict, List

from back.core.errors import InfrastructureError

_TRIPLESTORE_PREFIX = "triplestore_"
_ANALYTICS_PREFIX = "graph_metrics_"

# Companion tables the analytics Lakeflow job writes next to ``<out>``.
_ANALYTICS_SUFFIXES = ("_type_predicates", "_type_profiles", "_summary")

# Scratch tables from a run that died before its own cleanup: ``<out>_work_<stage>``.
_WORK_RE = re.compile(r"_work(?:_.*)?$")

_VIEW_NAME_RE = re.compile(r"^triplestore_(?P<safe>.+)_V(?P<version>[^_]+)$", re.I)


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


def analytics_base(name: str) -> str:
    """Strip the work/companion suffix to get the ``graph_metrics_<slug>`` base."""
    stripped = _WORK_RE.sub("", name)
    for suffix in _ANALYTICS_SUFFIXES:
        if stripped.endswith(suffix):
            return stripped[: -len(suffix)]
    return stripped


def analytics_match_key(name: str) -> str:
    """Join key of an analytics table: the ``<safe>_<version>`` slug, or ``""``."""
    base = analytics_base(name)
    if not base.startswith(_ANALYTICS_PREFIX):
        return ""
    return base[len(_ANALYTICS_PREFIX) :].lower()


def domain_match_key(name: str) -> str:
    """Join key of a triple-store object: ``triplestore_<safe>_V<n>`` → ``<safe>_<n>``.

    The analytics slug is built from ``uc_domain_folder``, which drops offending
    characters where the view name replaces them with underscores. The two agree
    for an ordinary domain name and diverge for a punctuated one — a divergence
    that surfaces as an orphan group rather than a silently hidden table.
    """
    m = _VIEW_NAME_RE.match(object_base(name))
    if not m:
        return ""
    return f"{m.group('safe')}_{m.group('version')}".lower()


def _analytics_drop_sort_key(name: str) -> tuple:
    """Scratch first, then companions, then the base — a predictable progress list.

    All five are managed tables with no dependency between them, so this order
    is for readability, not for correctness.
    """
    if _WORK_RE.search(name):
        return (0, name)
    for order, suffix in enumerate(_ANALYTICS_SUFFIXES, start=1):
        if name.endswith(suffix):
            return (order, name)
    return (len(_ANALYTICS_SUFFIXES) + 1, name)


def group_analytics_objects(
    raw_tables: List[Dict[str, Any]],
    catalog: str,
    schema: str,
) -> Dict[str, Dict[str, Any]]:
    """Group UC entries named ``graph_metrics_*`` by their domain-version slug."""
    groups: Dict[str, Dict[str, Any]] = {}
    for tbl in raw_tables:
        name = (tbl.get("name") or "").strip()
        if not name.startswith(_ANALYTICS_PREFIX):
            continue
        key = analytics_match_key(name)
        if not key:
            continue
        full_name = (tbl.get("full_name") or f"{catalog}.{schema}.{name}").strip()
        table_type = str(tbl.get("table_type", "") or "")
        if key not in groups:
            groups[key] = {"key": key, "base": analytics_base(name), "items": []}
        groups[key]["items"].append(
            {
                "kind": uc_object_kind(table_type),
                "name": name,
                "full_name": full_name,
                "table_type": table_type,
            }
        )
    for grp in groups.values():
        grp["sorted_items"] = sorted(
            grp["items"],
            key=lambda o: _analytics_drop_sort_key(o["name"]),
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
