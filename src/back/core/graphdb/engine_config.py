"""Per-backend graph engine configuration (Lakebase / Neo4j / Lakehouse).

``graph_engine_config`` is stored as a nested object so backends share nothing::

    {
      "lakebase":  {"database": "...", "schema": "...", "sync_mode": "...", ...},
      "neo4j":     {"connections": [{"name": "...", "uri": "...", ...}, ...]},
      "lakehouse": {"warehouse_id": "..."}
    }

Legacy flat Neo4j blobs (single ``uri`` / ``username`` profile, or pre-nesting
shapes) are still folded into the ``neo4j`` bucket on read for Lakebase /
Lakehouse normalisation, but Neo4j *runtime* and Settings UI use only
``neo4j.connections[]`` — there is no auto-migration of a flat profile into a
named connection.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, MutableMapping, Optional

# Keys that belong exclusively to the Neo4j settings panel.
_NEO4J_FLAT_KEYS = frozenset(
    {
        "uri",
        "auth_method",
        "encrypted",
        "username",
        "password",
        "secret_scope",
        "secret_key",
        "neo4j_database",  # legacy namespaced key from shared-flat era
        "connections",  # named Neo4j connection profiles
    }
)

_BACKEND_KEYS = frozenset({"lakebase", "neo4j", "lakehouse"})


def _empty_normalized() -> Dict[str, Dict[str, Any]]:
    return {"lakebase": {}, "neo4j": {}, "lakehouse": {}}


def _as_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _coerce_bool(value: Any, *, default: bool) -> bool:
    """Coerce a stored JSON scalar to bool, tolerating strings/ints."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return default


def is_nested_graph_engine_config(cfg: Optional[Mapping[str, Any]]) -> bool:
    """True when *cfg* already uses a per-backend bucket."""
    if not isinstance(cfg, Mapping):
        return False
    return (
        isinstance(cfg.get("lakebase"), dict)
        or isinstance(cfg.get("neo4j"), dict)
        or isinstance(cfg.get("lakehouse"), dict)
    )


def _looks_like_neo4j_section(cfg: Mapping[str, Any]) -> bool:
    """True when *cfg* is already a Neo4j bucket (not a nested/flat root).

    Used so ``neo4j_section({"connections": [...]})`` round-trips through
    ``normalize`` without dumping ``connections`` into Lakebase.
    """
    if any(k in cfg for k in _BACKEND_KEYS):
        return False
    # Lakebase / shared flat markers → use the flat or nested paths.
    if any(
        k in cfg
        for k in (
            "schema",
            "sync_mode",
            "sync_table_mode",
            "sync_uc_catalog",
            "sync_uc_schema",
            "warehouse_id",
            "lakebase_branch",
        )
    ):
        return False
    return isinstance(cfg.get("connections"), list)

def normalize_graph_engine_config(
    cfg: Optional[Mapping[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Return ``{lakebase, neo4j, lakehouse}``, migrating flat legacy blobs.

    Always returns a fresh dict with all three keys present (possibly empty).
    Within the Neo4j bucket, ``neo4j_database`` is folded into ``database``.
    A bare Neo4j section (``connections`` / ``uri`` without backend wrappers)
    is accepted and placed under ``neo4j``.
    """
    if not isinstance(cfg, Mapping):
        return _empty_normalized()

    # Already a Neo4j section passed through neo4j_section() / factory create().
    if _looks_like_neo4j_section(cfg) and not is_nested_graph_engine_config(cfg):
        return {
            "lakebase": {},
            "neo4j": _finalize_neo4j_bucket(_as_dict(cfg)),
            "lakehouse": {},
        }

    if is_nested_graph_engine_config(cfg):
        lakebase = _as_dict(cfg.get("lakebase"))
        neo4j = _as_dict(cfg.get("neo4j"))
        lakehouse = _as_dict(cfg.get("lakehouse"))
        # Fold any stray flat leftovers left from a partial write.
        for key, value in cfg.items():
            if key in _BACKEND_KEYS:
                continue
            if key == "warehouse_id":
                if value and not lakehouse.get("warehouse_id"):
                    lakehouse["warehouse_id"] = value
                continue
            if key in _NEO4J_FLAT_KEYS or key == "neo4j_database":
                if key == "neo4j_database":
                    if value and not neo4j.get("database"):
                        neo4j["database"] = value
                elif key == "connections":
                    if value and not neo4j.get("connections"):
                        neo4j["connections"] = value
                elif key not in neo4j:
                    neo4j[key] = value
            elif key == "database":
                # Ambiguous leftover — prefer Lakebase unless Neo4j has no DB yet
                # and the value looks like the Bolt default while a URI exists.
                raw = str(value or "").strip()
                if not raw:
                    continue
                if (
                    raw.lower() == "neo4j"
                    and str(neo4j.get("uri") or "").strip()
                    and not lakebase.get("database")
                ):
                    if not neo4j.get("database"):
                        neo4j["database"] = raw
                elif "database" not in lakebase:
                    lakebase["database"] = value
            elif key not in lakebase:
                lakebase[key] = value
        return {
            "lakebase": lakebase,
            "neo4j": _finalize_neo4j_bucket(neo4j),
            "lakehouse": _finalize_lakehouse_bucket(lakehouse),
        }

    # ---- flat legacy shape ----
    lakebase: Dict[str, Any] = {}
    neo4j: Dict[str, Any] = {}
    lakehouse: Dict[str, Any] = {}
    for key, value in cfg.items():
        if key in _BACKEND_KEYS:
            continue
        if key == "warehouse_id":
            if value:
                lakehouse["warehouse_id"] = value
            continue
        if key == "neo4j_database":
            if value:
                neo4j["database"] = value
            continue
        if key == "connections":
            neo4j["connections"] = value
            continue
        if key in _NEO4J_FLAT_KEYS:
            neo4j[key] = value
            continue
        if key == "database":
            continue  # handled below
        lakebase[key] = value

    raw_db = str(cfg.get("database") or "").strip()
    has_neo4j_marker = bool(
        str(cfg.get("uri") or "").strip()
        or str(cfg.get("neo4j_database") or "").strip()
        or isinstance(cfg.get("connections"), list)
    )
    if raw_db:
        if has_neo4j_marker and raw_db.lower() == "neo4j":
            # Historical pollution of the shared ``database`` key — Bolt default.
            if not neo4j.get("database"):
                neo4j["database"] = raw_db
        else:
            lakebase["database"] = raw_db

    return {
        "lakebase": lakebase,
        "neo4j": _finalize_neo4j_bucket(neo4j),
        "lakehouse": _finalize_lakehouse_bucket(lakehouse),
    }


def _finalize_neo4j_connection(entry: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalise one named Neo4j connection profile (no password invention)."""
    out = dict(entry)
    name = str(out.get("name") or "").strip()
    if name:
        out["name"] = name
    legacy = out.pop("neo4j_database", None)
    if legacy and not str(out.get("database") or "").strip():
        out["database"] = legacy
    db = str(out.get("database") or "").strip()
    out["database"] = db or "neo4j"
    if "encrypted" in out:
        out["encrypted"] = bool(out.get("encrypted"))
    auth = str(out.get("auth_method") or "").strip()
    if auth:
        out["auth_method"] = auth
    return out


def _finalize_neo4j_bucket(neo4j: MutableMapping[str, Any]) -> Dict[str, Any]:
    out = dict(neo4j)
    legacy = out.pop("neo4j_database", None)
    if legacy and not str(out.get("database") or "").strip():
        out["database"] = legacy
    raw_conns = out.get("connections")
    if isinstance(raw_conns, list):
        cleaned: List[Dict[str, Any]] = []
        for item in raw_conns:
            if not isinstance(item, Mapping):
                continue
            profile = _finalize_neo4j_connection(item)
            if not str(profile.get("name") or "").strip():
                continue
            cleaned.append(profile)
        out["connections"] = cleaned
    elif "connections" in out:
        out["connections"] = []
    return out


def _finalize_lakehouse_bucket(lakehouse: MutableMapping[str, Any]) -> Dict[str, Any]:
    out = dict(lakehouse)
    wid = str(out.get("warehouse_id") or "").strip()
    if wid:
        out["warehouse_id"] = wid
    elif "warehouse_id" in out:
        out["warehouse_id"] = ""
    # Only stamp use_sea onto a non-empty bucket so an absent Lakehouse config
    # stays ``{}`` (callers rely on the empty-bucket contract).
    if out:
        out["use_sea"] = _coerce_bool(out.get("use_sea"), default=False)
    return out


def lakebase_section(cfg: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Return the Lakebase connection/options dict from any stored shape."""
    return dict(normalize_graph_engine_config(cfg).get("lakebase") or {})


def neo4j_section(cfg: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Return the Neo4j bucket from any stored shape (may include ``connections``)."""
    return dict(normalize_graph_engine_config(cfg).get("neo4j") or {})


def list_neo4j_connections(cfg: Optional[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """Return named Neo4j connection profiles from ``neo4j.connections``.

    Flat legacy keys on the Neo4j bucket are ignored — they are never turned
    into a synthetic connection.
    """
    neo = neo4j_section(cfg)
    raw = neo.get("connections")
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        profile = _finalize_neo4j_connection(item)
        name = str(profile.get("name") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(profile)
    return out


def resolve_neo4j_connection(
    cfg: Optional[Mapping[str, Any]], name: str
) -> Dict[str, Any]:
    """Return the named Neo4j connection profile, or ``{}`` if missing."""
    target = str(name or "").strip()
    if not target:
        return {}
    for profile in list_neo4j_connections(cfg):
        if str(profile.get("name") or "").strip() == target:
            return dict(profile)
    return {}


def lakehouse_section(cfg: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Return the Lakehouse (Delta) connection dict from any stored shape."""
    return dict(normalize_graph_engine_config(cfg).get("lakehouse") or {})


def resolve_lakehouse_warehouse_id(cfg: Optional[Mapping[str, Any]]) -> str:
    """Return ``lakehouse.warehouse_id`` (empty string when unset)."""
    return str(lakehouse_section(cfg).get("warehouse_id") or "").strip()


def resolve_lakehouse_use_sea(cfg: Optional[Mapping[str, Any]]) -> bool:
    """Return ``lakehouse.use_sea`` (default ``False`` — Thrift transport).

    ``True`` selects the Statement Execution API, required for serverless
    Lakehouse/RT SQL warehouses.
    """
    return _coerce_bool(lakehouse_section(cfg).get("use_sea"), default=False)
