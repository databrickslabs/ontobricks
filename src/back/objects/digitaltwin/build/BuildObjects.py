"""Objects one context version built in its graph backend (Graph → Build → Objects).

Settings → Lakebase / Lakehouse / Neo4j already list *every* object of the
workspace. This class serves the same inventory scoped to the **current**
context version and to the backend it uses, as backend-agnostic *groups* the
Build page renders without any per-backend knowledge.

Each item carries a ``drop`` descriptor (``api`` + the payload the matching
Settings drop endpoint expects), so the page can delete it without knowing
whether it is a Postgres table, a Unity Catalog object or a Neo4j graph.
Items inside a group, and groups themselves, are ordered so that dropping them
top to bottom is safe (views before tables, sync registrations before their
source, analytics last).
"""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional, Tuple

from back.core.graphdb.GraphDBFactory import GraphDBFactory
from back.core.helpers import effective_graph_name, effective_view_table, safe_identifier
from back.core.logging import get_logger

logger = get_logger(__name__)

# Postgres companion suffixes of one graph version, longest first.
_PG_TABLE_SUFFIXES: Tuple[str, ...] = (
    "_entity_search_asserted",
    "_entity_search",
    "_props",
    "_adj_out",
    "_adj_in",
    "__app",
    "_sync",
)
_SIM_SUFFIX_RE = re.compile(r"__(sim|smu|sms|smp|smb|smo)_[a-z0-9]+$", re.I)
_GRAPH_NAME_RE = re.compile(r"^(?P<safe>.+)_V(?P<version>[^_]+)$", re.I)

# Usage buckets in display order. Titles reuse the Build → Cache vocabulary.
USAGES: Tuple[Tuple[str, str], ...] = (
    ("graph", "Graph data"),
    ("inferred", "Inferred"),
    ("cache", "Search cache"),
    ("simulation", "Simulations"),
    ("analytics", "Analytics"),
)
_INFERRED_SUFFIXES: Tuple[str, ...] = ("__app", "_inferred")
_CACHE_SUFFIXES: Tuple[str, ...] = (
    "_adj_out",
    "_adj_in",
    "_props",
    "_entity_search",
    "_entity_search_asserted",
)


def usage_of(name: str) -> str:
    """Classify one physical object by what the graph uses it for.

    Simulation sandboxes first (their ``__sim_<id>`` suffix can follow a cache
    suffix), then analytics output, then the writable companions, then the
    search cache. Anything else is graph data: the union view, the ``_sync``
    bulk table, the Delta ``_data`` / ``_graph`` relations and the R2RML view.
    """
    if _SIM_SUFFIX_RE.search(name):
        return "simulation"
    if name.startswith("graph_metrics_") or name.endswith("_analytics"):
        return "analytics"
    if name.endswith(_INFERRED_SUFFIXES):
        return "inferred"
    if name.endswith(_CACHE_SUFFIXES):
        return "cache"
    return "graph"


class BuildObjects:
    """Per-backend inventory of the current context version's physical objects."""

    @classmethod
    def list_for_domain(cls, domain, session_mgr, settings) -> Dict[str, Any]:
        """Return ``{backend, graph_name, groups, message}`` for *domain*."""
        backend = GraphDBFactory._resolve_graph_backend(domain)
        builders: Dict[str, Callable[..., List[Dict[str, Any]]]] = {
            "databricks": cls._delta_groups,
            "lakebase": cls._lakebase_groups,
            "neo4j": cls._neo4j_groups,
        }
        build = builders.get(backend)
        groups = build(domain, session_mgr, settings) if build else []
        return {
            "success": True,
            "backend": backend,
            "graph_name": effective_graph_name(domain),
            "groups": groups,
            "usages": [{"key": key, "title": title} for key, title in USAGES],
            "message": "" if build else "This context has no graph backend.",
        }

    # ------------------------------------------------------------------
    # Delta (Lakehouse)
    # ------------------------------------------------------------------

    @classmethod
    def _delta_groups(cls, domain, session_mgr, settings) -> List[Dict[str, Any]]:
        probe = GraphDBFactory.get_health_probe(None, settings, engine="delta")
        catalog, schema, leaf = effective_view_table(domain, settings).split(".", 2)
        key = probe.domain_match_key(leaf)
        location = f"{catalog}.{schema}"
        try:
            raw = probe.fetch_schema_tables(catalog, schema)
        except Exception as exc:  # noqa: BLE001
            logger.warning("build objects: UC listing failed: %s", exc)
            return [cls._group("triplestore", "Triple store", location, [], error=str(exc))]

        grouped = probe.group_triplestore_objects(raw, catalog, schema)
        items = [
            cls._uc_item(i)
            for g in grouped.values()
            if probe.domain_match_key(g["base"]) == key
            for i in g["sorted_items"]
        ]
        return [
            cls._group("triplestore", "Triple store", location, items)
        ] + cls._analytics_groups(settings, catalog, schema, key, raw)

    @classmethod
    def _analytics_groups(
        cls,
        settings,
        catalog: str,
        schema: str,
        key: str,
        raw: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        """Analytics job output of this version (any backend), best-effort.

        Empty unless the version has analytics tables or the scan failed.
        """
        from back.objects.domain.settings.SettingsService import SettingsService

        def analytics() -> List[Dict[str, Any]]:
            tables = raw
            if tables is None:
                tables = GraphDBFactory.get_health_probe(
                    None, settings, engine="delta"
                ).fetch_schema_tables(catalog, schema)
            location, found, message = SettingsService._analytics_objects(
                settings, catalog, schema, tables
            )
            items = [
                cls._analytics_item(i)
                for g in found
                if g["key"] == key
                for i in g["items"]
            ]
            return [cls._group("analytics", "Analytics", location, items, error=message)]

        return [g for g in cls._guarded(analytics) if g["items"] or g["error"]]

    # ------------------------------------------------------------------
    # Lakebase
    # ------------------------------------------------------------------

    @classmethod
    def _lakebase_groups(cls, domain, session_mgr, settings) -> List[Dict[str, Any]]:
        from back.objects.domain.settings.SettingsService import SettingsService

        graph_name = effective_graph_name(domain)
        base = (safe_identifier(graph_name) or "triples").lower()

        def postgres() -> List[Dict[str, Any]]:
            data = SettingsService.graph_engine_lakebase_objects_result(
                "", "", session_mgr, settings
            )
            registry = data.get("registry_schema") or "ontobricks_registry"
            found = [
                (kind, o)
                for kind, rows in (("view", data.get("views")), ("table", data.get("tables")))
                for o in rows or []
                if o.get("schema") != registry and cls.pg_base(o["name"], kind) == base
            ]
            found.sort(key=lambda ko: (ko[0] != "view", ko[1]["name"]))  # views first
            items = [cls._pg_item(kind, o) for kind, o in found]
            return [cls._group("postgres", "Lakebase (Postgres)", "", items)]

        def unity_catalog() -> List[Dict[str, Any]]:
            data = SettingsService.graph_engine_lakebase_sync_objects_result(
                "", "", session_mgr, settings
            )
            tables = [
                t
                for t in data.get("uc_tables") or []
                if cls.uc_sync_base(t.get("name", "")) == base
            ]
            # Registrations first: the synced table must go before its source.
            tables.sort(key=lambda t: (not t.get("is_sync"), t.get("name", "")))
            location = ".".join(
                p for p in (data.get("uc_catalog"), data.get("uc_schema")) if p
            )
            return [
                cls._group(
                    "unity_catalog",
                    "Unity Catalog",
                    location,
                    [cls._uc_sync_item(t) for t in tables],
                )
            ]

        groups = cls._guarded(postgres) + cls._guarded(unity_catalog)
        catalog, schema = cls._registry_location(domain)
        key = cls._graph_key(graph_name)
        return groups + (
            cls._analytics_groups(settings, catalog, schema, key)
            if catalog and schema and key
            else []
        )

    # ------------------------------------------------------------------
    # Neo4j
    # ------------------------------------------------------------------

    @classmethod
    def _neo4j_groups(cls, domain, session_mgr, settings) -> List[Dict[str, Any]]:
        from back.objects.domain.settings.SettingsService import SettingsService

        info = getattr(domain, "info", None) or {}
        connection = str(info.get("neo4j_connection") or "").strip()
        label = GraphDBFactory.get_builder(None, engine="neo4j").sanitise_label(
            effective_graph_name(domain)
        )

        def graph() -> List[Dict[str, Any]]:
            data = SettingsService.graph_engine_neo4j_labels_result(
                session_mgr, settings, connection_name=connection
            )
            items = [
                {
                    "kind": "graph",
                    "name": g["label"],
                    "detail": f"{g.get('nodes', 0)} nodes · {g.get('edges', 0)} edges",
                    "drop": {
                        "api": "neo4j",
                        "label": g["label"],
                        "connection_name": connection,
                    },
                }
                for g in data.get("graphs") or []
                if g.get("label") == label
            ]
            return [cls._group("graph", "Neo4j graph", data.get("database", ""), items)]

        return cls._guarded(graph)

    # ------------------------------------------------------------------
    # Naming rules (pure, unit-tested)
    # ------------------------------------------------------------------

    @staticmethod
    def pg_base(name: str, kind: str) -> str:
        """Graph base shared by every Postgres object of one version."""
        stem = _SIM_SUFFIX_RE.sub("", name)
        if kind == "table":
            for suffix in _PG_TABLE_SUFFIXES:
                if stem.endswith(suffix):
                    return stem[: -len(suffix)]
        return stem

    @staticmethod
    def uc_sync_base(name: str) -> str:
        """UC object name without its ``_sync`` suffix (the Postgres base)."""
        return name[: -len("_sync")] if name.endswith("_sync") else name

    @staticmethod
    def _graph_key(graph_name: str) -> str:
        """``<Domain>_V<n>`` → the ``<domain>_<n>`` analytics slug."""
        match = _GRAPH_NAME_RE.match(graph_name or "")
        return f"{match['safe']}_{match['version']}".lower() if match else ""

    @staticmethod
    def _registry_location(domain) -> Tuple[str, str]:
        delta = getattr(domain, "delta", None) or {}
        return str(delta.get("catalog") or ""), str(delta.get("schema") or "")

    # ------------------------------------------------------------------
    # Item / group builders
    # ------------------------------------------------------------------

    @staticmethod
    def _group(
        key: str,
        title: str,
        location: str,
        items: List[Dict[str, Any]],
        *,
        error: str = "",
    ) -> Dict[str, Any]:
        tagged = [{**item, "usage": usage_of(item["name"])} for item in items]
        return {
            "key": key,
            "title": title,
            "location": location,
            "items": tagged,
            "error": error,
        }

    @staticmethod
    def _uc_item(item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "kind": item["kind"],
            "name": item["name"],
            "detail": item["full_name"],
            "drop": {"api": "uc", "full_name": item["full_name"], "is_sync": False},
        }

    @staticmethod
    def _analytics_item(item: Dict[str, Any]) -> Dict[str, Any]:
        kind = "work" if re.search(r"_work(_|$)", item["name"]) else "metrics"
        return {
            "kind": kind,
            "name": item["name"],
            "detail": item["full_name"],
            "drop": {"api": "uc", "full_name": item["full_name"], "is_sync": False},
        }

    @staticmethod
    def _pg_item(kind: str, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "kind": kind,
            "name": row["name"],
            "detail": row.get("schema", ""),
            "drop": {
                "api": "lakebase",
                "kind": kind,
                "schema": row.get("schema", ""),
                "name": row["name"],
            },
        }

    @staticmethod
    def _uc_sync_item(table: Dict[str, Any]) -> Dict[str, Any]:
        is_sync = bool(table.get("is_sync"))
        is_view = str(table.get("table_type", "")).upper() == "VIEW"
        return {
            "kind": "sync" if is_sync else ("view" if is_view else "table"),
            "name": table["name"],
            "detail": table.get("full_name", ""),
            "state": table.get("state", ""),
            "error": table.get("error", ""),
            "drop": {
                "api": "uc",
                "full_name": table.get("full_name", ""),
                "is_sync": is_sync,
            },
        }

    @classmethod
    def _guarded(
        cls, build: Callable[[], List[Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        """Run one section; a failure becomes an error group, never a failed tab."""
        try:
            return build()
        except Exception as exc:  # noqa: BLE001 - OntoBricksError and driver errors alike
            logger.warning("build objects section %s failed: %s", build.__name__, exc)
            title = build.__name__.replace("_", " ").capitalize()
            return [cls._group(build.__name__, title, "", [], error=str(exc))]
