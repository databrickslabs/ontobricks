"""Assemble and freshness-check Knowledge Graph stats payloads.

Extracted from ``api.routers.internal.dtwin.triplestore_stats``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def _dt():
    from back.objects.digitaltwin.DigitalTwin import DigitalTwin

    return DigitalTwin


class TwinGraphStats:
    """Classify predicates and wrap warehouse aggregates for /sync/stats."""

    @staticmethod
    def cache_is_fresh(cached: Optional[dict]) -> bool:
        if not cached:
            return False
        preds = cached.get("top_predicates") or []
        has_kind = preds and "kind" in preds[0]
        has_job_reason = "analytics_job_blocked_reason" in cached
        # Typed entities with no triples is not a real graph state.
        inconsistent = bool(cached.get("entity_types")) and not cached.get(
            "total_triples"
        )
        has_graph_cache = "graph_cache" in cached
        return bool(has_kind and has_job_reason and has_graph_cache and not inconsistent)

    @staticmethod
    def assemble(
        domain,
        settings,
        *,
        agg: dict,
        entity_types: list,
        top_predicates: list,
        inferred_count,
        job_available: bool,
        job_blocked_reason: str,
        companions: Optional[List[Dict[str, Any]]] = None,
        communities: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        total_count = agg["total"]
        label_count = agg["label_count"]
        type_count = sum(int(r.get("cnt", 0)) for r in entity_types)
        classified = _dt()(domain).classify_predicates(top_predicates)
        return {
            "success": True,
            "total_triples": total_count,
            "distinct_subjects": agg["distinct_subjects"],
            "distinct_predicates": agg["distinct_predicates"],
            "entity_types": [
                {"uri": r["type_uri"], "count": int(r["cnt"])} for r in entity_types
            ],
            "top_predicates": classified,
            "label_count": label_count,
            "type_assertion_count": type_count,
            "relationship_count": max(total_count - type_count - label_count, 0),
            "inferred_triples": inferred_count,
            "analytics_job_available": job_available,
            "analytics_job_blocked_reason": job_blocked_reason,
            "graph_cache": TwinGraphStats._graph_cache_block(
                domain, companions or []
            ),
            "intelligence": TwinGraphStats._intelligence_block(
                inferred_count, communities
            ),
        }

    @staticmethod
    def companion_inventory(store: Any, graph_name: str) -> List[Dict[str, Any]]:
        """Safe companion row counts for Overview; empty when unsupported."""
        fn = getattr(store, "companion_inventory", None)
        if not callable(fn):
            return []
        try:
            return list(fn(graph_name) or [])
        except Exception as exc:  # noqa: BLE001
            from back.core.logging import get_logger

            get_logger(__name__).debug("Companion inventory failed: %s", exc)
            return []

    @staticmethod
    def community_totals(domain: Any, store: Any, graph_name: str) -> Dict[str, Any]:
        """Materialized community partition totals, or an empty ready=false block."""
        empty = {
            "cluster_count": 0,
            "node_count": 0,
            "algorithm": "",
            "ready": False,
        }
        try:
            from back.objects.digitaltwin.TwinAnalytics import TwinAnalytics

            result = TwinAnalytics(domain).load_current_clusters(store, graph_name, 1)
            stats = (result or {}).get("stats") or {}
            if not stats:
                return empty
            return {
                "cluster_count": int(stats.get("cluster_count") or 0),
                "node_count": int(stats.get("node_count") or 0),
                "algorithm": str(stats.get("algorithm") or ""),
                "ready": True,
            }
        except Exception as exc:  # noqa: BLE001
            from back.core.logging import get_logger

            get_logger(__name__).debug("Community totals failed: %s", exc)
            return empty

    @staticmethod
    def _graph_cache_block(
        domain: Any, companions: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        from back.core.graphdb.GraphDBFactory import GraphDBFactory
        from back.core.graphdb.search_cache import (
            GRAPH_CACHE_STATE_DISABLED,
            GRAPH_CACHE_STATE_PENDING,
            SEARCH_CACHE_SQL_BACKENDS,
            normalize_graph_cache_enabled,
            normalize_graph_cache_state,
        )

        info = getattr(domain, "info", None)
        info = info if isinstance(info, dict) else {}
        backend = GraphDBFactory._resolve_graph_backend(domain)
        supported = backend in SEARCH_CACHE_SQL_BACKENDS
        enabled = bool(supported) and normalize_graph_cache_enabled(
            info.get("graph_cache_enabled")
        )
        state = normalize_graph_cache_state(
            info.get("graph_cache_state"),
            enabled=enabled,
            default_when_missing=(
                GRAPH_CACHE_STATE_PENDING if enabled else GRAPH_CACHE_STATE_DISABLED
            ),
        )
        # Prefer the stamp written by mark_graph_cache_ready; fall back to the
        # last Build time (companions are created during Build when no dedicated
        # Refresh cache has run yet).
        refreshed_at = str(info.get("graph_cache_refreshed_at") or "").strip() or None
        if not refreshed_at:
            refreshed_at = str(
                getattr(domain, "last_build", None) or info.get("last_build") or ""
            ).strip() or None
        return {
            "supported": supported,
            "backend": backend,
            "enabled": enabled,
            "state": state,
            "refreshed_at": refreshed_at,
            "companions": companions,
        }

    @staticmethod
    def _intelligence_block(
        inferred_count: Any, communities: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        stats = communities or {}
        return {
            "inferred_triples": inferred_count,
            "communities": {
                "cluster_count": int(stats.get("cluster_count") or 0),
                "node_count": int(stats.get("node_count") or 0),
                "algorithm": str(stats.get("algorithm") or ""),
                "ready": bool(stats.get("ready")),
            },
        }
