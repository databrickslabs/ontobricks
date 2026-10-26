"""Assemble and freshness-check Knowledge Graph stats payloads.

Extracted from ``api.routers.internal.dtwin.triplestore_stats``.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


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
        return bool(has_kind and has_job_reason and not inconsistent)

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
        }
