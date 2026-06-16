"""PitfallsService — bridge between OntoBricks rdflib Graph and OntologyPatternToolkit.

Takes the in-memory rdflib Graph from the current domain session, serializes it
to a temporary TTL file, and runs the requested pitfall patterns against it.
"""
from __future__ import annotations

import tempfile
from typing import Any, Dict, List, Optional

from rdflib import Graph

from back.core.logging import get_logger

logger = get_logger(__name__)

# Severity weights per pitfall: P1.x (Logical) are critical, P4.x (Semantic) are minor.
_PITFALL_WEIGHTS: Dict[str, int] = {
    "P1.1": 5, "P1.2": 4, "P1.3": 5,
    "P2.1": 3, "P2.2": 2, "P2.3": 2, "P2.4": 1, "P2.5": 2, "P2.6": 1,
    "P3.1": 1, "P3.2": 1, "P3.3": 1,
    "P4.1": 1, "P4.2": 1, "P4.3": 1, "P4.4": 2, "P4.5": 1, "P4.6": 1, "P4.7": 1,
}
_MAX_WEIGHT = sum(_PITFALL_WEIGHTS.values())  # 36


def _extract_issue_count(pitfall_id: str, result: Dict[str, Any]) -> int:
    """Return the primary issue count for a pitfall result dict."""
    if "count" in result and isinstance(result["count"], int):
        return result["count"]
    # P2.5 uses multi_domain_count + multi_range_count
    if "multi_domain_count" in result or "multi_range_count" in result:
        return (result.get("multi_domain_count") or 0) + (result.get("multi_range_count") or 0)
    return 0


_MAX_OCCURRENCES_PER_PITFALL = 5  # cap per-pattern count for normalization


def compute_precision_score(
    results: Dict[str, Dict[str, Any]],
    metadata: Dict[str, Any],
) -> int:
    """Compute a 0–100 ontology precision score from pitfall results.

    Critical pitfalls (P1.x) are weighted more heavily than minor ones (P4.x).
    Each pitfall's contribution is capped at _MAX_OCCURRENCES_PER_PITFALL so that a
    single noisy pattern cannot dominate the score.  The denominator is fixed
    (_MAX_WEIGHT × _MAX_OCCURRENCES_PER_PITFALL), making even a single minor warning
    produce a score strictly below 100.

    Returns:
        Integer score in [0, 100].  100 means zero pitfall occurrences found.
    """
    penalty = 0
    for pid, weight in _PITFALL_WEIGHTS.items():
        if pid not in results:
            continue
        count = _extract_issue_count(pid, results[pid])
        penalty += weight * min(count, _MAX_OCCURRENCES_PER_PITFALL)

    max_penalty = _MAX_WEIGHT * _MAX_OCCURRENCES_PER_PITFALL  # 36 × 5 = 180
    score = max(0, round(100 * (1 - penalty / max_penalty)))
    return score


def _group_results_by_category(
    selected_pitfalls: List[str],
    results: Dict[str, Dict[str, Any]],
    taxonomy: List[Dict[str, str]],
) -> Dict[str, Any]:
    """Group flat results dict by category, preserving taxonomy order."""
    grouped: Dict[str, Dict[str, Any]] = {}
    pitfall_map = {entry["pitfall_id"]: entry for entry in taxonomy}

    for pitfall_id in selected_pitfalls:
        meta = pitfall_map.get(pitfall_id, {})
        category = meta.get("category", "Unknown")
        title = meta.get("title", pitfall_id)

        if category not in grouped:
            grouped[category] = {}

        grouped[category][pitfall_id] = {
            "title": title,
            "result": results.get(pitfall_id, {"count": 0, "items": []}),
        }

    return grouped


class PitfallsService:
    """Run D2KLab ontology pitfall checks against an rdflib Graph."""

    def get_taxonomy(self) -> List[Dict[str, str]]:
        """Return the 19-pitfall taxonomy (no Graph needed, no heavy deps)."""
        from back.core.external.pitfalls.runner import PITFALL_TAXONOMY
        return [dict(e) for e in PITFALL_TAXONOMY]

    def get_available_patterns(self) -> List[str]:
        """Return sorted list of all available pitfall IDs."""
        from back.core.external.pitfalls.runner import OntologyPatternToolkit
        return OntologyPatternToolkit.available_patterns()

    def run_analysis(
        self,
        graph: Graph,
        patterns: Optional[List[str]] = None,
        model_name: str = "all-MiniLM-L6-v2",
    ) -> Dict[str, Any]:
        """Serialize *graph* to a temp TTL file then run pitfall analysis.

        Returns a dict with keys: metadata, selected_pitfalls, results, grouped_results.
        Raises ImportError if the pitfalls optional deps are missing.
        Raises ValueError for unknown pattern IDs.
        """
        from back.core.external.pitfalls.runner import OntologyPatternToolkit, _DEPS_AVAILABLE

        if not _DEPS_AVAILABLE:
            raise ImportError(
                "Pitfall detection requires optional dependencies. "
                "Install with: pip install .[pitfalls]"
            )

        if patterns is None:
            patterns = ["all"]

        with tempfile.NamedTemporaryFile(suffix=".ttl", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            graph.serialize(destination=tmp_path, format="turtle")
            logger.debug("PitfallsService: serialized graph to %s", tmp_path)

            from back.core.external.pitfalls.runner import OntologyPatternToolkit as _Toolkit
            toolkit = _Toolkit(tmp_path, model_name=model_name)
            selected = _Toolkit.available_patterns() if "all" in [p.upper() for p in patterns] else patterns
            results = toolkit.run_patterns(selected)

            grouped = _group_results_by_category(
                list(results.keys()),
                results,
                _Toolkit.pitfall_taxonomy(),
            )

            meta = toolkit.metadata()
            precision_score = compute_precision_score(results, meta)

            return {
                "metadata": meta,
                "selected_pitfalls": list(results.keys()),
                "results": results,
                "grouped_results": grouped,
                "precision_score": precision_score,
            }
        finally:
            import os
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
