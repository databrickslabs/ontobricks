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

            return {
                "metadata": toolkit.metadata(),
                "selected_pitfalls": list(results.keys()),
                "results": results,
                "grouped_results": grouped,
            }
        finally:
            import os
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
