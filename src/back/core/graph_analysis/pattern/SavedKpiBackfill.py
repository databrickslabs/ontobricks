"""KPI backfill for saved Detect runs written before the snapshot columns existed.

Split from :class:`SavedPatternStore` (size budget). It is a mixin: it relies on
the store's ``_out`` / ``_saved`` table names and its ``_safe_rows`` /
``_execute`` helpers.
"""

from __future__ import annotations

from typing import Any, Dict, List

from back.core.graph_analysis.pattern.saved_pattern_sql import as_int as _as_int
from back.core.graph_analysis.pattern.saved_pattern_sql import sql_lit as _lit
from back.core.logging import get_logger

logger = get_logger(__name__)


class SavedKpiBackfill:
    """Fills (and persists) missing hop / support / type / relation KPIs."""

    def _kpis_incomplete(self, item: Dict[str, Any]) -> bool:
        return not (
            _as_int(item.get("hop_count"))
            and _as_int(item.get("type_count"))
            and _as_int(item.get("relation_count"))
            and _as_int(item.get("pattern_count"))
        )

    def _live_metrics_map(self) -> Dict[str, Dict[str, int]]:
        """pattern_id → Discover KPIs from the latest Detect tables."""
        summary = self._safe_rows(f"SELECT pattern_count FROM {self._out}_summary")
        pattern_count = _as_int(summary[0].get("pattern_count")) if summary else 0
        shapes = self._safe_rows(
            f"SELECT pattern_id, hop_count, support FROM {self._out}_shapes"
        )
        edges = self._safe_rows(
            "SELECT pattern_id, parent_type, child_type "
            f"FROM {self._out}_shape_edges"
        )
        types_by: Dict[str, set[str]] = {}
        rel_by: Dict[str, int] = {}
        for edge in edges:
            pid = str(edge.get("pattern_id") or "")
            if not pid:
                continue
            rel_by[pid] = rel_by.get(pid, 0) + 1
            bucket = types_by.setdefault(pid, set())
            parent = str(edge.get("parent_type") or "")
            child = str(edge.get("child_type") or "")
            if parent:
                bucket.add(parent)
            if child:
                bucket.add(child)
        out: Dict[str, Dict[str, int]] = {}
        for row in shapes:
            pid = str(row.get("pattern_id") or "")
            if not pid:
                continue
            types = types_by.get(pid) or set()
            out[pid] = {
                "hop_count": _as_int(row.get("hop_count")),
                "support": _as_int(row.get("support")),
                "type_count": len(types) or 1,
                "relation_count": rel_by.get(pid, 0),
                "pattern_count": pattern_count,
            }
        return out

    def _enrich_missing_kpis(self, items: List[Dict[str, Any]]) -> None:
        """Fill / persist KPIs for rows saved before the snapshot columns existed."""
        missing = [item for item in items if self._kpis_incomplete(item)]
        if not missing:
            return
        live = self._live_metrics_map()
        summary = self._safe_rows(f"SELECT pattern_count FROM {self._out}_summary")
        run_count = _as_int(summary[0].get("pattern_count")) if summary else 0
        for item in missing:
            metrics = dict(live.get(item["pattern_id"]) or {})
            if run_count and not metrics.get("pattern_count"):
                metrics["pattern_count"] = run_count
            if not metrics and not item.get("type_count"):
                continue
            changed = False
            for key in (
                "hop_count",
                "support",
                "type_count",
                "relation_count",
                "pattern_count",
            ):
                if _as_int(item.get(key)):
                    continue
                value = _as_int(metrics.get(key))
                if not value and key == "support":
                    value = _as_int(item.get("size"))
                if value:
                    item[key] = value
                    changed = True
            if changed:
                self._persist_kpis(item)

    def _persist_kpis(self, item: Dict[str, Any]) -> None:
        sid = str(item.get("id") or "")
        if not sid:
            return
        try:
            self._execute(
                f"UPDATE {self._saved} SET "
                f"hop_count = {int(item.get('hop_count') or 0)}, "
                f"support = {int(item.get('support') or item.get('size') or 0)}, "
                f"type_count = {int(item.get('type_count') or 0)}, "
                f"relation_count = {int(item.get('relation_count') or 0)}, "
                f"pattern_count = {int(item.get('pattern_count') or 0)} "
                f"WHERE saved_id = {_lit(sid)}"
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("SavedPatternStore: could not backfill KPIs for %s: %s", sid, exc)
