"""Saved Detect runs, kept as a catalog snapshot in a Unity Catalog table.

Save stores **one row per pattern** from the last Detect run into
``graph_pattern_saved_<slug>``: label, focus, KPIs (hops / support /
type and relation counts), seed label, shape tokens and the diagram JSON.
Nothing is written to the RDF graph. Rows are independent of the Detect
output tables, so the next Detect (which overwrites those) leaves the
catalog and diagrams intact.

Members are **not** copied into the snapshot. The members modal reads them
from the live Detect output (``graph_patterns_<slug>``) while that run is
still current; after a new Detect the pattern ids change and the modal is
empty for older saves. Cardinalities (``support`` / ``size``) stay on the
snapshot so list and details pages still show the right counts.

Legacy saves (pre-forward-catalog) stored one row per member with the
diagram JSON duplicated on every row. Readers still understand those
rows; new saves never write that shape.
"""

from __future__ import annotations

import json
import re
from typing import Any, Callable, Dict, List, Optional

from back.core.errors import ValidationError
from back.core.graph_analysis.pattern.JobPatterns import (
    DEFAULT_MEMBERS_PAGE,
    MEMBERS_PAGE_MAX,
    fetch_shape_edges,
    shape_query,
)
from back.core.graph_analysis.pattern.pattern_diagram import (
    build_pattern_diagram,
    local_label as _local_label,
)
from back.core.logging import get_logger

logger = get_logger(__name__)

_SAVED_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,80}$")

_KPI_COLUMNS = (
    ("hop_count", "INT"),
    ("support", "INT"),
    ("type_count", "INT"),
    ("relation_count", "INT"),
    ("pattern_count", "INT"),
)

_SHAPE_COLUMNS = (
    ("seed_label", "STRING"),
    ("shape_json", "STRING"),
    ("diagram_json", "STRING"),
    ("focus_pattern_id", "STRING"),
)


def slug_saved_id(label: str, pattern_id: str) -> str:
    """Build a stable ``[a-z0-9-]`` id from *label* + a short hash suffix."""
    base = re.sub(r"[^a-z0-9]+", "-", (label or "").lower()).strip("-")
    base = (base or "pattern")[:40].strip("-") or "pattern"
    suffix = re.sub(r"[^a-z0-9]", "", (pattern_id or "").lower())[:8] or "00000000"
    return f"{base}-{suffix}"


def _lit(value: str) -> str:
    """Quote *value* as a Spark SQL string literal (escapes ``\\`` and ``'``)."""
    escaped = (value or "").replace("\\", "\\\\").replace("'", "''")
    return f"'{escaped}'"


def _diagram_needs_rebuild(diagram: Any, hop_count: int) -> bool:
    """True when the diagram is missing, anchor-only, or badly keyed.

    Rebuild when hop ≥ 1 nodes have no incoming edge (legacy empty-via
    orphans) or when the same type appears twice in one hop column (older
    ``hop|via|type`` node ids).
    """
    if not isinstance(diagram, dict) or not diagram.get("nodes"):
        return True
    if hop_count <= 0:
        return False
    nodes = diagram.get("nodes") or []
    if not any(int(n.get("hop") or 0) > 0 for n in nodes):
        return True
    targets = {e.get("target") for e in (diagram.get("edges") or [])}
    if any(int(n.get("hop") or 0) > 0 and n.get("id") not in targets for n in nodes):
        return True
    seen: set[tuple[int, str]] = set()
    for node in nodes:
        key = (int(node.get("hop") or 0), str(node.get("type") or ""))
        if key in seen:
            return True
        seen.add(key)
    return False


class SavedPatternStore:
    """Save, list, page and delete saved Detect runs through the Build warehouse.

    *query* runs a SELECT and returns rows; *execute* runs DDL/DML.
    """

    def __init__(
        self,
        output_table: str,
        saved_table: str,
        *,
        query: Callable[[str], Any],
        execute: Callable[[str], Any],
    ) -> None:
        self._out = output_table
        self._saved = saved_table
        self._query = query
        self._execute = execute
        self._table_ready = False

    @property
    def saved_table(self) -> str:
        return self._saved

    def save(self, pattern_id: str, label: str) -> Dict[str, Any]:
        """Snapshot the Detect catalog (patterns + cardinalities + diagrams).

        *pattern_id* is the pattern focused when Save ran (default selection on
        reopen). One row is written per pattern in ``_shapes`` — never the
        member list. Members stay on the live Detect table.
        """
        label = (label or "").strip() or "Pattern"
        focus_id = (pattern_id or "").strip()
        if not focus_id:
            raise ValidationError("pattern_id is required")
        self._ensure_table()
        pattern_ids = self._detect_pattern_ids(focus_id)
        if not pattern_ids:
            raise ValidationError(f"Unknown or empty pattern_id: {focus_id}")
        live = self._live_metrics_map()
        summary = self._safe_rows(
            f"SELECT pattern_count, anchor_type, bins FROM {self._out}_summary"
        )
        stats = summary[0] if summary else {}
        run_count = _as_int(stats.get("pattern_count")) or len(pattern_ids)
        shapes: List[Dict[str, Any]] = []
        total_size = 0
        diagrams = 0
        focus_metrics: Dict[str, int] = {}
        for pid in pattern_ids:
            metrics = self._metrics_from(live, pid, run_count)
            if metrics["support"] <= 0:
                continue
            shape = self._live_shape_payload(pid, stats=stats) or {}
            shapes.append({"pattern_id": pid, "metrics": metrics, "shape": shape})
            total_size += metrics["support"]
            if shape.get("diagram"):
                diagrams += 1
            if pid == focus_id:
                focus_metrics = metrics
        if not shapes:
            raise ValidationError(f"Unknown or empty pattern_id: {focus_id}")
        saved_id = self._allocate_saved_id(label, focus_id)
        try:
            self._execute(
                self._insert_catalog_sql(saved_id, label, focus_id, shapes)
            )
        except Exception:
            try:
                self._execute(
                    f"DELETE FROM {self._saved} WHERE saved_id = {_lit(saved_id)}"
                )
            except Exception:  # noqa: BLE001
                pass
            raise
        logger.info(
            "SavedPatternStore: saved %s (%d patterns, %d members by support, "
            "%d diagrams)",
            saved_id,
            len(shapes),
            total_size,
            diagrams,
        )
        return {
            "saved_id": saved_id,
            "label": label,
            "pattern_id": focus_id,
            "size": total_size,
            "pattern_count": len(shapes),
            "has_diagram": diagrams > 0,
            **focus_metrics,
        }

    def list_saved(self) -> List[Dict[str, Any]]:
        self._ensure_table()
        # One row per (saved_id, pattern_id) first so legacy member rows and
        # catalog rows share the same size = sum(support) semantics.
        sql = (
            "SELECT saved_id, max(saved_label) AS label, "
            "max(focus_pattern_id) AS focus_pattern_id, "
            "min(pattern_id) AS pattern_id, "
            "sum(support) AS size, "
            "count(*) AS pattern_count, "
            "min(saved_at) AS saved_at, "
            "max(hop_count) AS hop_count, max(support) AS support, "
            "max(type_count) AS type_count, max(relation_count) AS relation_count "
            f"FROM ("
            f"  SELECT saved_id, saved_label, focus_pattern_id, pattern_id, "
            f"         max(support) AS support, max(hop_count) AS hop_count, "
            f"         max(type_count) AS type_count, "
            f"         max(relation_count) AS relation_count, "
            f"         min(saved_at) AS saved_at "
            f"  FROM {self._saved} "
            f"  GROUP BY saved_id, saved_label, focus_pattern_id, pattern_id"
            f") p GROUP BY saved_id ORDER BY label, saved_id"
        )
        out = []
        for row in self._rows(sql):
            sid = str(row.get("saved_id") or "")
            if not sid:
                continue
            size = _as_int(row.get("size"))
            focus = str(row.get("focus_pattern_id") or "") or str(
                row.get("pattern_id") or ""
            )
            out.append(
                {
                    "id": sid,
                    "label": str(row.get("label") or "") or sid,
                    "pattern_id": focus,
                    "size": size,
                    "saved_at": str(row.get("saved_at") or ""),
                    "hop_count": _as_int(row.get("hop_count")),
                    "support": _as_int(row.get("support")) or size,
                    "type_count": _as_int(row.get("type_count")),
                    "relation_count": _as_int(row.get("relation_count")),
                    "pattern_count": _as_int(row.get("pattern_count")),
                }
            )
        self._enrich_missing_kpis(out)
        return out

    def patterns(self, saved_id: str) -> Dict[str, Any]:
        """Catalog of every pattern snapshotted under *saved_id* (Detect picker)."""
        self._check_saved_id(saved_id)
        self._ensure_table()
        rows = self._rows(
            "SELECT pattern_id AS id, "
            "max(seed_label) AS label, max(support) AS support, "
            "max(hop_count) AS hop_count, max(type_count) AS type_count, "
            "max(relation_count) AS relation_count, "
            "max(focus_pattern_id) AS focus_pattern_id, "
            "max(support) AS size "
            f"FROM {self._saved} WHERE saved_id = {_lit(saved_id)} "
            "GROUP BY pattern_id ORDER BY support DESC, pattern_id"
        )
        patterns = []
        focus = ""
        for row in rows:
            pid = str(row.get("id") or "")
            if not pid:
                continue
            if not focus:
                focus = str(row.get("focus_pattern_id") or "") or pid
            label = str(row.get("label") or "") or pid[:8]
            patterns.append(
                {
                    "id": pid,
                    "label": label,
                    "support": _as_int(row.get("support")) or _as_int(row.get("size")),
                    "hop_count": _as_int(row.get("hop_count")),
                    "type_count": _as_int(row.get("type_count")),
                    "relation_count": _as_int(row.get("relation_count")),
                    "size": _as_int(row.get("size")),
                }
            )
        return {
            "saved_id": saved_id,
            "focus_pattern_id": focus,
            "patterns": patterns,
            "count": len(patterns),
        }

    def members(
        self,
        saved_id: str,
        offset: int = 0,
        limit: int = DEFAULT_MEMBERS_PAGE,
        pattern_id: str = "",
    ) -> Dict[str, Any]:
        """Page of members for a saved run.

        Catalog saves keep no member rows: members are read from the live
        Detect output while that run is still current. Legacy member-row
        snapshots still page from the saved table.
        """
        self._check_saved_id(saved_id)
        self._ensure_table()
        page = max(1, min(_as_int(limit) or DEFAULT_MEMBERS_PAGE, MEMBERS_PAGE_MAX))
        start = max(0, _as_int(offset))
        pid = (pattern_id or "").strip()
        if self._has_member_rows(saved_id, pid):
            return self._members_from_saved(saved_id, pid, start, page)
        return self._members_from_detect(saved_id, pid, start, page)

    def shape(
        self, saved_id: str, pattern_id: str = ""
    ) -> Optional[Dict[str, Any]]:
        """Return the snapshotted shape/diagram for one pattern in *saved_id*."""
        self._check_saved_id(saved_id)
        self._ensure_table()
        pid = (pattern_id or "").strip()
        if not pid:
            catalog = self.patterns(saved_id)
            pid = str(catalog.get("focus_pattern_id") or "")
            if not pid and catalog.get("patterns"):
                pid = str(catalog["patterns"][0]["id"])
        if not pid:
            return None
        rows = self._safe_rows(
            "SELECT pattern_id, "
            "max(hop_count) AS hop_count, max(support) AS support, "
            "max(type_count) AS type_count, max(relation_count) AS relation_count, "
            "max(seed_label) AS seed_label, max(shape_json) AS shape_json, "
            "max(diagram_json) AS diagram_json "
            f"FROM {self._saved} WHERE saved_id = {_lit(saved_id)} "
            f"AND pattern_id = {_lit(pid)} GROUP BY pattern_id"
        )
        if not rows:
            return None
        row = rows[0]
        pattern_id = str(row.get("pattern_id") or pid)
        hop_count = _as_int(row.get("hop_count"))
        diagram = _parse_json(row.get("diagram_json"))
        # Anchor-only diagrams happen when Save ran against Detect tables that
        # still lacked ``parent_via``: the edge query failed, `_safe_rows`
        # returned [], and the snapshot stored a single hop-0 node. Rebuild
        # from the live Detect tables when those still match this pattern.
        if _diagram_needs_rebuild(diagram, hop_count):
            live = self._live_shape_payload(pattern_id)
            if live and live.get("diagram") and not _diagram_needs_rebuild(
                live.get("diagram"), _as_int(live.get("hop_count")) or hop_count
            ):
                diagram = live["diagram"]
                self._persist_shape(
                    saved_id,
                    pattern_id,
                    seed_label=str(live.get("seed_label") or ""),
                    shape_json=str(live.get("shape_json") or "[]"),
                    diagram_json=json.dumps(diagram, separators=(",", ":")),
                )
            elif not isinstance(diagram, dict) or not diagram.get("nodes"):
                diagram = None
        return {
            "saved_id": saved_id,
            "pattern_id": pattern_id,
            "support": _as_int(row.get("support")),
            "hop_count": hop_count,
            "seed_label": str(row.get("seed_label") or ""),
            "shape_json": str(row.get("shape_json") or "[]"),
            "diagram": diagram,
            "_typeCount": _as_int(row.get("type_count")),
            "_relationCount": _as_int(row.get("relation_count")),
            "_fromSaved": True,
        }

    def delete(self, saved_id: str) -> int:
        """Drop the rows of one saved Detect run. Returns the number of rows removed."""
        self._check_saved_id(saved_id)
        self._ensure_table()
        total = self._member_count(saved_id)
        if total <= 0:
            return 0
        self._execute(f"DELETE FROM {self._saved} WHERE saved_id = {_lit(saved_id)}")
        return total

    def _ensure_table(self) -> None:
        if self._table_ready:
            return
        self._execute(
            f"CREATE TABLE IF NOT EXISTS {self._saved} ("
            "saved_id STRING, saved_label STRING, pattern_id STRING, "
            "focus_pattern_id STRING, "
            "anchor_uri STRING, anchor_label STRING, type_uri STRING, "
            "hop_count INT, support INT, type_count INT, relation_count INT, "
            "pattern_count INT, seed_label STRING, shape_json STRING, "
            "diagram_json STRING, saved_at TIMESTAMP) USING DELTA"
        )
        self._migrate_extra_columns()
        self._table_ready = True

    def _migrate_extra_columns(self) -> None:
        """Add KPI/shape columns on tables created before they were in the schema."""
        existing = self._column_names()
        for name, typ in _KPI_COLUMNS + _SHAPE_COLUMNS:
            if name.lower() in existing:
                continue
            try:
                self._execute(f"ALTER TABLE {self._saved} ADD COLUMN {name} {typ}")
            except Exception:  # noqa: BLE001 — race / already present
                pass

    def _column_names(self) -> set[str]:
        rows = self._safe_rows(f"DESCRIBE TABLE {self._saved}")
        names = {
            str(r.get("col_name") or r.get("name") or "").lower()
            for r in rows
            if (r.get("col_name") or r.get("name"))
        }
        if names:
            return names
        # SQLite test harness (and some dialects) use PRAGMA table_info.
        bare = self._saved.rsplit(".", 1)[-1]
        rows = self._safe_rows(f"PRAGMA table_info({bare})")
        return {str(r.get("name") or "").lower() for r in rows if r.get("name")}

    def _rows(self, sql: str) -> List[Dict[str, Any]]:
        return list(self._query(sql) or [])

    def _safe_rows(self, sql: str) -> List[Dict[str, Any]]:
        try:
            return self._rows(sql)
        except Exception:  # noqa: BLE001 — shapes/summary may be missing
            return []

    def _output_size(self, pattern_id: str) -> int:
        rows = self._rows(
            f"SELECT COUNT(*) AS n FROM {self._out} WHERE pattern_id = {_lit(pattern_id)}"
        )
        return _as_int(rows[0].get("n")) if rows else 0

    def _detect_pattern_ids(self, focus_id: str) -> List[str]:
        """All pattern ids from the last Detect run (shapes), focus first if missing."""
        rows = self._safe_rows(
            f"SELECT pattern_id FROM {self._out}_shapes "
            "ORDER BY support DESC, pattern_id"
        )
        ids: List[str] = []
        seen: set[str] = set()
        for row in rows:
            pid = str(row.get("pattern_id") or "").strip()
            if not pid or pid in seen:
                continue
            seen.add(pid)
            ids.append(pid)
        if focus_id and focus_id not in seen:
            # Unknown focus → reject (do not silently save unrelated patterns).
            if self._output_size(focus_id) <= 0:
                return []
            ids.insert(0, focus_id)
        elif focus_id and focus_id in seen:
            ids = [focus_id] + [p for p in ids if p != focus_id]
        elif not ids and focus_id and self._output_size(focus_id) > 0:
            ids = [focus_id]
        return ids

    def _member_count(self, saved_id: str) -> int:
        return self._member_count_where(f"saved_id = {_lit(saved_id)}")

    def _member_count_where(self, where: str) -> int:
        rows = self._rows(f"SELECT COUNT(*) AS n FROM {self._saved} WHERE {where}")
        return _as_int(rows[0].get("n")) if rows else 0

    def _has_member_rows(self, saved_id: str, pattern_id: str = "") -> bool:
        """True when this save still holds legacy per-member rows."""
        where = f"saved_id = {_lit(saved_id)} AND coalesce(anchor_uri, '') <> ''"
        if pattern_id:
            where += f" AND pattern_id = {_lit(pattern_id)}"
        rows = self._safe_rows(f"SELECT 1 AS n FROM {self._saved} WHERE {where} LIMIT 1")
        return bool(rows)

    def _members_from_saved(
        self, saved_id: str, pattern_id: str, start: int, page: int
    ) -> Dict[str, Any]:
        where = f"saved_id = {_lit(saved_id)} AND coalesce(anchor_uri, '') <> ''"
        if pattern_id:
            where += f" AND pattern_id = {_lit(pattern_id)}"
        total = self._member_count_where(where)
        rows = self._rows(
            "SELECT anchor_uri AS uri, anchor_label AS label, type_uri "
            f"FROM {self._saved} WHERE {where} "
            f"ORDER BY anchor_uri LIMIT {page} OFFSET {start}"
        )
        return self._members_payload(saved_id, pattern_id, start, page, total, rows)

    def _members_from_detect(
        self, saved_id: str, pattern_id: str, start: int, page: int
    ) -> Dict[str, Any]:
        """Page members from the live Detect output for patterns in this save."""
        catalog = self.patterns(saved_id)
        allowed = {str(p["id"]) for p in catalog.get("patterns") or []}
        if pattern_id:
            if pattern_id not in allowed:
                return self._members_payload(saved_id, pattern_id, start, page, 0, [])
            where = f"pattern_id = {_lit(pattern_id)}"
        elif allowed:
            listed = ", ".join(_lit(pid) for pid in sorted(allowed))
            where = f"pattern_id IN ({listed})"
        else:
            return self._members_payload(saved_id, pattern_id, start, page, 0, [])
        try:
            total_rows = self._rows(
                f"SELECT COUNT(*) AS n FROM {self._out} WHERE {where}"
            )
            total = _as_int(total_rows[0].get("n")) if total_rows else 0
            rows = self._rows(
                "SELECT anchor_uri AS uri, label, type_uri "
                f"FROM {self._out} WHERE {where} "
                f"ORDER BY anchor_uri LIMIT {page} OFFSET {start}"
            )
        except Exception:  # noqa: BLE001 — Detect tables may be gone
            return self._members_payload(saved_id, pattern_id, start, page, 0, [])
        return self._members_payload(saved_id, pattern_id, start, page, total, rows)

    @staticmethod
    def _members_payload(
        saved_id: str,
        pattern_id: str,
        start: int,
        page: int,
        total: int,
        rows: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        members = []
        for row in rows:
            uri = str(row.get("uri") or "")
            type_uri = str(row.get("type_uri") or "")
            members.append(
                {
                    "uri": uri,
                    "label": str(row.get("label") or "") or _local_label(uri),
                    "type": type_uri,
                    "type_label": _local_label(type_uri),
                }
            )
        return {
            "saved_id": saved_id,
            "pattern_id": pattern_id,
            "total": total,
            "offset": start,
            "limit": page,
            "members": members,
            "has_more": start + len(members) < total,
        }

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

    def _metrics_from(
        self, live: Dict[str, Dict[str, int]], pattern_id: str, run_count: int
    ) -> Dict[str, int]:
        metrics = dict(live.get(pattern_id) or {})
        support = _as_int(metrics.get("support"))
        if support <= 0:
            support = self._output_size(pattern_id)
        return {
            "hop_count": _as_int(metrics.get("hop_count")),
            "support": support,
            "type_count": _as_int(metrics.get("type_count")) or 1,
            "relation_count": _as_int(metrics.get("relation_count")),
            "pattern_count": _as_int(metrics.get("pattern_count")) or run_count,
        }

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

    def _live_shape_payload(
        self, pattern_id: str, *, stats: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Build the Discover shape+diagram payload from the live Detect tables."""
        rows = self._safe_rows(shape_query(self._out, pattern_id))
        if not rows:
            return None
        row = rows[0]
        raw = str(row.get("shape_json") or "[]")
        tokens = _parse_json(raw)
        if not isinstance(tokens, list):
            tokens = []
        support = _as_int(row.get("support"))
        try:
            edge_rows = fetch_shape_edges(self._query, self._out, pattern_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "SavedPatternStore: shape_edges read failed for %s: %s",
                pattern_id,
                exc,
            )
            edge_rows = None
        if stats is None:
            summary = self._safe_rows(
                f"SELECT anchor_type, bins FROM {self._out}_summary"
            )
            stats = summary[0] if summary else {}
        diagram = None
        if edge_rows is not None:
            try:
                diagram = build_pattern_diagram(
                    anchor_type=str(stats.get("anchor_type") or ""),
                    support=support,
                    edge_rows=edge_rows,
                    tokens=[str(t) for t in tokens],
                    bins=_as_int(stats.get("bins")),
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "SavedPatternStore: diagram build failed for %s: %s",
                    pattern_id,
                    exc,
                )
                diagram = None
        return {
            "pattern_id": pattern_id,
            "support": support,
            "hop_count": _as_int(row.get("hop_count")),
            "seed_label": str(row.get("seed_label") or ""),
            "shape_json": raw,
            "diagram": diagram,
        }

    def _persist_shape(
        self,
        saved_id: str,
        pattern_id: str,
        *,
        seed_label: str,
        shape_json: str,
        diagram_json: str,
    ) -> None:
        try:
            self._execute(
                f"UPDATE {self._saved} SET "
                f"seed_label = {_lit(seed_label)}, "
                f"shape_json = {_lit(shape_json)}, "
                f"diagram_json = {_lit(diagram_json)} "
                f"WHERE saved_id = {_lit(saved_id)} "
                f"AND pattern_id = {_lit(pattern_id)}"
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "SavedPatternStore: could not persist shape for %s/%s: %s",
                saved_id,
                pattern_id,
                exc,
            )

    def _insert_catalog_sql(
        self,
        saved_id: str,
        label: str,
        focus_id: str,
        shapes: List[Dict[str, Any]],
    ) -> str:
        """One INSERT with one SELECT row per pattern (no member fan-out)."""
        selects = []
        for item in shapes:
            pid = str(item["pattern_id"])
            metrics = item["metrics"]
            shape = item["shape"]
            diagram = shape.get("diagram")
            diagram_json = (
                json.dumps(diagram, separators=(",", ":")) if diagram else ""
            )
            seed_label = str(shape.get("seed_label") or "")
            shape_json = str(shape.get("shape_json") or "[]")
            selects.append(
                f"SELECT {_lit(saved_id)} AS saved_id, {_lit(label)} AS saved_label, "
                f"{_lit(pid)} AS pattern_id, {_lit(focus_id)} AS focus_pattern_id, "
                f"CAST(NULL AS STRING) AS anchor_uri, "
                f"CAST(NULL AS STRING) AS anchor_label, "
                f"CAST(NULL AS STRING) AS type_uri, "
                f"{int(metrics['hop_count'])} AS hop_count, "
                f"{int(metrics['support'])} AS support, "
                f"{int(metrics['type_count'])} AS type_count, "
                f"{int(metrics['relation_count'])} AS relation_count, "
                f"{int(metrics['pattern_count'])} AS pattern_count, "
                f"{_lit(seed_label)} AS seed_label, "
                f"{_lit(shape_json)} AS shape_json, "
                f"{_lit(diagram_json)} AS diagram_json, "
                f"current_timestamp() AS saved_at"
            )
        body = "\nUNION ALL\n".join(selects)
        return (
            f"INSERT INTO {self._saved} "
            "(saved_id, saved_label, pattern_id, focus_pattern_id, "
            "anchor_uri, anchor_label, type_uri, hop_count, support, "
            "type_count, relation_count, pattern_count, seed_label, "
            "shape_json, diagram_json, saved_at)\n"
            f"{body}"
        )

    def _allocate_saved_id(self, label: str, pattern_id: str) -> str:
        base = slug_saved_id(label, pattern_id)
        existing = {
            str(row.get("saved_id") or "")
            for row in self._safe_rows(
                f"SELECT DISTINCT saved_id FROM {self._saved}"
            )
        }
        if base not in existing:
            return base
        for i in range(2, 10_000):
            candidate = f"{base}-{i}"
            if candidate not in existing:
                return candidate
        raise RuntimeError("could not allocate a unique pattern saved_id")

    @staticmethod
    def _check_saved_id(saved_id: str) -> None:
        if not _SAVED_ID_RE.match(saved_id or ""):
            raise ValidationError(f"Invalid saved pattern id: {saved_id!r}")


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _parse_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None
