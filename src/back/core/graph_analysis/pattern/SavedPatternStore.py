"""Saved discovered patterns, kept as a snapshot in a Unity Catalog table.

Save copies the member rows of one discovered pattern (last Detect run) into
``graph_pattern_saved_<slug>``, next to the job output. Nothing is written to
the RDF graph. Saved rows are independent of the output table, so the next
Detect (which overwrites that table) leaves them intact.

One row per member::

    saved_id, saved_label, pattern_id, anchor_uri, anchor_label, type_uri, saved_at

A save is a single ``INSERT … SELECT``, which Delta applies atomically, so a
failed save never leaves a half-written pattern behind.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, List

from back.core.errors import ValidationError
from back.core.graph_analysis.pattern.JobPatterns import (
    DEFAULT_MEMBERS_PAGE,
    MEMBERS_PAGE_MAX,
)
from back.core.graph_analysis.pattern.pattern_diagram import (
    local_label as _local_label,
)
from back.core.logging import get_logger

logger = get_logger(__name__)

_SAVED_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,80}$")


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


class SavedPatternStore:
    """Save, list, page and delete saved patterns through the Build warehouse.

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
        """Snapshot the members of *pattern_id* (last Detect run) under *label*."""
        label = (label or "").strip() or "Pattern"
        pattern_id = (pattern_id or "").strip()
        if not pattern_id:
            raise ValidationError("pattern_id is required")
        self._ensure_table()
        size = self._output_size(pattern_id)
        if size <= 0:
            raise ValidationError(f"Unknown or empty pattern_id: {pattern_id}")
        saved_id = self._allocate_saved_id(label, pattern_id)
        self._execute(self._insert_sql(saved_id, label, pattern_id))
        logger.info("SavedPatternStore: saved %s (%d members)", saved_id, size)
        return {
            "saved_id": saved_id,
            "label": label,
            "pattern_id": pattern_id,
            "size": size,
        }

    def list_saved(self) -> List[Dict[str, Any]]:
        self._ensure_table()
        sql = (
            "SELECT saved_id, max(saved_label) AS label, "
            "min(pattern_id) AS pattern_id, count(*) AS size, "
            "min(saved_at) AS saved_at "
            f"FROM {self._saved} GROUP BY saved_id ORDER BY label, saved_id"
        )
        out = []
        for row in self._rows(sql):
            sid = str(row.get("saved_id") or "")
            if not sid:
                continue
            out.append(
                {
                    "id": sid,
                    "label": str(row.get("label") or "") or sid,
                    "pattern_id": str(row.get("pattern_id") or ""),
                    "size": _as_int(row.get("size")),
                    "saved_at": str(row.get("saved_at") or ""),
                }
            )
        return out

    def members(
        self, saved_id: str, offset: int = 0, limit: int = DEFAULT_MEMBERS_PAGE
    ) -> Dict[str, Any]:
        self._check_saved_id(saved_id)
        self._ensure_table()
        page = max(1, min(_as_int(limit) or DEFAULT_MEMBERS_PAGE, MEMBERS_PAGE_MAX))
        start = max(0, _as_int(offset))
        total = self._member_count(saved_id)
        rows = self._rows(
            "SELECT anchor_uri AS uri, anchor_label AS label, type_uri "
            f"FROM {self._saved} WHERE saved_id = {_lit(saved_id)} "
            f"ORDER BY anchor_uri LIMIT {page} OFFSET {start}"
        )
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
            "total": total,
            "offset": start,
            "limit": page,
            "members": members,
            "has_more": start + len(members) < total,
        }

    def delete(self, saved_id: str) -> int:
        """Drop the rows of one saved pattern. Returns the number of members removed."""
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
            "anchor_uri STRING, anchor_label STRING, type_uri STRING, "
            "saved_at TIMESTAMP) USING DELTA"
        )
        self._table_ready = True

    def _rows(self, sql: str) -> List[Dict[str, Any]]:
        return list(self._query(sql) or [])

    def _output_size(self, pattern_id: str) -> int:
        rows = self._rows(
            f"SELECT COUNT(*) AS n FROM {self._out} WHERE pattern_id = {_lit(pattern_id)}"
        )
        return _as_int(rows[0].get("n")) if rows else 0

    def _member_count(self, saved_id: str) -> int:
        rows = self._rows(
            f"SELECT COUNT(*) AS n FROM {self._saved} WHERE saved_id = {_lit(saved_id)}"
        )
        return _as_int(rows[0].get("n")) if rows else 0

    def _insert_sql(self, saved_id: str, label: str, pattern_id: str) -> str:
        return (
            f"INSERT INTO {self._saved} "
            "(saved_id, saved_label, pattern_id, anchor_uri, anchor_label, "
            "type_uri, saved_at) "
            f"SELECT {_lit(saved_id)}, {_lit(label)}, pattern_id, anchor_uri, "
            "coalesce(label, ''), coalesce(type_uri, ''), current_timestamp() "
            f"FROM {self._out} WHERE pattern_id = {_lit(pattern_id)}"
        )

    def _allocate_saved_id(self, label: str, pattern_id: str) -> str:
        base = slug_saved_id(label, pattern_id)
        existing = {row["id"] for row in self.list_saved()}
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
