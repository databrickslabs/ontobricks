"""Write and read named discovered patterns as RDF triples.

Each Save creates one pattern URI under ``…/pattern/{saved_id}``. Many saved
patterns coexist; delete clears only that URI's subject/object triples.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional

from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL
from back.core.graph_analysis.PatternVocabulary import PatternVocabulary
from back.core.logging import get_logger

logger = get_logger(__name__)

MEMBERS_PAGE_MAX = 500
DEFAULT_MEMBERS_PAGE = 100
_BATCH_SIZE = 50_000


def slug_saved_id(label: str, pattern_id: str) -> str:
    """Build a stable ``[a-z0-9_-]{1,}`` id from *label* + a short hash suffix."""
    base = re.sub(r"[^a-z0-9]+", "-", (label or "").lower()).strip("-")
    base = (base or "pattern")[:40].strip("-") or "pattern"
    suffix = re.sub(r"[^a-z0-9]", "", (pattern_id or "").lower())[:8] or "00000000"
    return f"{base}-{suffix}"


def _local_label(uri: str) -> str:
    tail = (uri or "").rstrip("/").rsplit("/", 1)[-1]
    return tail.rsplit("#", 1)[-1] or uri


def _bounded(value: Any, default: int, upper: int) -> int:
    try:
        n = int(value)
    except (TypeError, ValueError):
        n = default
    return max(1, min(n, upper))


class PatternMaterializer:
    """Materialise, list and delete named patterns on a graph store."""

    def __init__(self, store: Any, graph_name: str, base_uri: str) -> None:
        self._store = store
        self._graph_name = graph_name
        self._base_uri = base_uri or ""
        vocab = PatternVocabulary
        self._prefix = vocab.pattern_prefix(self._base_uri)
        self._in_pattern = vocab.in_pattern(self._base_uri)
        self._klass = vocab.pattern_class(self._base_uri)
        self._size_pred = vocab.pattern_size(self._base_uri)
        self._from_discovery = vocab.from_discovery(self._base_uri)

    def _esc(self, value: str) -> str:
        return (value or "").replace("'", "''")

    def _existing_saved_ids(self) -> set:
        ids = set()
        for row in self.list_saved():
            sid = str(row.get("id") or "")
            if sid:
                ids.add(sid)
        return ids

    def _allocate_saved_id(self, label: str, pattern_id: str) -> str:
        base = slug_saved_id(label, pattern_id)
        existing = self._existing_saved_ids()
        if base not in existing:
            return base
        for i in range(2, 10_000):
            candidate = f"{base}-{i}"
            if candidate not in existing:
                return candidate
        raise RuntimeError("could not allocate a unique pattern saved_id")

    def materialize(
        self,
        patterns: Any,
        pattern_id: str,
        label: str,
        on_progress: Optional[Callable[[int, str], None]] = None,
    ) -> Dict[str, Any]:
        """Write one named pattern from the last discovery run.

        Returns ``{saved_id, uri, inserted, size}``. On failure the new
        pattern URI is cleared so nothing half-written stays visible.
        """
        label = (label or "").strip() or "Pattern"
        pattern_id = (pattern_id or "").strip()
        if not pattern_id:
            raise ValueError("pattern_id is required")

        shape = patterns.shape(pattern_id) if hasattr(patterns, "shape") else None
        size = int((shape or {}).get("support", 0) or 0)
        if size <= 0:
            # Fall back to a members probe.
            page = patterns.members(pattern_id, offset=0, limit=1)
            size = int(page.get("total", 0) or 0)
        if size <= 0:
            raise ValueError(f"unknown or empty pattern_id: {pattern_id}")

        saved_id = self._allocate_saved_id(label, pattern_id)
        uri = PatternVocabulary.pattern(self._base_uri, saved_id)
        try:
            if callable(getattr(self._store, "insert_triples_from_select", None)):
                inserted = self._materialize_with_sql(
                    patterns.output_table, pattern_id, uri, label, size
                )
            else:
                inserted = self._materialize_in_batches(
                    patterns, pattern_id, uri, label, size, on_progress
                )
        except Exception:
            logger.exception("PatternMaterializer: materialize failed, clearing %s", saved_id)
            self.delete(saved_id)
            raise
        self._rebuild_adjacency(deleted=False, inserted=inserted)
        return {
            "saved_id": saved_id,
            "uri": uri,
            "inserted": inserted,
            "size": size,
            "label": label,
            "pattern_id": pattern_id,
        }

    def membership_select_sql(self, out: str, pattern_id: str, pattern_uri: str) -> str:
        return (
            f"SELECT anchor_uri AS subject, '{self._esc(self._in_pattern)}' AS predicate, "
            f"'{self._esc(pattern_uri)}' AS object FROM {out} "
            f"WHERE pattern_id = '{self._esc(pattern_id)}'"
        )

    def metadata_select_sql(
        self, pattern_uri: str, label: str, pattern_id: str, size: int
    ) -> str:
        uri = f"'{self._esc(pattern_uri)}'"
        rows = [
            (RDF_TYPE, f"'{self._esc(self._klass)}'"),
            (RDFS_LABEL, f"'{self._esc(label)}'"),
            (self._from_discovery, f"'{self._esc(pattern_id)}'"),
            (self._size_pred, f"'{int(size)}'"),
        ]
        return "\nUNION ALL\n".join(
            f"SELECT {uri} AS subject, '{self._esc(pred)}' AS predicate, {obj} AS object"
            for pred, obj in rows
        )

    def _materialize_with_sql(
        self, out: str, pattern_id: str, pattern_uri: str, label: str, size: int
    ) -> int:
        insert = self._store.insert_triples_from_select
        insert(self._graph_name, self.membership_select_sql(out, pattern_id, pattern_uri))
        insert(
            self._graph_name,
            self.metadata_select_sql(pattern_uri, label, pattern_id, size),
        )
        return size + 4

    def _materialize_in_batches(
        self,
        patterns: Any,
        pattern_id: str,
        pattern_uri: str,
        label: str,
        size: int,
        on_progress: Optional[Callable[[int, str], None]],
    ) -> int:
        meta = [
            {"subject": pattern_uri, "predicate": RDF_TYPE, "object": self._klass},
            {"subject": pattern_uri, "predicate": RDFS_LABEL, "object": label},
            {
                "subject": pattern_uri,
                "predicate": self._from_discovery,
                "object": pattern_id,
            },
            {
                "subject": pattern_uri,
                "predicate": self._size_pred,
                "object": str(size),
            },
        ]
        inserted = int(self._store.insert_triples(self._graph_name, meta) or 0)
        written = 0
        total = max(1, size)
        for batch in patterns.iter_memberships(pattern_id, _BATCH_SIZE):
            triples = [
                {
                    "subject": row["uri"],
                    "predicate": self._in_pattern,
                    "object": pattern_uri,
                }
                for row in batch
                if row.get("uri")
            ]
            inserted += int(self._store.insert_triples(self._graph_name, triples) or 0)
            written += len(batch)
            if on_progress:
                pct = 20 + int(70 * min(1.0, written / total))
                on_progress(pct, f"Writing memberships ({written:,} / {total:,})")
        return inserted

    def delete(self, saved_id: str) -> int:
        """Drop triples for one saved pattern. Returns rows deleted when known."""
        if self._store is None:
            return 0
        uri = PatternVocabulary.pattern(self._base_uri, saved_id)
        delete_fn = getattr(self._store, "delete_cohort_triples", None)
        deleted = 0
        if callable(delete_fn):
            try:
                # Prefix delete on the exact pattern URI (as subject) plus
                # memberships whose object is that URI.
                deleted = int(
                    delete_fn(self._graph_name, uri, self._in_pattern) or 0
                )
            except Exception as exc:
                logger.warning("PatternMaterializer: delete_cohort_triples failed: %s", exc)
        else:
            deleted = self._delete_by_scan(uri)
        if deleted:
            self._rebuild_adjacency(deleted=True, inserted=0)
        return deleted

    def _delete_by_scan(self, pattern_uri: str) -> int:
        scan = getattr(self._store, "query_triples", None)
        if not callable(scan):
            return 0
        rows = scan(self._graph_name) or []
        keep = [
            t
            for t in rows
            if not (
                t.get("subject") == pattern_uri
                or (
                    t.get("predicate") == self._in_pattern
                    and t.get("object") == pattern_uri
                )
            )
        ]
        deleted = len(rows) - len(keep)
        if deleted <= 0:
            return 0
        # Prefer a wholesale replace when the store exposes it; otherwise
        # delete via insert-after-clear is not available — leave a note.
        replace = getattr(self._store, "replace_triples", None)
        if callable(replace):
            replace(self._graph_name, keep)
            return deleted
        # Fall back: delete_community-style if present with exact URI.
        clear = getattr(self._store, "delete_triples_for_uri", None)
        if callable(clear):
            return int(clear(self._graph_name, pattern_uri, self._in_pattern) or deleted)
        # Last resort for test fakes that keep an in-memory list.
        if hasattr(self._store, "rows") and isinstance(self._store.rows, list):
            self._store.rows[:] = keep
            return deleted
        logger.warning(
            "PatternMaterializer: no delete path for %s on %s",
            pattern_uri,
            type(self._store).__name__,
        )
        return 0

    def list_saved(self) -> List[Dict[str, Any]]:
        """Named materializations: id, label, size, uri."""
        if self._is_sql():
            return self._list_saved_sql()
        return self._list_saved_scan()

    def _is_sql(self) -> bool:
        return getattr(self._store, "query_dialect", "sql") == "sql"

    def _rel(self) -> str:
        return self._store._sql_relation(self._graph_name)

    def _rows(self, sql: str) -> List[Dict[str, Any]]:
        return [
            {str(k).lower(): v for k, v in row.items()}
            for row in (self._store.execute_query(sql) or [])
        ]

    def _list_saved_sql(self) -> List[Dict[str, Any]]:
        rel = self._rel()
        sql = (
            f"SELECT t.subject AS uri, "
            f"coalesce(lb.object, '') AS label, "
            f"coalesce(sz.object, '0') AS size "
            f"FROM {rel} t "
            f"LEFT JOIN {rel} lb ON lb.subject = t.subject "
            f"AND lb.predicate = '{self._esc(RDFS_LABEL)}' "
            f"LEFT JOIN {rel} sz ON sz.subject = t.subject "
            f"AND sz.predicate = '{self._esc(self._size_pred)}' "
            f"WHERE t.predicate = '{self._esc(RDF_TYPE)}' "
            f"AND t.object = '{self._esc(self._klass)}' "
            f"ORDER BY label, uri"
        )
        out = []
        for row in self._rows(sql):
            uri = str(row.get("uri") or "")
            sid = PatternVocabulary.parse_saved_id(uri)
            if not sid:
                continue
            try:
                size = int(row.get("size") or 0)
            except (TypeError, ValueError):
                size = 0
            out.append(
                {
                    "id": sid,
                    "uri": uri,
                    "label": str(row.get("label") or "") or sid,
                    "size": size,
                }
            )
        return out

    def _list_saved_scan(self) -> List[Dict[str, Any]]:
        scan = getattr(self._store, "query_triples", None)
        if not callable(scan):
            return []
        rows = scan(self._graph_name) or []
        by_uri: Dict[str, Dict[str, Any]] = {}
        for t in rows:
            if t.get("predicate") == RDF_TYPE and t.get("object") == self._klass:
                uri = str(t.get("subject") or "")
                sid = PatternVocabulary.parse_saved_id(uri)
                if sid:
                    by_uri.setdefault(uri, {"id": sid, "uri": uri, "label": sid, "size": 0})
        for t in rows:
            uri = str(t.get("subject") or "")
            if uri not in by_uri:
                continue
            if t.get("predicate") == RDFS_LABEL:
                by_uri[uri]["label"] = str(t.get("object") or by_uri[uri]["label"])
            elif t.get("predicate") == self._size_pred:
                try:
                    by_uri[uri]["size"] = int(t.get("object") or 0)
                except (TypeError, ValueError):
                    by_uri[uri]["size"] = 0
        return sorted(by_uri.values(), key=lambda r: (r["label"], r["uri"]))

    def members(
        self, saved_id: str, offset: int = 0, limit: int = DEFAULT_MEMBERS_PAGE
    ) -> Dict[str, Any]:
        page = _bounded(limit, DEFAULT_MEMBERS_PAGE, MEMBERS_PAGE_MAX)
        start = max(0, int(offset or 0))
        uri = PatternVocabulary.pattern(self._base_uri, saved_id)
        if self._is_sql():
            return self._members_sql(saved_id, uri, start, page)
        return self._members_scan(saved_id, uri, start, page)

    def _members_sql(
        self, saved_id: str, pattern_uri: str, offset: int, limit: int
    ) -> Dict[str, Any]:
        rel = self._rel()
        count_sql = (
            f"SELECT COUNT(*) AS n FROM {rel} "
            f"WHERE predicate = '{self._esc(self._in_pattern)}' "
            f"AND object = '{self._esc(pattern_uri)}'"
        )
        rows = self._rows(count_sql)
        total = int(rows[0].get("n", 0) or 0) if rows else 0
        page_sql = (
            f"SELECT m.subject AS uri, "
            f"coalesce(lb.object, '') AS label, "
            f"coalesce(ty.object, '') AS type_uri "
            f"FROM {rel} m "
            f"LEFT JOIN {rel} lb ON lb.subject = m.subject "
            f"AND lb.predicate = '{self._esc(RDFS_LABEL)}' "
            f"LEFT JOIN {rel} ty ON ty.subject = m.subject "
            f"AND ty.predicate = '{self._esc(RDF_TYPE)}' "
            f"WHERE m.predicate = '{self._esc(self._in_pattern)}' "
            f"AND m.object = '{self._esc(pattern_uri)}' "
            f"ORDER BY m.subject "
            f"LIMIT {int(limit)} OFFSET {int(offset)}"
        )
        members = []
        for row in self._rows(page_sql):
            u = str(row.get("uri") or "")
            type_uri = str(row.get("type_uri") or "")
            members.append(
                {
                    "uri": u,
                    "label": str(row.get("label") or "") or _local_label(u),
                    "type": type_uri,
                    "type_label": _local_label(type_uri),
                }
            )
        return {
            "saved_id": saved_id,
            "total": total,
            "offset": offset,
            "limit": limit,
            "members": members,
            "has_more": offset + len(members) < total,
        }

    def _members_scan(
        self, saved_id: str, pattern_uri: str, offset: int, limit: int
    ) -> Dict[str, Any]:
        scan = getattr(self._store, "query_triples", None)
        rows = scan(self._graph_name) if callable(scan) else []
        members_uris = sorted(
            {
                str(t.get("subject") or "")
                for t in rows or []
                if t.get("predicate") == self._in_pattern
                and t.get("object") == pattern_uri
                and t.get("subject")
            }
        )
        labels = {
            t["subject"]: t["object"]
            for t in rows or []
            if t.get("predicate") == RDFS_LABEL
        }
        types = {
            t["subject"]: t["object"]
            for t in rows or []
            if t.get("predicate") == RDF_TYPE
        }
        page = members_uris[offset : offset + limit]
        members = []
        for u in page:
            type_uri = str(types.get(u) or "")
            members.append(
                {
                    "uri": u,
                    "label": str(labels.get(u) or "") or _local_label(u),
                    "type": type_uri,
                    "type_label": _local_label(type_uri),
                }
            )
        total = len(members_uris)
        return {
            "saved_id": saved_id,
            "total": total,
            "offset": offset,
            "limit": limit,
            "members": members,
            "has_more": offset + len(members) < total,
        }

    def _rebuild_adjacency(self, *, deleted: bool, inserted: int) -> None:
        if not deleted and inserted <= 0:
            return
        if getattr(self._store, "supports_adjacency", False) is not True:
            return
        from back.core.graphdb.search_cache import rebuild_graph_cache_if_enabled

        rebuild_graph_cache_if_enabled(self._store, self._graph_name, None)
