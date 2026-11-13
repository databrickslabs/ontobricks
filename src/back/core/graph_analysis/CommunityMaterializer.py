"""Write and read the active community partition as RDF triples.

The partition comes from the community job's output tables
(:class:`~back.core.graph_analysis.JobCommunities.JobCommunities`), never from
a payload posted by the browser: a partition of a large graph has one
membership triple per entity, far beyond any request body.

Writes are set-based. A store that can run ``INSERT … SELECT`` against the
job's Delta tables (the Lakehouse engine, ``insert_triples_from_select``) does
the whole write on the warehouse; every other store is fed keyset-paginated
batches, so app memory stays bounded whatever the partition size.

Reads are bounded too: the cluster list is top-N by size, members are paged,
and the Explorer asks for assignments of the nodes it draws only.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL
from back.core.graph_analysis.CommunityVocabulary import CommunityVocabulary
from back.core.logging import get_logger

logger = get_logger(__name__)

#: Upper bounds on what one request may read from the materialized partition.
CURRENT_TOP_MAX = 200
DEFAULT_CURRENT_TOP = 50
ASSIGNMENTS_MAX = 5_000
MEMBERS_PAGE_MAX = 500

_BATCH_SIZE = 50_000


def _local_label(uri: str) -> str:
    tail = (uri or "").rstrip("/").rsplit("/", 1)[-1]
    return tail.rsplit("#", 1)[-1] or uri


class CommunityMaterializer:
    """Replace, clear and read the active cluster partition on a graph store."""

    def __init__(self, store: Any, graph_name: str, base_uri: str) -> None:
        self._store = store
        self._graph_name = graph_name
        self._base_uri = base_uri or ""
        vocab = CommunityVocabulary
        self._prefix = vocab.cluster_prefix(self._base_uri)
        self._in_cluster = vocab.in_cluster(self._base_uri)
        self._klass = vocab.cluster_class(self._base_uri)
        self._size_pred = vocab.cluster_size(self._base_uri)
        self._algo_pred = vocab.from_algorithm(self._base_uri)
        self._mod_pred = vocab.modularity(self._base_uri)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def materialize(
        self,
        communities: Any,
        stats: Dict[str, Any],
        on_progress: Optional[Callable[[int, str], None]] = None,
    ) -> int:
        """Replace the partition with the job output read through *communities*.

        Returns the number of triples written. A failure part-way through
        clears the partition again, so no half-written partition stays visible.
        """
        self.clear()
        algorithm = str(stats.get("algorithm") or "label_propagation")
        modularity = str(stats.get("modularity", 0.0))
        try:
            if callable(getattr(self._store, "insert_triples_from_select", None)):
                inserted = self._materialize_with_sql(
                    communities.output_table, algorithm, modularity, stats
                )
            else:
                inserted = self._materialize_in_batches(
                    communities, algorithm, modularity, stats, on_progress
                )
        except Exception:
            logger.exception("CommunityMaterializer: materialize failed, clearing")
            self.clear()
            raise
        self._rebuild_adjacency(deleted=True, inserted=inserted)
        return inserted

    def _cluster_uri_sql(self) -> str:
        return f"concat('{self._esc(self._prefix)}c-', CAST(community_rank AS STRING))"

    def membership_select_sql(self, out: str) -> str:
        return (
            f"SELECT node_uri AS subject, '{self._esc(self._in_cluster)}' AS predicate, "
            f"{self._cluster_uri_sql()} AS object FROM {out}"
        )

    def metadata_select_sql(self, out: str, algorithm: str, modularity: str) -> str:
        uri = self._cluster_uri_sql()
        rows = [
            (RDF_TYPE, f"'{self._esc(self._klass)}'"),
            (RDFS_LABEL, "concat('Cluster #', CAST(community_rank AS STRING))"),
            (self._algo_pred, f"'{self._esc(algorithm)}'"),
            (self._size_pred, "CAST(size AS STRING)"),
            (self._mod_pred, f"'{self._esc(modularity)}'"),
        ]
        return "\nUNION ALL\n".join(
            f"SELECT {uri} AS subject, '{self._esc(pred)}' AS predicate, {obj} AS object "
            f"FROM {out}_sizes"
            for pred, obj in rows
        )

    def _materialize_with_sql(
        self, out: str, algorithm: str, modularity: str, stats: Dict[str, Any]
    ) -> int:
        insert = self._store.insert_triples_from_select
        insert(self._graph_name, self.membership_select_sql(out))
        insert(self._graph_name, self.metadata_select_sql(out, algorithm, modularity))
        return int(stats.get("node_count", 0) or 0) + 5 * int(
            stats.get("cluster_count", 0) or 0
        )

    def _materialize_in_batches(
        self,
        communities: Any,
        algorithm: str,
        modularity: str,
        stats: Dict[str, Any],
        on_progress: Optional[Callable[[int, str], None]],
    ) -> int:
        total = max(1, int(stats.get("node_count", 0) or 0))
        inserted = 0
        written = 0
        for batch in communities.iter_memberships(_BATCH_SIZE):
            triples = [
                {
                    "subject": uri,
                    "predicate": self._in_cluster,
                    "object": CommunityVocabulary.cluster(self._base_uri, cid),
                }
                for uri, cid in batch
                if uri
            ]
            inserted += int(self._store.insert_triples(self._graph_name, triples) or 0)
            written += len(batch)
            if on_progress:
                pct = 20 + int(70 * min(1.0, written / total))
                on_progress(pct, f"Writing memberships ({written:,} / {total:,})")
        for batch in communities.iter_sizes(_BATCH_SIZE):
            triples: List[Dict[str, str]] = []
            for cid, size in batch:
                uri = CommunityVocabulary.cluster(self._base_uri, cid)
                triples.extend(
                    [
                        {"subject": uri, "predicate": RDF_TYPE, "object": self._klass},
                        {"subject": uri, "predicate": RDFS_LABEL, "object": f"Cluster #{cid}"},
                        {"subject": uri, "predicate": self._algo_pred, "object": algorithm},
                        {"subject": uri, "predicate": self._size_pred, "object": str(size)},
                        {"subject": uri, "predicate": self._mod_pred, "object": modularity},
                    ]
                )
            inserted += int(self._store.insert_triples(self._graph_name, triples) or 0)
        return inserted

    def clear(self) -> int:
        """Remove every community triple for this domain. Returns rows deleted."""
        if self._store is None:
            return 0
        delete_fn = getattr(self._store, "delete_community_triples", None)
        if not callable(delete_fn):
            delete_fn = getattr(self._store, "delete_cohort_triples", None)
        deleted = 0
        if callable(delete_fn):
            try:
                deleted = int(delete_fn(self._graph_name, self._prefix, self._in_cluster) or 0)
            except Exception as exc:
                logger.warning("CommunityMaterializer: delete failed: %s", exc)
        if deleted != 0:
            self._rebuild_adjacency(deleted=True, inserted=0)
        return deleted

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def _is_sql(self) -> bool:
        return getattr(self._store, "query_dialect", "sql") == "sql"

    def _esc(self, value: str) -> str:
        return (value or "").replace("'", "''")

    def _rel(self) -> str:
        return self._store._sql_relation(self._graph_name)

    def _rows(self, sql: str) -> List[Dict[str, Any]]:
        return [
            {str(k).lower(): v for k, v in row.items()}
            for row in (self._store.execute_query(sql) or [])
            if isinstance(row, dict)
        ]

    def _partition_triples(self) -> List[Dict[str, str]]:
        """Full scan fallback for stores without SQL (Neo4j)."""
        out = []
        for t in self._store.query_triples(self._graph_name) or []:
            subj = str(t.get("subject") or "")
            pred = str(t.get("predicate") or "")
            if subj.startswith(self._prefix) or pred == self._in_cluster:
                out.append({"subject": subj, "predicate": pred, "object": str(t.get("object") or "")})
        return out

    def load_current(self, limit: int = DEFAULT_CURRENT_TOP) -> Dict[str, Any]:
        """Top-N materialized clusters by size plus totals, or empty.

        Never returns member lists: use :meth:`members` and :meth:`assignments`.
        """
        if self._store is None:
            return {"clusters": [], "stats": None}
        top = max(1, min(int(limit or DEFAULT_CURRENT_TOP), CURRENT_TOP_MAX))
        if self._is_sql():
            sizes, totals, meta = self._sql_current(top)
        else:
            sizes, totals, meta = self._scan_current(top)
        if not sizes:
            return {"clusters": [], "stats": None}
        clusters = [
            {
                "id": CommunityVocabulary.parse_cluster_id(uri),
                "uri": uri,
                "size": size,
                "label": f"Cluster #{CommunityVocabulary.parse_cluster_id(uri)}",
            }
            for uri, size in sizes
        ]
        stats = {
            "cluster_count": totals[0],
            "node_count": totals[1],
            "algorithm": meta.get(self._algo_pred) or "label_propagation",
            "modularity": _as_float(meta.get(self._mod_pred), 0.0),
        }
        return {"clusters": clusters, "stats": stats}

    def _sql_current(self, top: int):
        rel, size_pred, prefix = self._rel(), self._esc(self._size_pred), self._esc(self._prefix)
        where = f"predicate = '{size_pred}' AND subject LIKE '{prefix}%'"
        try:
            totals_rows = self._rows(
                f"SELECT COUNT(*) AS n, SUM(CAST(object AS BIGINT)) AS members "
                f"FROM {rel} WHERE {where}"
            )
            top_rows = self._rows(
                f"SELECT subject, object FROM {rel} WHERE {where} "
                f"ORDER BY CAST(object AS BIGINT) DESC, subject LIMIT {top}"
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("CommunityMaterializer: SQL load failed: %s", exc)
            return [], (0, 0), {}
        sizes = [(str(r.get("subject") or ""), int(r.get("object") or 0)) for r in top_rows]
        if not sizes:
            return [], (0, 0), {}
        t = totals_rows[0] if totals_rows else {}
        meta_rows = self._rows(
            f"SELECT predicate, object FROM {rel} WHERE subject = '{self._esc(sizes[0][0])}'"
        )
        meta = {str(r.get("predicate") or ""): str(r.get("object") or "") for r in meta_rows}
        return sizes, (int(t.get("n") or 0), int(t.get("members") or 0)), meta

    def _scan_current(self, top: int):
        triples = self._partition_triples()
        sizes: Dict[str, int] = {}
        meta: Dict[str, Dict[str, str]] = {}
        for t in triples:
            if t["predicate"] == self._size_pred and t["subject"].startswith(self._prefix):
                sizes[t["subject"]] = int(t["object"]) if t["object"].isdigit() else 0
            elif t["subject"].startswith(self._prefix):
                meta.setdefault(t["subject"], {})[t["predicate"]] = t["object"]
        ranked = sorted(sizes.items(), key=lambda kv: (-kv[1], kv[0]))
        first = meta.get(ranked[0][0], {}) if ranked else {}
        return ranked[:top], (len(sizes), sum(sizes.values())), first

    def assignments(self, uris: List[str]) -> Dict[str, int]:
        """``{entity_uri: cluster_id}`` for at most :data:`ASSIGNMENTS_MAX` URIs."""
        wanted = [u for u in dict.fromkeys(uris or []) if u][:ASSIGNMENTS_MAX]
        if not wanted or self._store is None:
            return {}
        if self._is_sql():
            in_list = ", ".join(f"'{self._esc(u)}'" for u in wanted)
            rows = self._rows(
                f"SELECT subject, object FROM {self._rel()} "
                f"WHERE predicate = '{self._esc(self._in_cluster)}' AND subject IN ({in_list})"
            )
            pairs = [(str(r.get("subject") or ""), str(r.get("object") or "")) for r in rows]
        else:
            keep = set(wanted)
            pairs = [
                (t["subject"], t["object"])
                for t in self._partition_triples()
                if t["predicate"] == self._in_cluster and t["subject"] in keep
            ]
        out: Dict[str, int] = {}
        for subj, obj in pairs:
            cid = CommunityVocabulary.parse_cluster_id(obj)
            if subj and cid >= 0:
                out[subj] = cid
        return out

    def members(self, cluster_id: int, offset: int = 0, limit: int = 100) -> Dict[str, Any]:
        """One page of a materialized cluster's members, with label and type."""
        page = max(1, min(int(limit or 100), MEMBERS_PAGE_MAX))
        start = max(0, int(offset or 0))
        uri = CommunityVocabulary.cluster(self._base_uri, int(cluster_id))
        if self._store is None:
            return {"cluster_id": int(cluster_id), "offset": start, "members": [], "has_more": False}
        if self._is_sql():
            rel = self._rel()
            rows = self._rows(
                f"SELECT subject FROM {rel} WHERE predicate = '{self._esc(self._in_cluster)}' "
                f"AND object = '{self._esc(uri)}' ORDER BY subject "
                f"LIMIT {page + 1} OFFSET {start}"
            )
            subjects = [str(r.get("subject") or "") for r in rows]
        else:
            subjects = sorted(
                t["subject"]
                for t in self._partition_triples()
                if t["predicate"] == self._in_cluster and t["object"] == uri
            )[start: start + page + 1]
        has_more = len(subjects) > page
        subjects = subjects[:page]
        meta = self._entity_meta(subjects)
        members = [
            {
                "uri": s,
                "label": meta.get(s, {}).get("label") or _local_label(s),
                "type": meta.get(s, {}).get("type", ""),
                "type_label": _local_label(meta.get(s, {}).get("type", "")),
            }
            for s in subjects
        ]
        return {"cluster_id": int(cluster_id), "offset": start, "members": members, "has_more": has_more}

    def _entity_meta(self, subjects: List[str]) -> Dict[str, Dict[str, str]]:
        if not subjects or not self._is_sql():
            return {}
        in_list = ", ".join(f"'{self._esc(s)}'" for s in subjects)
        rows = self._rows(
            f"SELECT subject, predicate, object FROM {self._rel()} "
            f"WHERE subject IN ({in_list}) AND predicate IN ('{RDF_TYPE}', '{RDFS_LABEL}')"
        )
        meta: Dict[str, Dict[str, str]] = {}
        for r in rows:
            cell = meta.setdefault(str(r.get("subject") or ""), {})
            key = "type" if r.get("predicate") == RDF_TYPE else "label"
            cell.setdefault(key, str(r.get("object") or ""))
        return meta

    def _rebuild_adjacency(self, *, deleted: bool, inserted: int) -> None:
        if not deleted and inserted <= 0:
            return
        if getattr(self._store, "supports_adjacency", False) is not True:
            return
        from back.core.graphdb.search_cache import rebuild_graph_cache_if_enabled

        rebuild_graph_cache_if_enabled(self._store, self._graph_name, None)


def _as_float(value: Any, default: float) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
