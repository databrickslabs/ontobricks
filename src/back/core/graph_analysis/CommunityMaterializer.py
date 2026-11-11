"""Write and read a single community partition as RDF triples."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL
from back.core.graph_analysis.CommunityVocabulary import CommunityVocabulary
from back.core.logging import get_logger

logger = get_logger(__name__)


class CommunityMaterializer:
    """Replace-or-clear the active cluster partition on a graph store."""

    def __init__(self, store: Any, graph_name: str, base_uri: str) -> None:
        self._store = store
        self._graph_name = graph_name
        self._base_uri = base_uri or ""

    def materialize(
        self,
        clusters: List[Dict[str, Any]],
        stats: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Delete the previous partition, then insert *clusters*.

        Returns the number of triples inserted.
        """
        self.clear()
        triples = self._build_triples(clusters or [], stats or {})
        if not triples:
            return 0
        inserted = int(self._store.insert_triples(self._graph_name, triples) or 0)
        self._rebuild_adjacency(deleted=True, inserted=inserted)
        return inserted

    def clear(self) -> int:
        """Remove every community triple for this domain. Returns rows deleted."""
        if self._store is None:
            return 0
        prefix = CommunityVocabulary.cluster_prefix(self._base_uri)
        predicate = CommunityVocabulary.in_cluster(self._base_uri)
        delete_fn = getattr(self._store, "delete_community_triples", None)
        if not callable(delete_fn):
            delete_fn = getattr(self._store, "delete_cohort_triples", None)
        deleted = 0
        if callable(delete_fn):
            try:
                deleted = int(delete_fn(self._graph_name, prefix, predicate) or 0)
            except Exception as exc:
                logger.warning("CommunityMaterializer: delete failed: %s", exc)
        if deleted != 0:
            self._rebuild_adjacency(deleted=True, inserted=0)
        return deleted

    def load_current(self) -> Dict[str, Any]:
        """Return the materialized partition, or empty clusters/stats."""
        triples = self._load_partition_triples()
        if not triples:
            return {"clusters": [], "stats": None}

        vocab = CommunityVocabulary
        in_cluster = vocab.in_cluster(self._base_uri)
        size_pred = vocab.cluster_size(self._base_uri)
        algo_pred = vocab.from_algorithm(self._base_uri)
        res_pred = vocab.resolution(self._base_uri)
        mod_pred = vocab.modularity(self._base_uri)

        members_by_uri: Dict[str, List[str]] = {}
        meta: Dict[str, Dict[str, str]] = {}
        for t in triples:
            subj = t.get("subject") or ""
            pred = t.get("predicate") or ""
            obj = t.get("object") or ""
            if pred == in_cluster:
                members_by_uri.setdefault(obj, []).append(subj)
                continue
            cell = meta.setdefault(subj, {})
            if pred == size_pred:
                cell["size"] = obj
            elif pred == algo_pred:
                cell["algorithm"] = obj
            elif pred == res_pred:
                cell["resolution"] = obj
            elif pred == mod_pred:
                cell["modularity"] = obj
            elif pred == RDFS_LABEL:
                cell["label"] = obj

        clusters: List[Dict[str, Any]] = []
        for uri, members in members_by_uri.items():
            cid = vocab.parse_cluster_id(uri)
            info = meta.get(uri, {})
            size = int(info["size"]) if info.get("size", "").isdigit() else len(members)
            clusters.append(
                {
                    "id": cid if cid >= 0 else len(clusters),
                    "uri": uri,
                    "members": sorted(members),
                    "size": size,
                    "label": info.get("label") or f"Cluster #{cid}",
                }
            )
        clusters.sort(key=lambda c: c["id"])

        sample = next(iter(meta.values()), {}) if meta else {}
        stats = {
            "cluster_count": len(clusters),
            "algorithm": sample.get("algorithm") or "louvain",
            "resolution": _as_float(sample.get("resolution"), 1.0),
            "modularity": _as_float(sample.get("modularity"), 0.0),
        }
        return {"clusters": clusters, "stats": stats}

    def _build_triples(
        self, clusters: List[Dict[str, Any]], stats: Dict[str, Any]
    ) -> List[Dict[str, str]]:
        klass = CommunityVocabulary.cluster_class(self._base_uri)
        in_cluster = CommunityVocabulary.in_cluster(self._base_uri)
        size_pred = CommunityVocabulary.cluster_size(self._base_uri)
        algo_pred = CommunityVocabulary.from_algorithm(self._base_uri)
        res_pred = CommunityVocabulary.resolution(self._base_uri)
        mod_pred = CommunityVocabulary.modularity(self._base_uri)
        algorithm = str(stats.get("algorithm") or "louvain")
        resolution = str(stats.get("resolution", 1.0))
        modularity = str(stats.get("modularity", 0.0))

        triples: List[Dict[str, str]] = []
        for raw in clusters:
            cid = int(raw.get("id", 0) or 0)
            members = list(raw.get("members") or [])
            size = int(raw.get("size") or len(members))
            uri = CommunityVocabulary.cluster(self._base_uri, cid)
            triples.extend(
                [
                    {"subject": uri, "predicate": RDF_TYPE, "object": klass},
                    {
                        "subject": uri,
                        "predicate": RDFS_LABEL,
                        "object": f"Cluster #{cid}",
                    },
                    {"subject": uri, "predicate": algo_pred, "object": algorithm},
                    {"subject": uri, "predicate": size_pred, "object": str(size)},
                    {"subject": uri, "predicate": res_pred, "object": resolution},
                    {"subject": uri, "predicate": mod_pred, "object": modularity},
                ]
            )
            for member in members:
                if isinstance(member, dict):
                    member = member.get("uri") or ""
                if member:
                    triples.append(
                        {
                            "subject": str(member),
                            "predicate": in_cluster,
                            "object": uri,
                        }
                    )
        return triples

    def _load_partition_triples(self) -> List[Dict[str, str]]:
        if self._store is None:
            return []
        prefix = CommunityVocabulary.cluster_prefix(self._base_uri)
        predicate = CommunityVocabulary.in_cluster(self._base_uri)
        dialect = getattr(self._store, "query_dialect", "sql")
        if dialect == "sql":
            try:
                rel = self._store._sql_relation(self._graph_name)
                prefix_esc = self._store._sql_escape(prefix)
                pred_esc = self._store._sql_escape(predicate)
                sql = (
                    f"SELECT subject, predicate, object FROM {rel} "
                    f"WHERE subject LIKE '{prefix_esc}%' "
                    f"OR predicate = '{pred_esc}'"
                )
                return [_norm_triple(t) for t in (self._store.execute_query(sql) or [])]
            except Exception as exc:
                logger.warning("CommunityMaterializer: SQL load failed: %s", exc)
        triples = self._store.query_triples(self._graph_name) or []
        return [
            t
            for t in (_norm_triple(x) for x in triples)
            if str(t.get("subject", "")).startswith(prefix)
            or t.get("predicate") == predicate
        ]

    def _rebuild_adjacency(self, *, deleted: bool, inserted: int) -> None:
        if not deleted and inserted <= 0:
            return
        if getattr(self._store, "supports_adjacency", False) is not True:
            return
        from back.core.graphdb.search_cache import rebuild_graph_cache_if_enabled

        rebuild_graph_cache_if_enabled(
            self._store, self._graph_name, None
        )


def _norm_triple(row: Dict[str, Any]) -> Dict[str, str]:
    if not isinstance(row, dict):
        return {"subject": "", "predicate": "", "object": ""}
    lower = {str(k).lower(): v for k, v in row.items()}
    return {
        "subject": str(lower.get("subject") or ""),
        "predicate": str(lower.get("predicate") or ""),
        "object": str(lower.get("object") or ""),
    }


def _as_float(value: Any, default: float) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
