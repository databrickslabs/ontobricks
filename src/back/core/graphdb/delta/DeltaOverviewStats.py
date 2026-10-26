"""Knowledge Graph Overview stats from managed Lakehouse companions.

Pass-through ``…_graph`` / ``…_data`` VIEWs re-run R2RML SQL on every
``COUNT(*)``. Explorer already reads ``…_entity_search`` / ``…_adj_*`` /
``…_props`` Delta tables; Overview and the status badge must do the same.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.core.graphdb.GraphDBBackend import GraphDBBackend
from back.core.graphdb.entity_search import is_missing_relation_error


class DeltaOverviewStats:
    """Read Overview aggregates from graph-index companions when they exist."""

    def __init__(self, store: Any) -> None:
        self._store = store

    def aggregate(self, table_name: str) -> Dict[str, int]:
        if not self._store.entity_search_ready(table_name):
            return GraphDBBackend.get_aggregate_stats(self._store, table_name)
        search = self._store.entity_search_table_id(table_name)
        props = self._store.props_table_id(table_name)
        adj_out, _ = self._store.adjacency_table_ids(table_name)
        subjects, labels = self._subject_label_counts(search)
        total = self._count_star(props)
        if total is None:
            total = subjects
        predicates = self._distinct_predicates(table_name, adj_out)
        return {
            "total": int(total),
            "distinct_subjects": int(subjects),
            "distinct_predicates": int(predicates),
            "type_assertion_count": int(subjects),
            "label_count": int(labels),
        }

    def triple_count(self, table_name: str) -> Optional[int]:
        """Row count of ``…_props`` (Delta answers ``COUNT(*)`` from file stats)."""
        return self._count_star(self._store.props_table_id(table_name))

    def type_distribution(self, table_name: str) -> List[Dict[str, Any]]:
        if not self._store.entity_search_ready(table_name):
            return GraphDBBackend.get_type_distribution(self._store, table_name)
        search = self._store._sql_relation(self._store.entity_search_table_id(table_name))
        sql = (
            f"SELECT type_uri AS type_uri, COUNT(*) AS cnt FROM {search} "
            f"GROUP BY type_uri ORDER BY cnt DESC"
        )
        return self._store.execute_query(sql) or []

    def predicate_distribution(self, table_name: str) -> List[Dict[str, Any]]:
        if not self._store.adjacency_ready(table_name):
            return GraphDBBackend.get_predicate_distribution(self._store, table_name)
        adj_out, _ = self._store.adjacency_table_ids(table_name)
        relation = self._store._sql_relation(adj_out)
        sql = (
            f"SELECT predicate AS predicate, COUNT(*) AS cnt FROM {relation} "
            f"GROUP BY predicate ORDER BY cnt DESC"
        )
        return self._store.execute_query(sql) or []

    def _subject_label_counts(self, search_fqn: str) -> tuple[int, int]:
        relation = self._store._sql_relation(search_fqn)
        sql = (
            f"SELECT COUNT(*) AS subjects, "
            f"SUM(CASE WHEN label != '' THEN 1 ELSE 0 END) AS labels "
            f"FROM {relation}"
        )
        rows = self._store.execute_query(sql) or []
        if not rows:
            return 0, 0
        row = rows[0]
        return int(row.get("subjects") or 0), int(row.get("labels") or 0)

    def _count_star(self, table_fqn: str) -> Optional[int]:
        if not table_fqn:
            return None
        relation = self._store._sql_relation(table_fqn)
        try:
            rows = self._store.execute_query(
                f"SELECT COUNT(*) AS cnt FROM {relation}"
            ) or []
        except Exception as exc:  # noqa: BLE001
            if is_missing_relation_error(exc):
                return None
            raise
        if not rows:
            return None
        return int(rows[0].get("cnt") or 0)

    def _distinct_predicates(self, table_name: str, adj_out: str) -> int:
        if not adj_out or not self._store.adjacency_ready(table_name):
            return 0
        relation = self._store._sql_relation(adj_out)
        sql = f"SELECT COUNT(DISTINCT predicate) AS cnt FROM {relation}"
        rows = self._store.execute_query(sql) or []
        if not rows:
            return 0
        return int(rows[0].get("cnt") or 0)
