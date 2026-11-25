"""Default SQL implementations of the graph-reasoning operations.

Stateless mixin folded into :class:`GraphDBBackend` so every SQL-backed store
(Delta, Lakebase Postgres, the read-only view store) inherits transitive /
symmetric / inverse expansion and shortest-path.  Non-SQL engines (Neo4j)
override these with native queries.

No ``__init__`` and no state of its own — it relies on the host backend for
``execute_query`` (abstract on ``GraphDBBackend``), ``_sql_escape`` and
``_sql_relation``.  Kept in its own file so the SQL reasoning ops have a home
well under the class-size budget, independent of the large ``GraphDBBackend``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.core.logging import get_logger

logger = get_logger(__name__)


class SQLGraphReasoningOps:
    """SQL recursive-CTE / anti-join reasoning ops shared by SQL backends."""

    def transitive_closure(
        self,
        table_name: str,
        predicate_uri: str,
        start_uri: Optional[str] = None,
        max_depth: int = 20,
    ) -> List[Dict[str, Any]]:
        """Compute transitive closure along *predicate_uri*.

        Returns triples ``(subject, predicate, object)`` reachable through
        transitive chains not already present as direct assertions.
        Default uses a recursive CTE (Databricks SQL / Spark SQL).
        """
        esc_pred = self._sql_escape(predicate_uri)
        start_filter = ""
        if start_uri:
            esc_start = self._sql_escape(start_uri)
            start_filter = f" AND subject = '{esc_start}'"
        sql = (
            f"WITH RECURSIVE tc AS (\n"
            f"  SELECT subject, object, 1 AS depth\n"
            f"  FROM {self._sql_relation(table_name)}\n"
            f"  WHERE predicate = '{esc_pred}'{start_filter}\n"
            f"  UNION ALL\n"
            f"  SELECT tc.subject, t.object, tc.depth + 1\n"
            f"  FROM tc\n"
            f"  JOIN {self._sql_relation(table_name)} t\n"
            f"    ON tc.object = t.subject AND t.predicate = '{esc_pred}'\n"
            f"  WHERE tc.depth < {int(max_depth)}\n"
            f")\n"
            f"SELECT DISTINCT tc.subject, '{esc_pred}' AS predicate, tc.object\n"
            f"FROM tc\n"
            f"WHERE NOT EXISTS (\n"
            f"  SELECT 1 FROM {self._sql_relation(table_name)} ex\n"
            f"  WHERE ex.subject = tc.subject\n"
            f"    AND ex.predicate = '{esc_pred}'\n"
            f"    AND ex.object = tc.object\n"
            f")"
        )
        try:
            return self.execute_query(sql) or []
        except Exception as e:
            logger.warning(
                "transitive_closure SQL failed on %s, returning empty result: %s",
                table_name,
                e,
                exc_info=True,
            )
            return []

    def symmetric_expand(
        self,
        table_name: str,
        predicate_uri: str,
    ) -> List[Dict[str, Any]]:
        """Find missing symmetric counterparts for *predicate_uri*.

        For every ``(a, P, b)`` where ``(b, P, a)`` does not exist,
        returns the missing ``(b, P, a)`` triple.
        """
        esc_pred = self._sql_escape(predicate_uri)
        sql = (
            f"SELECT t.object AS subject, '{esc_pred}' AS predicate, t.subject AS object\n"
            f"FROM {self._sql_relation(table_name)} t\n"
            f"WHERE t.predicate = '{esc_pred}'\n"
            f"  AND NOT EXISTS (\n"
            f"    SELECT 1 FROM {self._sql_relation(table_name)} inv\n"
            f"    WHERE inv.subject = t.object\n"
            f"      AND inv.predicate = '{esc_pred}'\n"
            f"      AND inv.object = t.subject\n"
            f"  )"
        )
        try:
            return self.execute_query(sql) or []
        except Exception as e:
            logger.warning(
                "symmetric_expand SQL failed on %s, returning empty result: %s",
                table_name,
                e,
                exc_info=True,
            )
            return []

    def shortest_path(
        self,
        table_name: str,
        source_uri: str,
        target_uri: str,
        max_depth: int = 10,
    ) -> List[Dict[str, Any]]:
        """Find shortest path between two entities.

        Default SQL implementation returns an empty list — shortest-path
        is expensive in SQL.  Graph backends override with native
        algorithms.
        """
        return []
