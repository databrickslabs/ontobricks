"""Default SQL implementations: BFS traversal and reasoning reads.

Implementation detail of :class:`~back.core.graphdb.interfaces.GraphBackend`;
mixed into it, not a public interface. SQL-based backends inherit these
recursive-CTE defaults; graph-native engines override with native traversal.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from back.core.logging import get_logger
from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL
from back.core.graphdb.adjacency import expand_entity_neighbors_sql, seeded_bfs_sql
from back.core.graphdb.entity_search import (
    entity_search_seed_sql,
    is_asserted_only_relation,
    is_missing_relation_error,
)

logger = get_logger(__name__)


class TraversalMixin:
    """Neighbour expansion, BFS find, and the SQL reasoning primitives."""

    def bfs_traversal(
        self,
        table_name: str,
        seed_where: str,
        depth: int,
        search: str = "",
        entity_type: str = "",
    ) -> List[Dict[str, Any]]:
        """BFS traversal from seed entities.

        *seed_where* is a SQL WHERE clause (including the ``WHERE`` keyword)
        applied to the seed subquery.  Used by SQL-based backends.

        *search* and *entity_type* are structured parameters for future
        non-SQL backends (Cypher, Gremlin) that cannot use raw SQL fragments.

        When both the adjacency and entity-search companions are ready,
        seeds from the entity-search index (structured *search*/*entity_type*
        rather than *seed_where*) and walks ``_adj_out``/``_adj_in`` instead
        of the raw SPO relation — same typed-endpoint restriction Explorer's
        expansion already has. Falls back to the SPO recursive CTE below
        when either companion is missing, or on a missing-table error
        mid-query.

        Returns rows with ``entity`` and ``min_lvl`` columns.
        """
        if self.adjacency_ready(table_name) and self.entity_search_ready(table_name):
            if is_asserted_only_relation(table_name):
                search_table = self.entity_search_asserted_table_id(table_name)
            else:
                search_table = self.entity_search_table_id(table_name)
            seed_sql = entity_search_seed_sql(
                search_table=self._sql_relation(search_table),
                entity_type=entity_type,
                search=search,
                escape=self._sql_escape,
            )
            adj_out, adj_in = self.adjacency_table_ids(table_name)
            sql = seeded_bfs_sql(
                flavor=self.sql_flavor(),
                adj_out=self._sql_relation(adj_out),
                adj_in=self._sql_relation(adj_in),
                seed_sql=seed_sql,
                depth=depth,
            )
            try:
                return self.execute_query(sql) or []
            except Exception as exc:  # noqa: BLE001
                if not is_missing_relation_error(exc):
                    raise
                logger.info(
                    "Adjacency/entity-search companions unavailable for BFS "
                    "find; using SPO fallback: %s",
                    exc,
                )

        edge_filters = (
            f"t.predicate != '{RDF_TYPE}' "
            f"AND t.predicate NOT LIKE '%#label' "
            f"AND t.predicate NOT LIKE '%/label' "
            f"AND t.predicate != '{RDFS_LABEL}' "
            f"AND (t.object LIKE 'http://%' OR t.object LIKE 'https://%')"
        )
        sql = (
            f"WITH RECURSIVE seeds AS (\n"
            f"  SELECT DISTINCT subject AS entity FROM {self._sql_relation(table_name)}{seed_where}\n"
            f"), bfs(entity, lvl) AS (\n"
            f"  SELECT entity, 0 FROM seeds\n"
            f"  UNION ALL\n"
            f"  SELECT\n"
            f"    CASE WHEN t.subject = b.entity THEN t.object ELSE t.subject END,\n"
            f"    b.lvl + 1\n"
            f"  FROM bfs b\n"
            f"  JOIN {self._sql_relation(table_name)} t ON (t.subject = b.entity OR t.object = b.entity)\n"
            f"  WHERE b.lvl < {depth} AND {edge_filters}\n"
            f")\n"
            f"SELECT entity, MIN(lvl) AS min_lvl FROM bfs GROUP BY entity"
        )
        return self.execute_query(sql) or []

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

    def delete_cohort_triples(
        self,
        table_name: str,
        cohort_uri_prefix: str,
        in_cohort_predicate: str,
    ) -> int:
        """Remove all triples produced by a cohort rule (idempotent).

        A cohort rule materialises two kinds of triples:

        * Cohort-entity triples whose **subject** starts with
          *cohort_uri_prefix* (``rdf:type``, ``rdfs:label``, ``fromRule``,
          ``cohortSize``).
        * Membership triples whose **predicate** is *in_cohort_predicate*
          and whose **object** starts with *cohort_uri_prefix*.

        Default implementation issues a single SQL ``DELETE`` covering
        both cases.  Future Cypher backends would override with a
        ``MATCH ... DELETE`` pair.  Returns the number of rows deleted
        (best-effort).
        """
        if not cohort_uri_prefix:
            return 0
        prefix_esc = self._sql_escape(cohort_uri_prefix)
        in_cohort_esc = self._sql_escape(in_cohort_predicate)
        sql = (
            f"DELETE FROM {self._sql_relation(table_name)} "
            f"WHERE subject LIKE '{prefix_esc}%' "
            f"OR (predicate = '{in_cohort_esc}' "
            f"    AND object LIKE '{prefix_esc}%')"
        )
        try:
            self.execute_query(sql)
            return -1
        except Exception as exc:
            logger.warning(
                "delete_cohort_triples failed on %s (%s): %s",
                table_name,
                cohort_uri_prefix,
                exc,
            )
            return 0

    def delete_community_triples(
        self,
        table_name: str,
        cluster_uri_prefix: str,
        in_cluster_predicate: str,
    ) -> int:
        """Remove the active community partition (idempotent).

        Same subject-prefix + membership-predicate shape as cohorts.
        """
        return self.delete_cohort_triples(
            table_name, cluster_uri_prefix, in_cluster_predicate
        )

    def expand_entity_neighbors(
        self, table_name: str, entity_uris: Set[str]
    ) -> Set[str]:
        """Expand one BFS level: find typed neighbors of *entity_uris*.

        Only returns URIs that have an ``rdf:type`` assertion (real entity
        instances, not class or property URIs).

        When :meth:`adjacency_ready` is true, reads clustered adjacency
        tables; otherwise falls back to the legacy SPO scan.
        """
        if not entity_uris:
            return set()
        if self.adjacency_ready(table_name):
            adj_out, adj_in = self.adjacency_table_ids(table_name)
            sql = expand_entity_neighbors_sql(
                self._sql_relation(adj_out),
                self._sql_relation(adj_in),
                list(entity_uris),
                self._sql_escape,
            )
        else:
            in_clause = ", ".join(f"'{self._sql_escape(e)}'" for e in entity_uris)
            rel = self._sql_relation(table_name)
            sql = (
                f"SELECT DISTINCT e.entity FROM ("
                f"  SELECT object AS entity FROM {rel} "
                f"  WHERE subject IN ({in_clause}) "
                f"  AND object LIKE 'http%' "
                f"  AND predicate != '{RDF_TYPE}' "
                f"  AND predicate != '{RDFS_LABEL}' "
                f"  UNION "
                f"  SELECT subject AS entity FROM {rel} "
                f"  WHERE object IN ({in_clause}) "
                f"  AND predicate != '{RDF_TYPE}' "
                f"  AND predicate != '{RDFS_LABEL}'"
                f") e "
                f"INNER JOIN {rel} t "
                f"ON t.subject = e.entity AND t.predicate = '{RDF_TYPE}'"
            )
        rows = self.execute_query(sql) or []
        return {r["entity"] for r in rows}
