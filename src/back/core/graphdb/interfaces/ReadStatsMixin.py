"""Default SQL implementations: aggregate stats, distributions, analysis scan.

Implementation detail of :class:`~back.core.graphdb.interfaces.GraphBackend`;
mixed into it, not a public interface. SQL-based backends inherit these
defaults; non-SQL engines override the ones they need.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL


class ReadStatsMixin:
    """Aggregate statistics, type/predicate distributions, and the filtered
    analysis scan — the read methods that summarise a whole triple store."""

    def get_inferred_triple_count(self, table_name: str) -> int:
        """Return the count of inferred/app-written triples for *table_name*.

        Backends that separate bulk-synced data from reasoning output
        (e.g. :class:`FlatStore`) override this to query only the
        writable companion table.  The default returns 0 (no distinction
        between synced and inferred triples in this backend).
        """
        return 0

    def _distinct_count_expr(self, column: str) -> str:
        """SQL expression for a distinct count of *column*.

        Default is the exact ``COUNT(DISTINCT ...)``. Backends with a fast
        approximate cardinality counter (e.g. Spark's ``approx_count_distinct``)
        override this so the aggregate-stats overview doesn't pay for a full
        distinct shuffle over multi-million-row triple tables.
        """
        return f"COUNT(DISTINCT {column})"

    def get_aggregate_stats(self, table_name: str) -> Dict[str, int]:
        """Return aggregate triple-store statistics in a single query.

        Keys: total, distinct_subjects, distinct_predicates,
              type_assertion_count, label_count.
        """
        sql = (
            f"SELECT "
            f"COUNT(*) AS total, "
            f"{self._distinct_count_expr('subject')} AS distinct_subjects, "
            f"{self._distinct_count_expr('predicate')} AS distinct_predicates, "
            f"SUM(CASE WHEN predicate = '{RDF_TYPE}' THEN 1 ELSE 0 END) AS type_assertion_count, "
            f"SUM(CASE WHEN predicate = '{RDFS_LABEL}' THEN 1 ELSE 0 END) AS label_count "
            f"FROM {self._sql_relation(table_name)}"
        )
        rows = self.execute_query(sql)
        row = rows[0] if rows else {}
        return {
            "total": int(row.get("total", 0)),
            "distinct_subjects": int(row.get("distinct_subjects", 0)),
            "distinct_predicates": int(row.get("distinct_predicates", 0)),
            "type_assertion_count": int(row.get("type_assertion_count", 0)),
            "label_count": int(row.get("label_count", 0)),
        }

    def get_type_distribution(self, table_name: str) -> List[Dict[str, Any]]:
        """Return count per ``rdf:type`` value, ordered descending."""
        sql = (
            f"SELECT object AS type_uri, COUNT(*) AS cnt FROM {self._sql_relation(table_name)} "
            f"WHERE predicate = '{RDF_TYPE}' GROUP BY object ORDER BY cnt DESC"
        )
        return self.execute_query(sql) or []

    def query_triples_for_analysis(
        self,
        table_name: str,
        *,
        class_filter: Optional[List[str]] = None,
        predicate_filter: Optional[List[str]] = None,
    ) -> List[Dict[str, str]]:
        """Return only the triples a filtered graph analysis actually needs.

        Without filters this is exactly :meth:`query_triples`.  With a
        *class_filter* the returned set is scoped to the instances of those
        classes **plus their direct neighbours** — every triple whose subject
        is in that scope.  That is enough to rebuild the same NetworkX graph
        the unfiltered path would build and then restrict: edges among the
        scope, and the ``rdf:type`` / ``rdfs:label`` triples of both the
        selected instances and their neighbours.

        This is what makes a filtered analysis viable on a large graph: the
        volume read scales with the selected subgraph rather than the whole
        store.

        ``rdf:type`` and ``rdfs:label`` are always retained regardless of
        *predicate_filter*, because the analysis needs them to resolve node
        types and display labels even though they never become graph edges.
        """
        if not class_filter and not predicate_filter:
            return self.query_triples(table_name)

        if self.query_dialect != "sql":
            # Non-SQL engines (Cypher, ...) have no default pushdown; fall
            # back to loading everything and filtering in Python so the
            # behaviour stays identical, just not cheaper.
            return self._filter_analysis_triples_in_python(
                self.query_triples(table_name),
                class_filter=class_filter,
                predicate_filter=predicate_filter,
            )

        rel = self._sql_relation(table_name)
        keep = ", ".join(f"'{p}'" for p in (RDF_TYPE, RDFS_LABEL))
        prefix = ""
        conditions: List[str] = []

        if class_filter:
            classes = ", ".join(f"'{self._sql_escape(c)}'" for c in class_filter)
            prefix = (
                f"WITH allowed AS (\n"
                f"  SELECT DISTINCT subject AS n FROM {rel}\n"
                f"  WHERE predicate = '{RDF_TYPE}' AND object IN ({classes})\n"
                f"), scope AS (\n"
                f"  SELECT n FROM allowed\n"
                f"  UNION\n"
                f"  SELECT e.object AS n FROM {rel} e\n"
                f"  WHERE e.subject IN (SELECT n FROM allowed)\n"
                f"    AND (e.object LIKE 'http://%' OR e.object LIKE 'https://%')\n"
                f"  UNION\n"
                f"  SELECT e.subject AS n FROM {rel} e\n"
                f"  WHERE e.object IN (SELECT n FROM allowed)\n"
                f")\n"
            )
            conditions.append("t.subject IN (SELECT n FROM scope)")

        if predicate_filter:
            excluded = ", ".join(
                f"'{self._sql_escape(p)}'" for p in predicate_filter
            )
            conditions.append(
                f"(t.predicate NOT IN ({excluded}) OR t.predicate IN ({keep}))"
            )

        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = (
            f"{prefix}"
            f"SELECT t.subject, t.predicate, t.object FROM {rel} t{where}"
        )
        return self.execute_query(sql) or []

    @staticmethod
    def _filter_analysis_triples_in_python(
        triples: List[Dict[str, str]],
        *,
        class_filter: Optional[List[str]] = None,
        predicate_filter: Optional[List[str]] = None,
    ) -> List[Dict[str, str]]:
        """In-Python equivalent of the :meth:`query_triples_for_analysis` SQL.

        Used by non-SQL backends so they return the same triple set, and by
        the tests as the oracle the generated SQL is checked against.
        """
        keep = {RDF_TYPE, RDFS_LABEL}
        out = triples

        if class_filter:
            wanted = set(class_filter)
            allowed = {
                t["subject"]
                for t in out
                if t.get("predicate") == RDF_TYPE and t.get("object") in wanted
            }
            scope = set(allowed)
            for t in out:
                subj = t.get("subject", "")
                obj = t.get("object", "")
                if subj in allowed and (
                    obj.startswith("http://") or obj.startswith("https://")
                ):
                    scope.add(obj)
                if obj in allowed:
                    scope.add(subj)
            out = [t for t in out if t.get("subject", "") in scope]

        if predicate_filter:
            excluded = set(predicate_filter)
            out = [
                t
                for t in out
                if t.get("predicate", "") not in excluded
                or t.get("predicate", "") in keep
            ]

        return out

    def get_predicate_distribution(self, table_name: str) -> List[Dict[str, Any]]:
        """Return count per predicate URI, ordered descending."""
        sql = (
            f"SELECT predicate, COUNT(*) AS cnt FROM {self._sql_relation(table_name)} "
            f"GROUP BY predicate ORDER BY cnt DESC"
        )
        return self.execute_query(sql) or []
