"""Abstract base class for graph database backends.

A graph DB backend is a graph-capable triple store (e.g. Lakebase Postgres,
Databricks Delta, or any future Cypher / Gremlin engine plugged in via
``_starter_kit/``) operating on the ``(subject, predicate, object)`` triple
model.  It is the single storage abstraction used across OntoBricks for
querying, reasoning, graph traversal, and analytics.

``GraphDBBackend`` provides:

* the core CRUD abstract methods every backend must implement;
* a large set of named query / reasoning methods with default SQL
  implementations (SQL-based backends inherit these; non-SQL engines such as
  Cypher or Gremlin override with native queries);
* graph-specific concerns — connection management, schema introspection, sync
  to/from remote storage, capability flags, and query-translator selection.
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from back.core.logging import get_logger
from back.core.helpers import sql_escape as _shared_sql_escape
from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL

logger = get_logger(__name__)


class GraphDBBackend(ABC):
    """Abstract base for graph DB engines (Lakebase Postgres, Delta, KuzuDB, ...).

    Subclasses must implement the core CRUD abstract methods **plus** the
    graph-specific abstract methods (``get_connection`` / ``close``).
    """

    # ------------------------------------------------------------------
    # Core abstract methods
    # ------------------------------------------------------------------

    @abstractmethod
    def create_table(self, table_name: str) -> None:
        """Create the (subject, predicate, object) table."""
        ...

    @abstractmethod
    def drop_table(self, table_name: str) -> None:
        """Drop if exists."""
        ...

    @abstractmethod
    def insert_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 500,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Batch insert triples, returns count inserted."""
        ...

    def delete_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 500,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Remove specific triples from the store. Returns count deleted.

        Default implementation raises NotImplementedError.
        Backends that support incremental sync should override this.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support delete_triples"
        )

    def synced_table_name(self, table_name: str) -> str:
        """Return the table name that contains only synced (non-materialized) triples.

        For backends that separate synced bulk data from app-written/inferred data
        (e.g. LakebaseFlatStore with its ``_sync`` / ``__app`` companion layout),
        this returns the synced-only side so callers can query without materialised
        triples.  The default returns *table_name* unchanged (no distinction).
        """
        return table_name

    @abstractmethod
    def query_triples(self, table_name: str) -> List[Dict[str, str]]:
        """SELECT all triples."""
        ...

    @abstractmethod
    def count_triples(self, table_name: str) -> int:
        """Count triples."""
        ...

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        ...

    @abstractmethod
    def get_status(self, table_name: str) -> Dict[str, Any]:
        """Return dict with count, last_modified, etc."""
        ...

    def optimize_table(self, table_name: str) -> None:
        """Run backend-specific optimization after bulk writes.

        Default is a no-op; backends that benefit from post-write
        optimization (e.g. Delta OPTIMIZE) should override this.
        """

    @abstractmethod
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute arbitrary SQL and return results."""
        ...

    # ------------------------------------------------------------------
    # Named query methods with default SQL implementations.
    #
    # SQL-based backends (Delta, Lakebase Postgres) inherit these defaults.
    # Future non-SQL engines (Cypher, Gremlin) override with native queries.
    # ------------------------------------------------------------------

    @staticmethod
    def _sql_escape(value: str) -> str:
        """Escape single quotes for SQL string literals."""
        return _shared_sql_escape(value)

    def _sql_relation(self, table_name: str) -> str:
        """SQL relation fragment for *table_name* in generated queries.

        Delta passes fully-qualified ``catalog.schema.table`` unchanged.
        Postgres backends resolve to a physical identifier under ``search_path``.
        """
        return table_name

    def sql_table_reference(self, graph_name: str) -> str:
        """Stable identifier for translators (SWRL, SPARQL, aggregate/DT SQL)."""
        return self._sql_relation(graph_name)

    def get_inferred_triple_count(self, table_name: str) -> int:
        """Return the count of inferred/app-written triples for *table_name*.

        Backends that separate bulk-synced data from reasoning output
        (e.g. :class:`LakebaseFlatStore`) override this to query only the
        writable companion table.  The default returns 0 (no distinction
        between synced and inferred triples in this backend).
        """
        return 0

    def get_aggregate_stats(self, table_name: str) -> Dict[str, int]:
        """Return aggregate triple-store statistics in a single query.

        Keys: total, distinct_subjects, distinct_predicates,
              type_assertion_count, label_count.
        """
        sql = (
            f"SELECT "
            f"COUNT(*) AS total, "
            f"COUNT(DISTINCT subject) AS distinct_subjects, "
            f"COUNT(DISTINCT predicate) AS distinct_predicates, "
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

    # ------------------------------------------------------------------
    # Graph-analytics aggregations.
    #
    # These compute the non-iterative half of the KG analytics page
    # (structure counts, degree, per-type profiles) inside the engine, so
    # they work on graphs far too large to load into memory.  The
    # entity-entity edge definition is supplied by the caller through
    # *excluded_predicates* — the exclusion policy belongs to the analytics
    # layer, not to storage.
    #
    # Self-loops are excluded.  NetworkX counts a self-loop as degree 2, so
    # results can differ from the in-memory path on graphs that contain
    # self-referencing triples.
    # ------------------------------------------------------------------

    def _analytics_edge_cte(
        self,
        table_name: str,
        excluded_predicates: List[str],
        class_filter: Optional[List[str]] = None,
    ) -> str:
        """Return the leading ``WITH`` clause defining the analytics edge set.

        Defines ``edges(src, dst)`` as the deduplicated, canonically ordered
        set of entity-entity edges, and — when *class_filter* is given —
        ``allowed(n)``, the instances of the selected classes.
        """
        rel = self._sql_relation(table_name)
        excluded = ", ".join(
            f"'{self._sql_escape(p)}'" for p in excluded_predicates
        ) or "''"

        parts: List[str] = []
        edge_conditions = [
            "t.subject <> ''",
            "t.object <> ''",
            "t.subject <> t.object",
            "(t.object LIKE 'http://%' OR t.object LIKE 'https://%')",
            f"t.predicate NOT IN ({excluded})",
        ]

        if class_filter:
            classes = ", ".join(
                f"'{self._sql_escape(c)}'" for c in class_filter
            )
            parts.append(
                f"allowed AS (\n"
                f"  SELECT DISTINCT subject AS n FROM {rel}\n"
                f"  WHERE predicate = '{RDF_TYPE}' AND object IN ({classes})\n"
                f")"
            )
            edge_conditions.append(
                "(t.subject IN (SELECT n FROM allowed) "
                "OR t.object IN (SELECT n FROM allowed))"
            )

        where_sql = "\n    AND ".join(edge_conditions)
        parts.append(
            f"edges AS (\n"
            f"  SELECT DISTINCT\n"
            f"    LEAST(t.subject, t.object) AS src,\n"
            f"    GREATEST(t.subject, t.object) AS dst\n"
            f"  FROM {rel} t\n"
            f"  WHERE {where_sql}\n"
            f")"
        )
        return "WITH " + ",\n".join(parts)

    @staticmethod
    def _degree_cte() -> str:
        """Return the ``deg(n, d)`` CTE body computing raw degree per node."""
        return (
            "deg AS (\n"
            "  SELECT n, COUNT(*) AS d FROM (\n"
            "    SELECT src AS n FROM edges\n"
            "    UNION ALL\n"
            "    SELECT dst AS n FROM edges\n"
            "  ) x GROUP BY n\n"
            ")"
        )

    def get_graph_structure_stats(
        self,
        table_name: str,
        *,
        excluded_predicates: List[str],
        class_filter: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        """Return ``edge_count``, ``graph_node_count`` and ``node_count``.

        ``graph_node_count`` counts nodes with at least one entity-entity
        edge.  ``node_count`` is the number of nodes the analysis reports:
        the same value unfiltered, or the full instance count of the
        selected classes (isolated instances included) when *class_filter*
        is given.
        """
        cte = self._analytics_edge_cte(
            table_name, excluded_predicates, class_filter
        )
        selects = ["e.edge_count", "n.graph_node_count"]
        froms = [
            "(SELECT COUNT(*) AS edge_count FROM edges) e",
            "(SELECT COUNT(*) AS graph_node_count FROM gnodes) n",
        ]
        if class_filter:
            selects.append("a.filtered_node_count")
            froms.append(
                "(SELECT COUNT(*) AS filtered_node_count FROM allowed) a"
            )

        sql = (
            f"{cte},\n"
            f"gnodes AS (\n"
            f"  SELECT src AS n FROM edges UNION SELECT dst AS n FROM edges\n"
            f")\n"
            f"SELECT {', '.join(selects)}\n"
            f"FROM {' CROSS JOIN '.join(froms)}"
        )
        rows = self.execute_query(sql)
        row = rows[0] if rows else {}
        graph_node_count = int(row.get("graph_node_count", 0) or 0)
        node_count = (
            int(row.get("filtered_node_count", 0) or 0)
            if class_filter
            else graph_node_count
        )
        return {
            "edge_count": int(row.get("edge_count", 0) or 0),
            "graph_node_count": graph_node_count,
            "node_count": node_count,
        }

    def get_top_nodes_by_degree(
        self,
        table_name: str,
        *,
        excluded_predicates: List[str],
        class_filter: Optional[List[str]] = None,
        top_n: int = 100,
    ) -> List[Dict[str, Any]]:
        """Return the *top_n* highest-degree nodes with label and type.

        The label/type join is applied **after** the ``LIMIT`` so it only
        touches the returned rows.
        """
        cte = self._analytics_edge_cte(
            table_name, excluded_predicates, class_filter
        )
        rel = self._sql_relation(table_name)
        scope = (
            " WHERE n IN (SELECT n FROM allowed)" if class_filter else ""
        )
        sql = (
            f"{cte},\n"
            f"{self._degree_cte()},\n"
            f"top AS (\n"
            f"  SELECT n, d FROM deg{scope} ORDER BY d DESC, n LIMIT {int(top_n)}\n"
            f")\n"
            f"SELECT top.n AS node_uri, top.d AS degree,\n"
            f"       MIN(lab.object) AS label, MIN(typ.object) AS type_uri\n"
            f"FROM top\n"
            f"LEFT JOIN {rel} lab\n"
            f"  ON lab.subject = top.n AND lab.predicate = '{RDFS_LABEL}'\n"
            f"LEFT JOIN {rel} typ\n"
            f"  ON typ.subject = top.n AND typ.predicate = '{RDF_TYPE}'\n"
            f"GROUP BY top.n, top.d\n"
            f"ORDER BY top.d DESC, top.n"
        )
        return self.execute_query(sql) or []

    def get_type_edge_stats(
        self,
        table_name: str,
        *,
        excluded_predicates: List[str],
        class_filter: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Return per-class ``connected_count`` and ``degree_sum``.

        ``connected_count`` is the number of instances of the class that
        have at least one entity-entity edge; ``degree_sum`` is the total of
        their raw degrees.  The caller turns these into a mean degree
        centrality by dividing by ``graph_node_count - 1``.
        """
        cte = self._analytics_edge_cte(
            table_name, excluded_predicates, class_filter
        )
        rel = self._sql_relation(table_name)
        sql = (
            f"{cte},\n"
            f"{self._degree_cte()}\n"
            f"SELECT ty.object AS type_uri,\n"
            f"       COUNT(*) AS connected_count,\n"
            f"       SUM(deg.d) AS degree_sum\n"
            f"FROM deg\n"
            f"JOIN {rel} ty\n"
            f"  ON ty.subject = deg.n AND ty.predicate = '{RDF_TYPE}'\n"
            f"GROUP BY ty.object"
        )
        return self.execute_query(sql) or []

    def get_type_predicate_pairs(
        self,
        table_name: str,
        *,
        excluded_predicates: List[str],
        class_filter: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Return the distinct ``(type_uri, predicate)`` pairs used by nodes.

        A predicate counts for a node whether the node is the subject or the
        (URI-valued) object, mirroring the in-memory profile builder.  Rows
        are aggregated in Python so no engine-specific array function is
        needed.
        """
        cte = self._analytics_edge_cte(
            table_name, excluded_predicates, class_filter
        )
        rel = self._sql_relation(table_name)
        excluded = ", ".join(
            f"'{self._sql_escape(p)}'" for p in excluded_predicates
        ) or "''"
        sql = (
            f"{cte},\n"
            f"gnodes AS (\n"
            f"  SELECT src AS n FROM edges UNION SELECT dst AS n FROM edges\n"
            f")\n"
            f"SELECT DISTINCT ty.object AS type_uri, t.predicate AS predicate\n"
            f"FROM {rel} t\n"
            f"JOIN gnodes g ON g.n = t.subject\n"
            f"JOIN {rel} ty\n"
            f"  ON ty.subject = t.subject AND ty.predicate = '{RDF_TYPE}'\n"
            f"WHERE t.predicate NOT IN ({excluded})\n"
            f"UNION\n"
            f"SELECT DISTINCT ty.object AS type_uri, t.predicate AS predicate\n"
            f"FROM {rel} t\n"
            f"JOIN gnodes g ON g.n = t.object\n"
            f"JOIN {rel} ty\n"
            f"  ON ty.subject = t.object AND ty.predicate = '{RDF_TYPE}'\n"
            f"WHERE t.predicate NOT IN ({excluded})\n"
            f"  AND (t.object LIKE 'http://%' OR t.object LIKE 'https://%')"
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

    def find_subjects_by_type(
        self,
        table_name: str,
        type_uri: str,
        limit: int = 50,
        offset: int = 0,
        search: Optional[str] = None,
    ) -> List[str]:
        """Return distinct subject URIs that are ``rdf:type`` *type_uri*.

        When *search* is given, matches against all literal values for the
        subject (label, data properties, etc.) — not just ``rdfs:label``.
        """
        esc_type = self._sql_escape(type_uri)
        conditions = [
            f"predicate = '{RDF_TYPE}'",
            f"object = '{esc_type}'",
        ]
        if search:
            esc = self._sql_escape(search).lower()
            conditions.append(
                f"subject IN ("
                f"SELECT DISTINCT subject FROM {self._sql_relation(table_name)} "
                f"WHERE predicate != '{RDF_TYPE}' "
                f"AND LOWER(object) LIKE '%{esc}%')"
            )
        sql = (
            f"SELECT DISTINCT subject FROM {self._sql_relation(table_name)} "
            f"WHERE {' AND '.join(conditions)} "
            f"ORDER BY subject "
            f"LIMIT {int(limit)} OFFSET {int(offset)}"
        )
        rows = self.execute_query(sql)
        return [r["subject"] for r in rows]

    def resolve_subject_by_id(
        self, table_name: str, type_uri: str, id_fragment: str
    ) -> Optional[str]:
        """Find a subject URI by type and trailing local-name fragment."""
        esc_type = self._sql_escape(type_uri)
        esc_id = self._sql_escape(id_fragment)
        sql = (
            f"SELECT DISTINCT subject FROM {self._sql_relation(table_name)} "
            f"WHERE predicate = '{RDF_TYPE}' "
            f"AND object = '{esc_type}' "
            f"AND (subject LIKE '%/{esc_id}' OR subject LIKE '%#{esc_id}')"
        )
        rows = self.execute_query(sql)
        return rows[0]["subject"] if rows else None

    def get_entity_metadata(
        self, table_name: str, subjects: List[str]
    ) -> List[Dict[str, str]]:
        """Return ``rdf:type`` and ``rdfs:label`` for each subject.

        Returns a list of dicts with keys ``uri``, ``type`` (full URI),
        and ``label`` (literal value or empty string).
        """
        if not subjects:
            return []
        in_clause = ", ".join(f"'{self._sql_escape(u)}'" for u in subjects)

        type_sql = (
            f"SELECT subject, object FROM {self._sql_relation(table_name)} "
            f"WHERE predicate = '{RDF_TYPE}' AND subject IN ({in_clause})"
        )
        label_sql = (
            f"SELECT subject, object FROM {self._sql_relation(table_name)} "
            f"WHERE predicate = '{RDFS_LABEL}' AND subject IN ({in_clause})"
        )

        type_rows = self.execute_query(type_sql) or []
        label_rows = self.execute_query(label_sql) or []

        types: Dict[str, str] = {}
        for r in type_rows:
            types.setdefault(r["subject"], r["object"])
        labels: Dict[str, str] = {}
        for r in label_rows:
            labels.setdefault(r["subject"], r["object"])

        return [
            {"uri": uri, "type": types.get(uri, ""), "label": labels.get(uri, "")}
            for uri in subjects
            if uri in types
        ]

    def get_triples_for_subjects(
        self, table_name: str, subjects: List[str]
    ) -> List[Dict[str, str]]:
        """Return all triples whose subject is in *subjects*."""
        if not subjects:
            return []
        in_clause = ", ".join(f"'{self._sql_escape(u)}'" for u in subjects)
        sql = (
            f"SELECT subject, predicate, object FROM {self._sql_relation(table_name)} "
            f"WHERE subject IN ({in_clause})"
        )
        return self.execute_query(sql)

    def get_predicates_for_type(self, table_name: str, type_uri: str) -> List[str]:
        """Return distinct predicates used by instances of *type_uri*."""
        esc_type = self._sql_escape(type_uri)
        sql = (
            f"SELECT DISTINCT predicate FROM {self._sql_relation(table_name)} "
            f"WHERE subject IN ("
            f"  SELECT subject FROM {self._sql_relation(table_name)} "
            f"  WHERE predicate = '{RDF_TYPE}' "
            f"  AND object = '{esc_type}' LIMIT 1"
            f")"
        )
        rows = self.execute_query(sql)
        return [r["predicate"] for r in rows]

    def paginated_triples(
        self,
        table_name: str,
        conditions: List[str],
        limit: int,
        offset: int,
    ) -> List[Dict[str, str]]:
        """Return triples matching *conditions* with LIMIT/OFFSET pagination."""
        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = (
            f"SELECT subject, predicate, object "
            f"FROM {self._sql_relation(table_name)}{where} LIMIT {limit} OFFSET {offset}"
        )
        return self.execute_query(sql)

    def paginated_count(self, table_name: str, conditions: List[str]) -> int:
        """Return count of triples matching *conditions*."""
        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = f"SELECT COUNT(*) AS cnt FROM {self._sql_relation(table_name)}{where}"
        rows = self.execute_query(sql)
        return int(rows[0]["cnt"]) if rows else 0

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

        Returns rows with ``entity`` and ``min_lvl`` columns.
        """
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

    def find_seed_subjects(
        self,
        table_name: str,
        entity_type: str = "",
        field: str = "any",
        match_type: str = "contains",
        value: str = "",
        limit: int = 0,
    ) -> Set[str]:
        """Return distinct subjects matching type and/or value criteria.

        *field* is ``"label"`` (match on ``rdfs:label``), ``"id"`` (match on
        the subject URI itself), or ``"any"`` (match either).
        *match_type* is ``"contains"``, ``"exact"``, ``"starts"``, or
        ``"ends"``. ``limit`` (when > 0) caps returned subjects for responsive
        preview queries.
        """
        esc_type = self._sql_escape(entity_type) if entity_type else ""
        safe_val = self._sql_escape(value.lower()) if value else ""
        rel = self._sql_relation(table_name)

        search_label = field in ("label", "any")
        search_id = field in ("id", "any")

        def _like(column: str) -> str:
            if match_type == "exact":
                return f"{column} = '{safe_val}'"
            if match_type == "starts":
                return f"{column} LIKE '{safe_val}%'"
            if match_type == "ends":
                return f"{column} LIKE '%{safe_val}'"
            return f"{column} LIKE '%{safe_val}%'"

        if entity_type and value:
            # Build a set of candidate subjects matching the text filter,
            # then intersect with the typed subjects.
            parts = []
            if search_id:
                parts.append(
                    f"SELECT DISTINCT subject FROM {rel} "
                    f"WHERE predicate = '{RDF_TYPE}' AND object = '{esc_type}' "
                    f"AND {_like('LOWER(subject)')}"
                )
            if search_label:
                parts.append(
                    f"SELECT DISTINCT subject FROM {rel} "
                    f"WHERE predicate = '{RDF_TYPE}' AND object = '{esc_type}' "
                    f"AND subject IN ("
                    f"SELECT subject FROM {rel} "
                    f"WHERE predicate = '{RDFS_LABEL}' AND {_like('LOWER(object)')})"
                )
            sql = " UNION ".join(parts)

        elif entity_type:
            sql = (
                f"SELECT DISTINCT subject FROM {rel} "
                f"WHERE predicate = '{RDF_TYPE}' AND object = '{esc_type}'"
            )

        else:
            # value only — search by label and/or URI fragment
            parts = []
            if search_label:
                parts.append(
                    f"SELECT DISTINCT subject FROM {rel} "
                    f"WHERE predicate = '{RDFS_LABEL}' AND {_like('LOWER(object)')}"
                )
            if search_id:
                parts.append(
                    f"SELECT DISTINCT subject FROM {rel} "
                    f"WHERE predicate = '{RDF_TYPE}' AND {_like('LOWER(subject)')}"
                )
            sql = " UNION ".join(parts)

        rows = self.execute_query(sql)
        return {r["subject"] for r in rows}

    def find_subjects_by_patterns(
        self, table_name: str, like_patterns: List[str]
    ) -> Set[str]:
        """Return subjects matching any of the given SQL LIKE patterns."""
        if not like_patterns:
            return set()
        like_clauses = " OR ".join(
            f"subject LIKE '{self._sql_escape(p)}'" for p in like_patterns
        )
        sql = f"SELECT DISTINCT subject FROM {self._sql_relation(table_name)} WHERE {like_clauses}"
        rows = self.execute_query(sql)
        return {r["subject"] for r in rows}

    # ------------------------------------------------------------------
    # Reasoning methods — default SQL implementations.
    # Future non-SQL engines (Cypher, Gremlin) override with native queries.
    # ------------------------------------------------------------------

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

    def expand_entity_neighbors(
        self, table_name: str, entity_uris: Set[str]
    ) -> Set[str]:
        """Expand one BFS level: find typed neighbors of *entity_uris*.

        Only returns URIs that have an ``rdf:type`` assertion (real entity
        instances, not class or property URIs).
        """
        if not entity_uris:
            return set()
        in_clause = ", ".join(f"'{self._sql_escape(e)}'" for e in entity_uris)
        sql = (
            f"SELECT DISTINCT e.entity FROM ("
            f"  SELECT object AS entity FROM {self._sql_relation(table_name)} "
            f"  WHERE subject IN ({in_clause}) "
            f"  AND object LIKE 'http%' "
            f"  AND predicate != '{RDF_TYPE}' "
            f"  AND predicate != '{RDFS_LABEL}' "
            f"  UNION "
            f"  SELECT subject AS entity FROM {self._sql_relation(table_name)} "
            f"  WHERE object IN ({in_clause}) "
            f"  AND predicate != '{RDF_TYPE}' "
            f"  AND predicate != '{RDFS_LABEL}'"
            f") e "
            f"INNER JOIN {self._sql_relation(table_name)} t "
            f"ON t.subject = e.entity AND t.predicate = '{RDF_TYPE}'"
        )
        rows = self.execute_query(sql) or []
        return {r["entity"] for r in rows}

    # ------------------------------------------------------------------
    # Capability flags — reasoning engines use these instead of isinstance
    # ------------------------------------------------------------------

    @property
    def supports_cypher(self) -> bool:
        """Whether this backend speaks Cypher (vs SQL)."""
        return False

    @property
    def supports_graph_model(self) -> bool:
        """Whether this backend uses a typed graph schema (node/rel tables)."""
        return False

    @property
    def query_dialect(self) -> str:
        """Return ``'sql'``, ``'cypher'``, or another dialect identifier."""
        return "sql"

    @staticmethod
    def is_cypher_backend(store) -> bool:
        """Check if *store* is a Cypher-capable graph DB backend."""
        return isinstance(store, GraphDBBackend) and store.supports_cypher

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    @abstractmethod
    def get_connection(self) -> Any:
        """Return (and lazily open) the native database connection."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Release the database connection and any related resources."""
        ...

    # ------------------------------------------------------------------
    # Schema helpers
    # ------------------------------------------------------------------

    def get_node_table(self, table_name: str) -> str:
        """Return the node-table identifier for *table_name*.

        Default returns *table_name* unchanged.  Backends with naming
        constraints (reserved words, character restrictions) should override.
        """
        return table_name

    def get_graph_schema(self) -> Optional[Any]:
        """Return the graph schema object, or *None* if not available."""
        return None

    # ------------------------------------------------------------------
    # Sync to/from remote storage (UC Volume)
    # ------------------------------------------------------------------

    def sync_to_remote(
        self,
        uc_path: str,
        volume_service: Any,
    ) -> Tuple[bool, str]:
        """Upload local DB to remote storage.  No-op by default."""
        return False, "Not supported by this backend"

    def sync_from_remote(
        self,
        uc_path: str,
        volume_service: Any,
    ) -> Tuple[bool, str]:
        """Download DB from remote storage.  No-op by default."""
        return False, "Not supported by this backend"

    def local_path(self) -> Optional[str]:
        """Return the local file/directory path, or *None* for remote-only."""
        return None

    def remote_archive_path(self, uc_domain_path: str) -> Optional[str]:
        """Return the remote archive path for sync, or *None*."""
        return None

    # ------------------------------------------------------------------
    # Reasoning support
    # ------------------------------------------------------------------

    def get_query_translator(self, table_name: str = "") -> Any:
        """Return the appropriate SWRL/rule query translator for this backend.

        SQL-based backends should return an ``SWRLSQLTranslator``.
        Cypher-based backends should return the matching Cypher translator.
        """
        from back.core.reasoning.SWRLSQLTranslator import SWRLSQLTranslator

        return SWRLSQLTranslator()
