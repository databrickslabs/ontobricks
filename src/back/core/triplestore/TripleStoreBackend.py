"""Abstract base class for triple store backends."""
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Set

from back.core.logging import get_logger
from back.core.helpers import sql_escape as _shared_sql_escape
from back.core.triplestore.constants import RDF_TYPE, RDFS_LABEL

logger = get_logger(__name__)


class TripleStoreBackend(ABC):
    """Abstract base class for triple store backends (e.g. Delta)."""

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
    # SQL-based backends (Delta) inherit these defaults.
    # Non-SQL backends (LadybugDB) override with native queries.
    # ------------------------------------------------------------------

    @staticmethod
    def _sql_escape(value: str) -> str:
        """Escape single quotes for SQL string literals."""
        return _shared_sql_escape(value)

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
            f"FROM {table_name}"
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
            f"SELECT object AS type_uri, COUNT(*) AS cnt FROM {table_name} "
            f"WHERE predicate = '{RDF_TYPE}' GROUP BY object ORDER BY cnt DESC"
        )
        return self.execute_query(sql) or []

    def get_predicate_distribution(self, table_name: str) -> List[Dict[str, Any]]:
        """Return count per predicate URI, ordered descending."""
        sql = (
            f"SELECT predicate, COUNT(*) AS cnt FROM {table_name} "
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
                f"SELECT DISTINCT subject FROM {table_name} "
                f"WHERE predicate != '{RDF_TYPE}' "
                f"AND LOWER(object) LIKE '%{esc}%')"
            )
        sql = (
            f"SELECT DISTINCT subject FROM {table_name} "
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
            f"SELECT DISTINCT subject FROM {table_name} "
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
            f"SELECT subject, object FROM {table_name} "
            f"WHERE predicate = '{RDF_TYPE}' AND subject IN ({in_clause})"
        )
        label_sql = (
            f"SELECT subject, object FROM {table_name} "
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
            f"SELECT subject, predicate, object FROM {table_name} "
            f"WHERE subject IN ({in_clause})"
        )
        return self.execute_query(sql)

    def get_predicates_for_type(
        self, table_name: str, type_uri: str
    ) -> List[str]:
        """Return distinct predicates used by instances of *type_uri*."""
        esc_type = self._sql_escape(type_uri)
        sql = (
            f"SELECT DISTINCT predicate FROM {table_name} "
            f"WHERE subject IN ("
            f"  SELECT subject FROM {table_name} "
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
            f"FROM {table_name}{where} LIMIT {limit} OFFSET {offset}"
        )
        return self.execute_query(sql)

    def paginated_count(
        self, table_name: str, conditions: List[str]
    ) -> int:
        """Return count of triples matching *conditions*."""
        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = f"SELECT COUNT(*) AS cnt FROM {table_name}{where}"
        rows = self.execute_query(sql)
        return int(rows[0]["cnt"]) if rows else 0

    def bfs_traversal(
        self, table_name: str, seed_where: str, depth: int,
        search: str = "", entity_type: str = "",
    ) -> List[Dict[str, Any]]:
        """BFS traversal from seed entities.

        *seed_where* is a SQL WHERE clause (including the ``WHERE`` keyword)
        applied to the seed subquery.  Used by SQL-based backends.

        *search* and *entity_type* are structured parameters for backends
        (e.g. LadybugDB/Cypher) that cannot use raw SQL fragments.

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
            f"  SELECT DISTINCT subject AS entity FROM {table_name}{seed_where}\n"
            f"), bfs(entity, lvl) AS (\n"
            f"  SELECT entity, 0 FROM seeds\n"
            f"  UNION ALL\n"
            f"  SELECT\n"
            f"    CASE WHEN t.subject = b.entity THEN t.object ELSE t.subject END,\n"
            f"    b.lvl + 1\n"
            f"  FROM bfs b\n"
            f"  JOIN {table_name} t ON (t.subject = b.entity OR t.object = b.entity)\n"
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
    ) -> Set[str]:
        """Return distinct subjects matching type and/or value criteria.

        *field* is ``"label"`` (match on ``rdfs:label``) or ``"id"`` (match on
        the subject URI itself).  *match_type* is ``"contains"``, ``"exact"``,
        ``"starts"``, or ``"ends"``.
        """
        esc_type = self._sql_escape(entity_type) if entity_type else ""
        safe_val = self._sql_escape(value.lower()) if value else ""

        def _like(column: str) -> str:
            if match_type == "exact":
                return f"{column} = '{safe_val}'"
            if match_type == "starts":
                return f"{column} LIKE '{safe_val}%'"
            if match_type == "ends":
                return f"{column} LIKE '%{safe_val}'"
            return f"{column} LIKE '%{safe_val}%'"

        if entity_type and value:
            if field == "id":
                like = _like("LOWER(subject)")
                sql = (
                    f"SELECT DISTINCT subject FROM {table_name} "
                    f"WHERE predicate = '{RDF_TYPE}' AND object = '{esc_type}' "
                    f"AND {like}"
                )
            else:
                like = _like("LOWER(object)")
                sql = (
                    f"SELECT DISTINCT subject FROM {table_name} "
                    f"WHERE predicate = '{RDF_TYPE}' AND object = '{esc_type}' "
                    f"AND subject IN ("
                    f"SELECT subject FROM {table_name} "
                    f"WHERE predicate = '{RDFS_LABEL}' AND {like})"
                )
        elif entity_type:
            sql = (
                f"SELECT DISTINCT subject FROM {table_name} "
                f"WHERE predicate = '{RDF_TYPE}' AND object = '{esc_type}'"
            )
        elif field == "id":
            like = _like("LOWER(subject)")
            sql = (
                f"SELECT DISTINCT subject FROM {table_name} "
                f"WHERE predicate = '{RDF_TYPE}' AND {like}"
            )
        else:
            like = _like("LOWER(object)")
            sql = (
                f"SELECT DISTINCT subject FROM {table_name} "
                f"WHERE predicate = '{RDFS_LABEL}' AND {like}"
            )

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
        sql = f"SELECT DISTINCT subject FROM {table_name} WHERE {like_clauses}"
        rows = self.execute_query(sql)
        return {r["subject"] for r in rows}

    # ------------------------------------------------------------------
    # Reasoning methods — default SQL implementations.
    # Non-SQL backends (LadybugDB) override with native queries.
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
            f"  FROM {table_name}\n"
            f"  WHERE predicate = '{esc_pred}'{start_filter}\n"
            f"  UNION ALL\n"
            f"  SELECT tc.subject, t.object, tc.depth + 1\n"
            f"  FROM tc\n"
            f"  JOIN {table_name} t\n"
            f"    ON tc.object = t.subject AND t.predicate = '{esc_pred}'\n"
            f"  WHERE tc.depth < {int(max_depth)}\n"
            f")\n"
            f"SELECT DISTINCT tc.subject, '{esc_pred}' AS predicate, tc.object\n"
            f"FROM tc\n"
            f"WHERE NOT EXISTS (\n"
            f"  SELECT 1 FROM {table_name} ex\n"
            f"  WHERE ex.subject = tc.subject\n"
            f"    AND ex.predicate = '{esc_pred}'\n"
            f"    AND ex.object = tc.object\n"
            f")"
        )
        try:
            return self.execute_query(sql) or []
        except Exception as e:
            logger.debug("transitive_closure SQL failed: %s", e)
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
            f"FROM {table_name} t\n"
            f"WHERE t.predicate = '{esc_pred}'\n"
            f"  AND NOT EXISTS (\n"
            f"    SELECT 1 FROM {table_name} inv\n"
            f"    WHERE inv.subject = t.object\n"
            f"      AND inv.predicate = '{esc_pred}'\n"
            f"      AND inv.object = t.subject\n"
            f"  )"
        )
        try:
            return self.execute_query(sql) or []
        except Exception as e:
            logger.debug("symmetric_expand SQL failed: %s", e)
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

    def expand_entity_neighbors(
        self, table_name: str, entity_uris: Set[str]
    ) -> Set[str]:
        """Expand one BFS level: find typed neighbors of *entity_uris*.

        Only returns URIs that have an ``rdf:type`` assertion (real entity
        instances, not class or property URIs).
        """
        if not entity_uris:
            return set()
        in_clause = ", ".join(
            f"'{self._sql_escape(e)}'" for e in entity_uris
        )
        sql = (
            f"SELECT DISTINCT e.entity FROM ("
            f"  SELECT object AS entity FROM {table_name} "
            f"  WHERE subject IN ({in_clause}) "
            f"  AND object LIKE 'http%' "
            f"  AND predicate != '{RDF_TYPE}' "
            f"  AND predicate != '{RDFS_LABEL}' "
            f"  UNION "
            f"  SELECT subject AS entity FROM {table_name} "
            f"  WHERE object IN ({in_clause}) "
            f"  AND predicate != '{RDF_TYPE}' "
            f"  AND predicate != '{RDFS_LABEL}'"
            f") e "
            f"INNER JOIN {table_name} t "
            f"ON t.subject = e.entity AND t.predicate = '{RDF_TYPE}'"
        )
        rows = self.execute_query(sql) or []
        return {r["entity"] for r in rows}
