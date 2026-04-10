"""LadybugDB flat-model triple store backend.

Triples are stored as rows in a single node table
``Triple(id INT64 PRIMARY KEY, subject STRING, predicate STRING,
object STRING)``.  All query methods are implemented in Cypher.
"""
from typing import Any, Callable, Dict, List, Optional, Set

from back.core.logging import get_logger
from back.core.triplestore.constants import RDF_TYPE, RDFS_LABEL
from back.core.triplestore.ladybugdb import LadybugBase
from back.core.helpers import validate_table_name

logger = get_logger(__name__)


class LadybugFlatStore(LadybugBase):
    """Flat-model LadybugDB backend.

    Every triple is a row in a single ``Triple`` node table with columns
    ``(id, subject, predicate, object)``.  Queries use Cypher MATCH on
    the flat table.
    """

    # -- Core CRUD -------------------------------------------------------

    def create_table(self, table_name: str) -> None:
        validate_table_name(table_name)
        node = self._node_table(table_name)
        conn = self._get_connection()
        conn.execute(
            f"CREATE NODE TABLE IF NOT EXISTS {node}("
            f"id INT64 PRIMARY KEY, "
            f"subject STRING, "
            f"predicate STRING, "
            f"object STRING)"
        )
        self._table_registry[node] = True
        self._next_id = 0
        logger.info("Created LadybugDB flat node table: %s", node)

    def drop_table(self, table_name: str) -> None:
        validate_table_name(table_name)
        node = self._node_table(table_name)
        conn = self._get_connection()
        try:
            conn.execute(f"DROP TABLE IF EXISTS {node}")
        except Exception as e:
            logger.debug("Drop table %s (may not exist): %s", node, e)
        self._table_registry.pop(node, None)
        self._next_id = 0
        logger.info("Dropped LadybugDB node table: %s", node)

    def insert_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 2000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        validate_table_name(table_name)
        if not triples:
            return 0
        node = self._node_table(table_name)
        conn = self._get_connection()

        try:
            result = conn.execute(f"MATCH (t:{node}) RETURN MAX(t.id) AS max_id")
            row = result.get_next() if result.has_next() else None
            if row and row[0] is not None:
                self._next_id = max(self._next_id, int(row[0]) + 1)
        except Exception:
            pass

        total = 0

        for i in range(0, len(triples), batch_size):
            batch = triples[i : i + batch_size]
            for t in batch:
                conn.execute(
                    f"CREATE (:{node} {{id: $id, "
                    f"subject: $s, predicate: $p, object: $o}})",
                    parameters={
                        "id": self._next_id,
                        "s": (t.get("subject", "") or ""),
                        "p": (t.get("predicate", "") or ""),
                        "o": (t.get("object", "") or ""),
                    },
                )
                self._next_id += 1
            total += len(batch)
            if on_progress:
                on_progress(total, len(triples))
            logger.debug(
                "Inserted batch %d-%d of %d into %s",
                i + 1, i + len(batch), len(triples), node,
            )

        logger.info("Inserted %d triples into %s", total, node)
        return total

    def delete_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 2000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        validate_table_name(table_name)
        if not triples:
            return 0
        node = self._node_table(table_name)
        conn = self._get_connection()
        deleted = 0

        for i in range(0, len(triples), batch_size):
            batch = triples[i : i + batch_size]
            for t in batch:
                s = (t.get("subject", "") or "")
                p = (t.get("predicate", "") or "")
                o = (t.get("object", "") or "")
                try:
                    conn.execute(
                        f"MATCH (t:{node}) "
                        f"WHERE t.subject = $s "
                        f"AND t.predicate = $p "
                        f"AND t.object = $o "
                        f"DELETE t",
                        parameters={"s": s, "p": p, "o": o},
                    )
                    deleted += 1
                except Exception as e:
                    logger.debug("Flat delete failed for (%s, %s, %s): %s", s[:40], p[:40], o[:40], e)
            if on_progress:
                on_progress(min(i + len(batch), len(triples)), len(triples))

        logger.info("Deleted %d triples from %s", deleted, node)
        return deleted

    def query_triples(self, table_name: str) -> List[Dict[str, str]]:
        validate_table_name(table_name)
        node = self._node_table(table_name)
        conn = self._get_connection()
        result = conn.execute(
            f"MATCH (t:{node}) RETURN t.subject AS subject, "
            f"t.predicate AS predicate, t.object AS object"
        )
        return [
            {"subject": row[0], "predicate": row[1], "object": row[2]}
            for row in result
        ]

    def count_triples(self, table_name: str) -> int:
        validate_table_name(table_name)
        node = self._node_table(table_name)
        conn = self._get_connection()
        result = conn.execute(f"MATCH (t:{node}) RETURN COUNT(t) AS cnt")
        row = result.get_next()
        return int(row[0]) if row else 0

    def table_exists(self, table_name: str) -> bool:
        if not table_name or not table_name.strip():
            return False
        if self._table_registry:
            return True
        node = self._node_table(table_name)
        conn = self._get_connection()
        try:
            conn.execute(f"MATCH (t:{node}) RETURN t LIMIT 0")
            self._table_registry[node] = True
            return True
        except Exception:
            return False

    def get_status(self, table_name: str) -> Dict[str, Any]:
        validate_table_name(table_name)
        count = self.count_triples(table_name)
        path = self._get_db_path()
        return {
            "count": count,
            "last_modified": None,
            "path": path,
            "format": "ladybug",
        }

    def optimize_table(self, table_name: str) -> None:
        pass

    # -- Named query overrides (Cypher on flat table) --------------------

    def get_aggregate_stats(self, table_name: str) -> Dict[str, int]:
        node = self._node_table(table_name)
        conn = self._get_connection()
        result = conn.execute(
            f"MATCH (t:{node}) RETURN "
            f"COUNT(t) AS total, "
            f"COUNT(DISTINCT t.subject) AS distinct_subjects, "
            f"COUNT(DISTINCT t.predicate) AS distinct_predicates"
        )
        row = result.get_next()
        total = int(row[0]) if row else 0
        subj = int(row[1]) if row else 0
        pred = int(row[2]) if row else 0

        type_result = conn.execute(
            f"MATCH (t:{node}) WHERE t.predicate = $p RETURN COUNT(t) AS cnt",
            parameters={"p": RDF_TYPE},
        )
        type_row = type_result.get_next()
        type_cnt = int(type_row[0]) if type_row else 0

        label_result = conn.execute(
            f"MATCH (t:{node}) WHERE t.predicate = $p RETURN COUNT(t) AS cnt",
            parameters={"p": RDFS_LABEL},
        )
        label_row = label_result.get_next()
        label_cnt = int(label_row[0]) if label_row else 0

        return {
            "total": total,
            "distinct_subjects": subj,
            "distinct_predicates": pred,
            "type_assertion_count": type_cnt,
            "label_count": label_cnt,
        }

    def get_type_distribution(self, table_name: str) -> List[Dict[str, Any]]:
        node = self._node_table(table_name)
        conn = self._get_connection()
        result = conn.execute(
            f"MATCH (t:{node}) WHERE t.predicate = $p "
            f"RETURN t.object AS type_uri, COUNT(t) AS cnt "
            f"ORDER BY cnt DESC",
            parameters={"p": RDF_TYPE},
        )
        return [
            {"type_uri": row[0], "cnt": int(row[1])}
            for row in result
        ]

    def get_predicate_distribution(self, table_name: str) -> List[Dict[str, Any]]:
        node = self._node_table(table_name)
        conn = self._get_connection()
        result = conn.execute(
            f"MATCH (t:{node}) "
            f"RETURN t.predicate AS predicate, COUNT(t) AS cnt "
            f"ORDER BY cnt DESC"
        )
        return [
            {"predicate": row[0], "cnt": int(row[1])}
            for row in result
        ]

    def find_seed_subjects(
        self,
        table_name: str,
        entity_type: str = "",
        field: str = "any",
        match_type: str = "contains",
        value: str = "",
    ) -> Set[str]:
        node = self._node_table(table_name)
        conn = self._get_connection()

        def _cypher_match(alias_prop: str, val: str) -> str:
            if match_type == "exact":
                return f"LOWER({alias_prop}) = $val"
            if match_type == "starts":
                return f"LOWER({alias_prop}) STARTS WITH $val"
            if match_type == "ends":
                return f"LOWER({alias_prop}) ENDS WITH $val"
            return f"LOWER({alias_prop}) CONTAINS $val"

        val_lower = value.lower() if value else ""
        search_label = field in ("label", "any")
        search_id = field in ("id", "any")

        if entity_type and value:
            type_rows = conn.execute(
                f"MATCH (t:{node}) "
                f"WHERE t.predicate = $rdf_type AND t.object = $type_uri "
                f"RETURN DISTINCT t.subject AS subject",
                parameters={"rdf_type": RDF_TYPE, "type_uri": entity_type},
            )
            typed_subjects = {row[0] for row in type_rows}
            if not typed_subjects:
                return set()

            matched: Set[str] = set()
            if search_label:
                lbl_cond = _cypher_match("lbl.object", val_lower)
                lbl_rows = conn.execute(
                    f"MATCH (lbl:{node}) "
                    f"WHERE lbl.predicate = $rdfs_label AND {lbl_cond} "
                    f"RETURN DISTINCT lbl.subject AS subject",
                    parameters={"rdfs_label": RDFS_LABEL, "val": val_lower},
                )
                matched.update(row[0] for row in lbl_rows)
            if search_id:
                id_cond = _cypher_match("t.subject", val_lower)
                id_rows = conn.execute(
                    f"MATCH (t:{node}) "
                    f"WHERE t.predicate = $rdf_type AND {id_cond} "
                    f"RETURN DISTINCT t.subject AS subject",
                    parameters={"rdf_type": RDF_TYPE, "val": val_lower},
                )
                matched.update(row[0] for row in id_rows)
            return typed_subjects & matched

        elif entity_type:
            result = conn.execute(
                f"MATCH (t:{node}) "
                f"WHERE t.predicate = $rdf_type AND t.object = $type_uri "
                f"RETURN DISTINCT t.subject AS subject",
                parameters={"rdf_type": RDF_TYPE, "type_uri": entity_type},
            )
            return {row[0] for row in result}

        else:
            matched: Set[str] = set()
            if search_label:
                lbl_cond = _cypher_match("t.object", val_lower)
                lbl_rows = conn.execute(
                    f"MATCH (t:{node}) "
                    f"WHERE t.predicate = $rdfs_label AND {lbl_cond} "
                    f"RETURN DISTINCT t.subject AS subject",
                    parameters={"rdfs_label": RDFS_LABEL, "val": val_lower},
                )
                matched.update(row[0] for row in lbl_rows)
            if search_id:
                id_cond = _cypher_match("t.subject", val_lower)
                id_rows = conn.execute(
                    f"MATCH (t:{node}) "
                    f"WHERE t.predicate = $rdf_type AND {id_cond} "
                    f"RETURN DISTINCT t.subject AS subject",
                    parameters={"rdf_type": RDF_TYPE, "val": val_lower},
                )
                matched.update(row[0] for row in id_rows)
            return matched

    def find_subjects_by_type(
        self,
        table_name: str,
        type_uri: str,
        limit: int = 50,
        offset: int = 0,
        search: Optional[str] = None,
    ) -> List[str]:
        node = self._node_table(table_name)
        conn = self._get_connection()

        if search:
            search_lower = search.lower()
            match_result = conn.execute(
                f"MATCH (m:{node}) "
                f"WHERE m.predicate <> $rdf_type "
                f"AND LOWER(m.object) CONTAINS $search "
                f"RETURN DISTINCT m.subject AS subject",
                parameters={"rdf_type": RDF_TYPE, "search": search_lower},
            )
            match_subjects = {row[0] for row in match_result}
            if not match_subjects:
                return []

            type_result = conn.execute(
                f"MATCH (t:{node}) "
                f"WHERE t.predicate = $rdf_type AND t.object = $type_uri "
                f"AND t.subject IN $subjects "
                f"RETURN DISTINCT t.subject AS subject "
                f"ORDER BY subject SKIP {int(offset)} LIMIT {int(limit)}",
                parameters={
                    "rdf_type": RDF_TYPE,
                    "type_uri": type_uri,
                    "subjects": list(match_subjects),
                },
            )
            return [row[0] for row in type_result]

        result = conn.execute(
            f"MATCH (t:{node}) "
            f"WHERE t.predicate = $rdf_type AND t.object = $type_uri "
            f"RETURN DISTINCT t.subject AS subject "
            f"ORDER BY subject SKIP {int(offset)} LIMIT {int(limit)}",
            parameters={"rdf_type": RDF_TYPE, "type_uri": type_uri},
        )
        return [row[0] for row in result]

    def resolve_subject_by_id(
        self, table_name: str, type_uri: str, id_fragment: str
    ) -> Optional[str]:
        node = self._node_table(table_name)
        conn = self._get_connection()
        result = conn.execute(
            f"MATCH (t:{node}) "
            f"WHERE t.predicate = $rdf_type AND t.object = $type_uri "
            f"AND (t.subject ENDS WITH $slash_id OR t.subject ENDS WITH $hash_id) "
            f"RETURN DISTINCT t.subject LIMIT 1",
            parameters={
                "rdf_type": RDF_TYPE,
                "type_uri": type_uri,
                "slash_id": f"/{id_fragment}",
                "hash_id": f"#{id_fragment}",
            },
        )
        row = result.get_next() if result.has_next() else None
        return row[0] if row else None

    def get_triples_for_subjects(
        self, table_name: str, subjects: List[str]
    ) -> List[Dict[str, str]]:
        if not subjects:
            return []
        node = self._node_table(table_name)
        conn = self._get_connection()
        result = conn.execute(
            f"MATCH (t:{node}) WHERE t.subject IN $subjects "
            f"RETURN t.subject AS subject, t.predicate AS predicate, "
            f"t.object AS object",
            parameters={"subjects": subjects},
        )
        return [
            {"subject": row[0], "predicate": row[1], "object": row[2]}
            for row in result
        ]

    def get_predicates_for_type(
        self, table_name: str, type_uri: str
    ) -> List[str]:
        node = self._node_table(table_name)
        conn = self._get_connection()
        sample = conn.execute(
            f"MATCH (t:{node}) "
            f"WHERE t.predicate = $rdf_type AND t.object = $type_uri "
            f"RETURN t.subject LIMIT 1",
            parameters={"rdf_type": RDF_TYPE, "type_uri": type_uri},
        )
        sample_row = sample.get_next() if sample.has_next() else None
        if not sample_row:
            return []
        subj = sample_row[0]
        pred_result = conn.execute(
            f"MATCH (t:{node}) WHERE t.subject = $subj "
            f"RETURN DISTINCT t.predicate AS predicate",
            parameters={"subj": subj},
        )
        return [row[0] for row in pred_result]

    def paginated_triples(
        self,
        table_name: str,
        conditions: List[str],
        limit: int,
        offset: int,
    ) -> List[Dict[str, str]]:
        node = self._node_table(table_name)
        conn = self._get_connection()
        cypher_conditions = self._translate_conditions(conditions, "t")
        where = f" WHERE {' AND '.join(cypher_conditions)}" if cypher_conditions else ""
        result = conn.execute(
            f"MATCH (t:{node}){where} "
            f"RETURN t.subject AS subject, t.predicate AS predicate, "
            f"t.object AS object "
            f"SKIP {int(offset)} LIMIT {int(limit)}"
        )
        return [
            {"subject": row[0], "predicate": row[1], "object": row[2]}
            for row in result
        ]

    def paginated_count(
        self, table_name: str, conditions: List[str]
    ) -> int:
        node = self._node_table(table_name)
        conn = self._get_connection()
        cypher_conditions = self._translate_conditions(conditions, "t")
        where = f" WHERE {' AND '.join(cypher_conditions)}" if cypher_conditions else ""
        result = conn.execute(
            f"MATCH (t:{node}){where} RETURN COUNT(t) AS cnt"
        )
        row = result.get_next()
        return int(row[0]) if row else 0

    def _bfs_resolve_seeds(
        self, conn, node: str, entity_type: str, search: str,
    ) -> Set[str]:
        """Resolve BFS seed entities by type filter, search text, or all typed subjects."""
        seeds: Set[str] = set()

        if entity_type:
            et_lower = entity_type.lower()
            type_result = conn.execute(
                f"MATCH (t:{node}) "
                f"WHERE t.predicate = $rdf_type "
                f"AND (toLower(t.object) ENDS WITH $hash_suffix "
                f"  OR toLower(t.object) ENDS WITH $slash_suffix) "
                f"RETURN DISTINCT t.subject",
                parameters={
                    "rdf_type": RDF_TYPE,
                    "hash_suffix": f"#{et_lower}",
                    "slash_suffix": f"/{et_lower}",
                },
            )
            type_seeds = {row[0] for row in type_result}
            seeds = type_seeds if not seeds else seeds & type_seeds

        if search:
            search_lower = search.lower()
            label_result = conn.execute(
                f"MATCH (t:{node}) "
                f"WHERE (t.predicate = $rdfs_label "
                f"  OR t.predicate ENDS WITH '#label' "
                f"  OR t.predicate ENDS WITH '/label' "
                f"  OR t.predicate ENDS WITH '#name' "
                f"  OR t.predicate ENDS WITH '/name') "
                f"AND toLower(t.object) CONTAINS $search "
                f"RETURN DISTINCT t.subject",
                parameters={"rdfs_label": RDFS_LABEL, "search": search_lower},
            )
            search_seeds = {row[0] for row in label_result}

            uri_result = conn.execute(
                f"MATCH (t:{node}) "
                f"WHERE t.predicate = $rdf_type "
                f"AND (toLower(t.subject) CONTAINS $slash_search "
                f"  OR toLower(t.subject) CONTAINS $hash_search) "
                f"RETURN DISTINCT t.subject",
                parameters={
                    "rdf_type": RDF_TYPE,
                    "slash_search": f"/{search_lower}",
                    "hash_search": f"#{search_lower}",
                },
            )
            for row in uri_result:
                search_seeds.add(row[0])

            seeds = search_seeds if not seeds else seeds & search_seeds

        if not search and not entity_type:
            all_result = conn.execute(
                f"MATCH (t:{node}) "
                f"WHERE t.predicate = $rdf_type "
                f"RETURN DISTINCT t.subject AS entity",
                parameters={"rdf_type": RDF_TYPE},
            )
            seeds = {row[0] for row in all_result}

        return seeds

    def _bfs_expand_level(
        self, conn, node: str, current_level: Set[str], entity_levels: Dict[str, int], lvl: int,
    ) -> Set[str]:
        """Expand one BFS level by querying forward and reverse edges. Returns the new frontier."""
        neighbors_result = conn.execute(
            f"MATCH (t:{node}) "
            f"WHERE t.subject IN $entities "
            f"AND t.predicate <> $rdf_type "
            f"AND t.predicate <> $rdfs_label "
            f"AND NOT t.predicate ENDS WITH '#label' "
            f"AND NOT t.predicate ENDS WITH '/label' "
            f"AND (t.object STARTS WITH 'http://' OR t.object STARTS WITH 'https://') "
            f"RETURN DISTINCT t.object AS neighbor",
            parameters={
                "entities": list(current_level),
                "rdf_type": RDF_TYPE,
                "rdfs_label": RDFS_LABEL,
            },
        )
        new_level: Set[str] = set()
        for row in neighbors_result:
            nb = row[0]
            if nb not in entity_levels:
                entity_levels[nb] = lvl
                new_level.add(nb)

        reverse_result = conn.execute(
            f"MATCH (t:{node}) "
            f"WHERE t.object IN $entities "
            f"AND t.predicate <> $rdf_type "
            f"AND t.predicate <> $rdfs_label "
            f"AND NOT t.predicate ENDS WITH '#label' "
            f"AND NOT t.predicate ENDS WITH '/label' "
            f"RETURN DISTINCT t.subject AS neighbor",
            parameters={
                "entities": list(current_level),
                "rdf_type": RDF_TYPE,
                "rdfs_label": RDFS_LABEL,
            },
        )
        for row in reverse_result:
            nb = row[0]
            if nb not in entity_levels:
                entity_levels[nb] = lvl
                new_level.add(nb)

        return new_level

    def bfs_traversal(
        self, table_name: str, seed_where: str, depth: int,
        search: str = "", entity_type: str = "",
    ) -> List[Dict[str, Any]]:
        """BFS traversal using iterative Cypher queries on the flat table."""
        node = self._node_table(table_name)
        conn = self._get_connection()

        seeds = self._bfs_resolve_seeds(conn, node, entity_type, search)
        if not seeds:
            return []

        entity_levels: Dict[str, int] = {s: 0 for s in seeds}
        current_level = set(seeds)

        for lvl in range(1, depth + 1):
            if not current_level:
                break
            current_level = self._bfs_expand_level(conn, node, current_level, entity_levels, lvl)

        return [
            {"entity": e, "min_lvl": l}
            for e, l in entity_levels.items()
        ]

    def find_subjects_by_patterns(
        self, table_name: str, like_patterns: List[str]
    ) -> Set[str]:
        if not like_patterns:
            return set()
        node = self._node_table(table_name)
        conn = self._get_connection()
        results: Set[str] = set()
        for pattern in like_patterns:
            suffix = pattern.lstrip("%")
            result = conn.execute(
                f"MATCH (t:{node}) "
                f"WHERE t.subject ENDS WITH $suffix "
                f"RETURN DISTINCT t.subject",
                parameters={"suffix": suffix},
            )
            for row in result:
                results.add(row[0])
        return results

    # -- Reasoning overrides (Cypher on flat table) -----------------------

    def transitive_closure(
        self,
        table_name: str,
        predicate_uri: str,
        start_uri: Optional[str] = None,
        max_depth: int = 20,
    ) -> List[Dict[str, Any]]:
        """Compute transitive closure using iterative Cypher on the flat table."""
        node = self._node_table(table_name)
        conn = self._get_connection()

        direct_result = conn.execute(
            f"MATCH (t:{node}) WHERE t.predicate = $pred RETURN t.subject, t.object",
            parameters={"pred": predicate_uri},
        )
        edges: List[tuple] = [(row[0], row[1]) for row in direct_result]
        if not edges:
            return []

        graph_fwd: Dict[str, Set[str]] = {}
        existing: Set[tuple] = set()
        for s, o in edges:
            graph_fwd.setdefault(s, set()).add(o)
            existing.add((s, o))

        inferred: List[Dict[str, Any]] = []
        for start in list(graph_fwd.keys()):
            if start_uri and start != start_uri:
                continue
            visited: Set[str] = set()
            frontier = set(graph_fwd.get(start, set()))
            depth = 1
            while frontier and depth < max_depth:
                next_frontier: Set[str] = set()
                for mid in frontier:
                    if mid in visited:
                        continue
                    visited.add(mid)
                    if (start, mid) not in existing:
                        inferred.append({
                            "subject": start,
                            "predicate": predicate_uri,
                            "object": mid,
                        })
                    for nxt in graph_fwd.get(mid, set()):
                        if nxt not in visited:
                            next_frontier.add(nxt)
                frontier = next_frontier
                depth += 1

        logger.info("Flat transitive closure: %d inferred for %s", len(inferred), predicate_uri)
        return inferred

    def symmetric_expand(
        self,
        table_name: str,
        predicate_uri: str,
    ) -> List[Dict[str, Any]]:
        """Find missing symmetric counterparts using Cypher on the flat table."""
        node = self._node_table(table_name)
        conn = self._get_connection()

        result = conn.execute(
            f"MATCH (t:{node}) WHERE t.predicate = $pred RETURN t.subject, t.object",
            parameters={"pred": predicate_uri},
        )
        pairs: Set[tuple] = set()
        for row in result:
            pairs.add((row[0], row[1]))

        inferred: List[Dict[str, Any]] = []
        for s, o in pairs:
            if (o, s) not in pairs:
                inferred.append({
                    "subject": o,
                    "predicate": predicate_uri,
                    "object": s,
                })

        logger.info("Flat symmetric expand: %d missing for %s", len(inferred), predicate_uri)
        return inferred

    def shortest_path(
        self,
        table_name: str,
        source_uri: str,
        target_uri: str,
        max_depth: int = 10,
    ) -> List[Dict[str, Any]]:
        """Shortest path not supported on flat model."""
        return []

    def expand_entity_neighbors(
        self, table_name: str, entity_uris: Set[str]
    ) -> Set[str]:
        if not entity_uris:
            return set()
        node = self._node_table(table_name)
        conn = self._get_connection()
        uris = list(entity_uris)

        forward = conn.execute(
            f"MATCH (t:{node}) "
            f"WHERE t.subject IN $uris "
            f"AND t.object STARTS WITH 'http' "
            f"AND t.predicate <> $rdf_type "
            f"AND t.predicate <> $rdfs_label "
            f"RETURN DISTINCT t.object AS entity",
            parameters={"uris": uris, "rdf_type": RDF_TYPE, "rdfs_label": RDFS_LABEL},
        )
        candidates = {row[0] for row in forward}

        reverse = conn.execute(
            f"MATCH (t:{node}) "
            f"WHERE t.object IN $uris "
            f"AND t.predicate <> $rdf_type "
            f"AND t.predicate <> $rdfs_label "
            f"RETURN DISTINCT t.subject AS entity",
            parameters={"uris": uris, "rdf_type": RDF_TYPE, "rdfs_label": RDFS_LABEL},
        )
        for row in reverse:
            candidates.add(row[0])

        if not candidates:
            return set()

        typed = conn.execute(
            f"MATCH (t:{node}) "
            f"WHERE t.subject IN $candidates AND t.predicate = $rdf_type "
            f"RETURN DISTINCT t.subject AS entity",
            parameters={"candidates": list(candidates), "rdf_type": RDF_TYPE},
        )
        return {row[0] for row in typed}
