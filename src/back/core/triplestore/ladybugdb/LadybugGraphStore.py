"""LadybugDB graph-model triple store backend.

Uses typed node tables and relationship tables derived from the project
ontology.  Falls back to the flat-model base class for operations that
do not have a graph-specific implementation.
"""
from typing import Any, Callable, Dict, List, Optional, Set

from back.core.logging import get_logger
from back.core.triplestore.constants import RDF_TYPE, RDFS_LABEL
from back.core.triplestore.ladybugdb.LadybugFlatStore import LadybugFlatStore
from back.core.triplestore.ladybugdb.GraphSchema import GraphSchema
from back.core.helpers import validate_table_name
from shared.config.constants import DEFAULT_GRAPH_NAME

logger = get_logger(__name__)


class LadybugGraphStore(LadybugFlatStore):
    """Graph-model LadybugDB backend.

    OWL classes map to node tables and object properties map to
    relationship tables.  When the schema cannot be built or graph tables
    are missing, all inherited flat-model methods remain available as
    fallbacks.
    """

    def __init__(
        self,
        db_path: str = "/tmp/ontobricks",
        db_name: str = DEFAULT_GRAPH_NAME,
        ontology: Optional[Dict[str, Any]] = None,
        auto_restore: Optional[Callable] = None,
    ) -> None:
        super().__init__(
            db_path=db_path, db_name=db_name,
            ontology=ontology, auto_restore=auto_restore,
        )
        self._init_graph_schema()

    # -- Schema initialisation -------------------------------------------

    def _init_graph_schema(self) -> None:
        """Build the graph schema from ontology if available.

        Probes the database to verify expected node tables exist.
        Falls back to flat mode if they don't.
        """
        if self._graph_schema is not None or self._graph_schema_checked:
            return
        if not self._ontology:
            self._graph_schema_checked = True
            return
        try:
            from back.core.triplestore.ladybugdb.GraphSchemaBuilder import GraphSchemaBuilder
            classes = self._ontology.get("classes", [])
            properties = self._ontology.get("properties", [])
            base_uri = self._ontology.get("base_uri", "")
            if classes:
                schema = GraphSchemaBuilder.generate_graph_schema(
                    classes, properties, base_uri=base_uri
                )
                if self._graph_tables_exist(schema):
                    self._graph_schema = schema
                    logger.info(
                        "LadybugDB graph schema initialised: %d node tables, %d rel tables",
                        len(schema.node_tables),
                        len(schema.rel_tables),
                    )
                else:
                    logger.info(
                        "LadybugDB: graph tables not found in database — using flat model"
                    )
        except Exception as e:
            logger.warning("Could not build graph schema: %s", e)
            self._graph_schema = None
        finally:
            self._graph_schema_checked = True

    def _graph_tables_exist(self, schema) -> bool:
        """Return True if at least one expected graph node table exists."""
        try:
            conn = self._get_connection()
            for tbl in schema.node_tables:
                if tbl == schema.fallback_node_table:
                    continue
                try:
                    conn.execute(f"MATCH (n:{tbl}) RETURN n.uri LIMIT 0")
                    return True
                except Exception:
                    continue
            return False
        except Exception:
            return False

    @property
    def use_graph_model(self) -> bool:
        self._init_graph_schema()
        return self._graph_schema is not None

    # -- Core CRUD overrides ---------------------------------------------

    def create_table(self, table_name: str) -> None:
        validate_table_name(table_name)
        conn = self._get_connection()
        if self.use_graph_model:
            from back.core.triplestore.ladybugdb.GraphSchemaBuilder import GraphSchemaBuilder
            stmts = GraphSchemaBuilder.generate_ddl(self._graph_schema)
            for stmt in stmts:
                try:
                    conn.execute(stmt)
                except Exception as e:
                    logger.warning("DDL statement failed (may already exist): %s — %s", stmt[:80], e)
            for tbl in self._graph_schema.node_tables:
                self._table_registry[tbl] = True
            logger.info("Created LadybugDB graph schema (%d DDL statements)", len(stmts))
        else:
            super().create_table(table_name)

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
        if self.use_graph_model:
            return self._insert_triples_graph(triples, batch_size, on_progress)
        return super().insert_triples(table_name, triples, batch_size, on_progress)

    def _insert_triples_graph(
        self,
        triples: List[Dict[str, str]],
        batch_size: int,
        on_progress: Optional[Callable[[int, int], None]],
    ) -> int:
        """Insert triples using the true graph model."""
        from back.core.triplestore.ladybugdb.GraphSchemaBuilder import GraphSchemaBuilder

        schema = self._graph_schema
        conn = self._get_connection()

        node_inserts, rel_inserts, attr_updates = GraphSchemaBuilder.classify_triples(
            triples, schema,
        )

        total_ops = (
            sum(len(nodes) for nodes in node_inserts.values())
            + len(rel_inserts)
        )
        completed = 0

        created_uris: Set[str] = set()

        for table_name, nodes in node_inserts.items():
            for node in nodes:
                uri = node["uri"]
                if uri in created_uris:
                    continue
                label = (node.get("label", "") or "").replace("'", "\\'")
                uri_esc = uri.replace("'", "\\'")
                try:
                    conn.execute(
                        f"CREATE (:{table_name} {{uri: '{uri_esc}', label: '{label}'}})"
                    )
                    created_uris.add(uri)
                except Exception as e:
                    logger.debug("Node insert failed (may exist): %s — %s", uri[:60], e)
                completed += 1
                if on_progress and completed % batch_size == 0:
                    on_progress(completed, total_ops)

        for rel in rel_inserts:
            rel_table = rel["rel_table"]
            from_uri = rel["from_uri"].replace("'", "\\'")
            to_uri = rel["to_uri"].replace("'", "\\'")
            rel_def = schema.rel_tables.get(rel_table)
            if not rel_def:
                continue

            from_tbl = rel_def.from_table
            to_tbl = rel_def.to_table

            if rel["from_uri"] not in created_uris:
                try:
                    conn.execute(
                        f"CREATE (:{from_tbl} {{uri: '{from_uri}', label: ''}})"
                    )
                    created_uris.add(rel["from_uri"])
                except Exception:
                    pass
            if rel["to_uri"] not in created_uris:
                try:
                    conn.execute(
                        f"CREATE (:{to_tbl} {{uri: '{to_uri}', label: ''}})"
                    )
                    created_uris.add(rel["to_uri"])
                except Exception:
                    pass

            try:
                conn.execute(
                    f"MATCH (a:{from_tbl} {{uri: '{from_uri}'}}), "
                    f"(b:{to_tbl} {{uri: '{to_uri}'}}) "
                    f"CREATE (a)-[:{rel_table}]->(b)"
                )
            except Exception as e:
                logger.debug("Relationship insert failed: %s — %s", rel_table, e)
            completed += 1
            if on_progress and completed % batch_size == 0:
                on_progress(completed, total_ops)

        attr_applied = 0
        for subj_uri, attrs in attr_updates.items():
            subj_table = None
            for tbl_name, nodes in node_inserts.items():
                if any(n["uri"] == subj_uri for n in nodes):
                    subj_table = tbl_name
                    break
            if not subj_table:
                for tbl_name, node_def in schema.node_tables.items():
                    if tbl_name == schema.fallback_node_table:
                        continue
                    col = schema.resolve_data_property(tbl_name, attrs[0]["predicate"])
                    if col:
                        subj_table = tbl_name
                        break
            if not subj_table:
                continue

            set_parts = []
            for attr in attrs:
                col = schema.resolve_data_property(subj_table, attr["predicate"])
                if col:
                    val = (attr.get("value", "") or "").replace("'", "\\'")
                    set_parts.append(f"n.{col} = '{val}'")
            if set_parts:
                uri_esc = subj_uri.replace("'", "\\'")
                try:
                    conn.execute(
                        f"MATCH (n:{subj_table} {{uri: '{uri_esc}'}}) "
                        f"SET {', '.join(set_parts)}"
                    )
                    attr_applied += len(set_parts)
                except Exception as e:
                    logger.debug("Attr update failed for %s: %s", subj_uri[:60], e)

        if on_progress:
            on_progress(total_ops, total_ops)

        logger.info(
            "Graph insert: %d nodes, %d relationships, %d attributes",
            len(created_uris), len(rel_inserts), attr_applied,
        )
        return len(triples)

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
        if self.use_graph_model:
            return self._delete_triples_graph(triples, batch_size, on_progress)
        return super().delete_triples(table_name, triples, batch_size, on_progress)

    def _delete_triples_graph(
        self,
        triples: List[Dict[str, str]],
        batch_size: int,
        on_progress: Optional[Callable[[int, int], None]],
    ) -> int:
        """Delete triples from the graph model."""
        from back.core.triplestore.ladybugdb.GraphSchemaBuilder import GraphSchemaBuilder

        schema = self._graph_schema
        conn = self._get_connection()

        node_inserts, rel_inserts, attr_updates = GraphSchemaBuilder.classify_triples(
            triples, schema,
        )

        deleted = 0
        total = len(triples)

        for rel in rel_inserts:
            rel_table = rel["rel_table"]
            from_uri = rel["from_uri"].replace("'", "\\'")
            to_uri = rel["to_uri"].replace("'", "\\'")
            rel_def = schema.rel_tables.get(rel_table)
            if not rel_def:
                continue
            try:
                conn.execute(
                    f"MATCH (a:{rel_def.from_table} {{uri: '{from_uri}'}})"
                    f"-[r:{rel_table}]->"
                    f"(b:{rel_def.to_table} {{uri: '{to_uri}'}}) "
                    f"DELETE r"
                )
                deleted += 1
            except Exception as e:
                logger.debug("Graph rel delete failed: %s — %s", rel_table, e)

        for subj_uri, attrs in attr_updates.items():
            subj_table = None
            for tbl_name, nodes in node_inserts.items():
                if any(n["uri"] == subj_uri for n in nodes):
                    subj_table = tbl_name
                    break
            if not subj_table:
                for tbl_name in schema.node_tables:
                    if tbl_name == schema.fallback_node_table:
                        continue
                    col = schema.resolve_data_property(tbl_name, attrs[0]["predicate"])
                    if col:
                        subj_table = tbl_name
                        break
            if not subj_table:
                continue

            set_parts = []
            for attr in attrs:
                col = schema.resolve_data_property(subj_table, attr["predicate"])
                if col:
                    set_parts.append(f"n.{col} = NULL")
            if set_parts:
                uri_esc = subj_uri.replace("'", "\\'")
                try:
                    conn.execute(
                        f"MATCH (n:{subj_table} {{uri: '{uri_esc}'}}) "
                        f"SET {', '.join(set_parts)}"
                    )
                    deleted += len(set_parts)
                except Exception as e:
                    logger.debug("Graph attr delete failed for %s: %s", subj_uri[:60], e)

        nodes_to_delete: Set[str] = set()
        for tbl_name, nodes in node_inserts.items():
            for node in nodes:
                uri = node["uri"]
                if uri not in nodes_to_delete:
                    uri_esc = uri.replace("'", "\\'")
                    try:
                        conn.execute(
                            f"MATCH (n:{tbl_name} {{uri: '{uri_esc}'}}) DETACH DELETE n"
                        )
                        nodes_to_delete.add(uri)
                        deleted += 1
                    except Exception as e:
                        logger.debug("Graph node delete failed: %s — %s", uri[:60], e)

        if on_progress:
            on_progress(total, total)

        logger.info("Graph delete: %d operations for %d triples", deleted, total)
        return deleted

    # -- Query overrides -------------------------------------------------

    def query_triples(self, table_name: str) -> List[Dict[str, str]]:
        validate_table_name(table_name)
        if self.use_graph_model:
            return self._materialize_all_triples()
        return super().query_triples(table_name)

    def count_triples(self, table_name: str) -> int:
        validate_table_name(table_name)
        if self.use_graph_model:
            count = self._count_graph_triples()
            if count > 0:
                return count
        return super().count_triples(table_name)

    def table_exists(self, table_name: str) -> bool:
        if not table_name or not table_name.strip():
            return False
        if self._table_registry:
            return True
        conn = self._get_connection()
        if self.use_graph_model:
            for tbl_name in self._graph_schema.node_tables:
                try:
                    conn.execute(f"MATCH (n:{tbl_name}) RETURN n LIMIT 0")
                    self._table_registry[tbl_name] = True
                    return True
                except Exception:
                    pass
        return super().table_exists(table_name)

    def get_aggregate_stats(self, table_name: str) -> Dict[str, int]:
        if self.use_graph_model:
            return self._aggregate_stats_graph()
        return super().get_aggregate_stats(table_name)

    def find_subjects_by_type(
        self,
        table_name: str,
        type_uri: str,
        limit: int = 50,
        offset: int = 0,
        search: Optional[str] = None,
    ) -> List[str]:
        if self.use_graph_model:
            return self._find_subjects_by_type_graph(type_uri, limit, offset, search)
        return super().find_subjects_by_type(table_name, type_uri, limit, offset, search)

    def get_triples_for_subjects(
        self, table_name: str, subjects: List[str]
    ) -> List[Dict[str, str]]:
        if not subjects:
            return []
        if self.use_graph_model:
            rows = self._get_triples_for_subjects_graph(subjects)
            if rows:
                return rows
        return super().get_triples_for_subjects(table_name, subjects)

    def get_predicates_for_type(
        self, table_name: str, type_uri: str
    ) -> List[str]:
        if self.use_graph_model:
            return self._get_predicates_for_type_graph(type_uri)
        return super().get_predicates_for_type(table_name, type_uri)

    def bfs_traversal(
        self, table_name: str, seed_where: str, depth: int,
        search: str = "", entity_type: str = "",
    ) -> List[Dict[str, Any]]:
        if self.use_graph_model:
            return self._bfs_traversal_graph(seed_where, depth,
                                             search=search, entity_type=entity_type)
        return super().bfs_traversal(table_name, seed_where, depth,
                                     search=search, entity_type=entity_type)

    # -- Reasoning methods ------------------------------------------------

    def transitive_closure(
        self,
        table_name: str,
        predicate_uri: str,
        start_uri: Optional[str] = None,
        max_depth: int = 20,
    ) -> List[Dict[str, Any]]:
        """Compute transitive closure using variable-length Cypher paths."""
        if not self.use_graph_model:
            logger.debug("transitive_closure: graph model not active, delegating to flat model")
            return super().transitive_closure(table_name, predicate_uri, start_uri, max_depth)

        schema = self._graph_schema
        rel_table = schema.property_uri_to_table.get(predicate_uri)
        if not rel_table:
            logger.debug("transitive_closure: no rel table for %s, skipping", predicate_uri)
            return []

        rel_def = schema.rel_tables.get(rel_table)
        if not rel_def:
            return []

        conn = self._get_connection()
        from_tbl = rel_def.from_table
        to_tbl = rel_def.to_table

        not_exists = f"WHERE NOT EXISTS {{ MATCH (a)-[:{rel_table}]->(b) }}"
        if start_uri:
            start_esc = start_uri.replace("'", "\\'")
            cypher = (
                f"MATCH (a:{from_tbl})-[:{rel_table}*2..{int(max_depth)} acyclic]->(b:{to_tbl}) "
                f"WHERE a.uri = '{start_esc}' "
                f"AND NOT EXISTS {{ MATCH (a)-[:{rel_table}]->(b) }} "
                f"RETURN DISTINCT a.uri AS subject, b.uri AS object"
            )
        else:
            cypher = (
                f"MATCH (a:{from_tbl})-[:{rel_table}*2..{int(max_depth)} acyclic]->(b:{to_tbl}) "
                f"{not_exists} "
                f"RETURN DISTINCT a.uri AS subject, b.uri AS object"
            )

        try:
            r = conn.execute(cypher)
            results = []
            for row in r:
                results.append({
                    "subject": row[0],
                    "predicate": predicate_uri,
                    "object": row[1],
                })
            logger.info("Transitive closure via Cypher: %d inferred triples for %s", len(results), rel_table)
            return results
        except Exception as e:
            logger.warning("Cypher transitive closure failed: %s", e)
            return []

    def symmetric_expand(
        self,
        table_name: str,
        predicate_uri: str,
    ) -> List[Dict[str, Any]]:
        """Find missing symmetric counterparts using Cypher."""
        if not self.use_graph_model:
            logger.debug("symmetric_expand: graph model not active, delegating to flat model")
            return super().symmetric_expand(table_name, predicate_uri)

        schema = self._graph_schema
        rel_table = schema.property_uri_to_table.get(predicate_uri)
        if not rel_table:
            logger.debug("symmetric_expand: no rel table for %s, skipping", predicate_uri)
            return []

        rel_def = schema.rel_tables.get(rel_table)
        if not rel_def:
            return []

        conn = self._get_connection()
        from_tbl = rel_def.from_table
        to_tbl = rel_def.to_table

        cypher = (
            f"MATCH (a:{from_tbl})-[:{rel_table}]->(b:{to_tbl}) "
            f"WHERE NOT EXISTS {{ MATCH (b)-[:{rel_table}]->(a) }} "
            f"RETURN DISTINCT b.uri AS subject, a.uri AS object"
        )

        try:
            r = conn.execute(cypher)
            results = []
            for row in r:
                results.append({
                    "subject": row[0],
                    "predicate": predicate_uri,
                    "object": row[1],
                })
            logger.info("Symmetric expand via Cypher: %d missing triples for %s", len(results), rel_table)
            return results
        except Exception as e:
            logger.warning("Cypher symmetric expand failed: %s", e)
            return []

    def shortest_path(
        self,
        table_name: str,
        source_uri: str,
        target_uri: str,
        max_depth: int = 10,
    ) -> List[Dict[str, Any]]:
        """Find shortest path using Cypher SHORTEST algorithm."""
        if not self.use_graph_model:
            return []

        conn = self._get_connection()
        src_esc = source_uri.replace("'", "\\'")
        tgt_esc = target_uri.replace("'", "\\'")

        cypher = (
            f"MATCH (a {{uri: '{src_esc}'}})-[e* SHORTEST 1..{int(max_depth)}]->(b {{uri: '{tgt_esc}'}}) "
            f"RETURN length(e) AS hops, properties(nodes(e), 'uri') AS path_uris"
        )

        try:
            r = conn.execute(cypher)
            results = []
            for row in r:
                results.append({
                    "source": source_uri,
                    "target": target_uri,
                    "hops": row[0],
                    "path": row[1] if len(row) > 1 else [],
                })
            return results
        except Exception as e:
            logger.debug("Cypher shortest path failed: %s", e)
            return []

    # -- Graph-model implementations -------------------------------------

    def _count_graph_triples(self) -> int:
        """Count materialized triples from the graph model."""
        schema = self._graph_schema
        conn = self._get_connection()
        total = 0
        for tbl_name in schema.node_tables:
            if tbl_name == schema.fallback_node_table:
                continue
            try:
                r = conn.execute(f"MATCH (n:{tbl_name}) RETURN COUNT(n) AS cnt")
                row = r.get_next()
                node_count = int(row[0]) if row else 0
                total += node_count * 2
            except Exception:
                pass
        for rel_name in schema.rel_tables:
            try:
                r = conn.execute(f"MATCH ()-[r:{rel_name}]->() RETURN COUNT(r) AS cnt")
                row = r.get_next()
                total += int(row[0]) if row else 0
            except Exception:
                pass
        return total

    def _materialize_all_triples(self) -> List[Dict[str, str]]:
        """Reconstruct (s, p, o) triples from the graph model."""
        schema = self._graph_schema
        conn = self._get_connection()
        triples: List[Dict[str, str]] = []

        for tbl_name, node_def in schema.node_tables.items():
            if tbl_name == schema.fallback_node_table:
                continue
            prop_cols = [GraphSchema.safe_identifier(p) for p in node_def.properties]
            extra_returns = "".join(f", n.{c} AS {c}" for c in prop_cols)
            try:
                r = conn.execute(
                    f"MATCH (n:{tbl_name}) RETURN n.uri AS uri, n.label AS label{extra_returns}"
                )
                for row in r:
                    uri = row[0]
                    triples.append({"subject": uri, "predicate": RDF_TYPE, "object": node_def.class_uri})
                    if row[1]:
                        triples.append({"subject": uri, "predicate": RDFS_LABEL, "object": row[1]})
                    for idx, col in enumerate(prop_cols):
                        val_idx = idx + 2
                        if val_idx < len(row) and row[val_idx]:
                            pred_uri = node_def.property_uris.get(col, "")
                            if pred_uri:
                                triples.append({"subject": uri, "predicate": pred_uri, "object": str(row[val_idx])})
            except Exception as exc:
                logger.warning("Materialize node table '%s' failed: %s", tbl_name, exc)

        for rel_name, rel_def in schema.rel_tables.items():
            try:
                r = conn.execute(
                    f"MATCH (a:{rel_def.from_table})-[r:{rel_name}]->(b:{rel_def.to_table}) "
                    f"RETURN a.uri, b.uri"
                )
                count = 0
                for row in r:
                    triples.append({"subject": row[0], "predicate": rel_def.property_uri, "object": row[1]})
                    count += 1
                logger.debug(
                    "Materialized %d triples for relationship '%s' (predicate='%s')",
                    count, rel_name, rel_def.property_uri,
                )
            except Exception as exc:
                logger.warning(
                    "Materialize relationship '%s' (%s→%s) failed: %s",
                    rel_name, rel_def.from_table, rel_def.to_table, exc,
                )

        return triples

    def _aggregate_stats_graph(self) -> Dict[str, int]:
        """Aggregate stats from the graph model."""
        schema = self._graph_schema
        conn = self._get_connection()
        total_nodes = 0
        total_rels = 0
        distinct_subjects: Set[str] = set()

        for tbl_name in schema.node_tables:
            if tbl_name == schema.fallback_node_table:
                continue
            try:
                r = conn.execute(f"MATCH (n:{tbl_name}) RETURN n.uri")
                for row in r:
                    distinct_subjects.add(row[0])
                    total_nodes += 1
            except Exception:
                pass

        for rel_name in schema.rel_tables:
            try:
                r = conn.execute(f"MATCH ()-[r:{rel_name}]->() RETURN COUNT(r) AS cnt")
                row = r.get_next()
                total_rels += int(row[0]) if row else 0
            except Exception:
                pass

        type_cnt = total_nodes
        label_cnt = total_nodes
        total = type_cnt + label_cnt + total_rels
        pred_count = len(schema.rel_tables) + 2

        return {
            "total": total,
            "distinct_subjects": len(distinct_subjects),
            "distinct_predicates": pred_count,
            "type_assertion_count": type_cnt,
            "label_count": label_cnt,
        }

    def _find_subjects_by_type_graph(
        self, type_uri: str, limit: int, offset: int, search: Optional[str]
    ) -> List[str]:
        """Find subjects by type using node table lookup."""
        schema = self._graph_schema
        tbl = schema.get_node_table(type_uri)
        if not tbl:
            logger.warning("LadybugDB graph: no node table for type_uri=%s", type_uri)
            return []
        conn = self._get_connection()
        node_def = schema.node_tables.get(tbl)
        logger.info("LadybugDB graph search: table=%s, type_uri=%s, search=%s", tbl, type_uri, search)

        if search:
            search_lower = search.lower()
            where_clauses = ["LOWER(n.label) CONTAINS $search"]
            if node_def:
                for prop_name in node_def.properties:
                    col = GraphSchema.safe_identifier(prop_name)
                    where_clauses.append(f"LOWER(n.{col}) CONTAINS $search")
            where_expr = " OR ".join(where_clauses)
            r = conn.execute(
                f"MATCH (n:{tbl}) WHERE {where_expr} "
                f"RETURN n.uri ORDER BY n.uri SKIP {int(offset)} LIMIT {int(limit)}",
                parameters={"search": search_lower},
            )
        else:
            r = conn.execute(
                f"MATCH (n:{tbl}) "
                f"RETURN n.uri ORDER BY n.uri SKIP {int(offset)} LIMIT {int(limit)}"
            )
        return [row[0] for row in r]

    def _get_triples_for_subjects_graph(
        self, subjects: List[str]
    ) -> List[Dict[str, str]]:
        """Reconstruct triples for specific subjects from the graph model."""
        schema = self._graph_schema
        conn = self._get_connection()
        triples: List[Dict[str, str]] = []

        for tbl_name, node_def in schema.node_tables.items():
            if tbl_name == schema.fallback_node_table:
                continue

            prop_cols = [GraphSchema.safe_identifier(p) for p in node_def.properties]
            extra_returns = "".join(f", n.{c} AS {c}" for c in prop_cols)

            try:
                r = conn.execute(
                    f"MATCH (n:{tbl_name}) WHERE n.uri IN $subjects "
                    f"RETURN n.uri AS uri, n.label AS label{extra_returns}",
                    parameters={"subjects": subjects},
                )
                for row in r:
                    uri = row[0]
                    triples.append({"subject": uri, "predicate": RDF_TYPE, "object": node_def.class_uri})
                    if row[1]:
                        triples.append({"subject": uri, "predicate": RDFS_LABEL, "object": row[1]})
                    for idx, col in enumerate(prop_cols):
                        val_idx = idx + 2
                        if val_idx < len(row) and row[val_idx]:
                            pred_uri = node_def.property_uris.get(col, "")
                            if pred_uri:
                                triples.append({"subject": uri, "predicate": pred_uri, "object": str(row[val_idx])})
            except Exception as e:
                logger.debug("Graph triples query failed for %s: %s", tbl_name, e)

        for rel_name, rel_def in schema.rel_tables.items():
            try:
                r = conn.execute(
                    f"MATCH (a:{rel_def.from_table})-[r:{rel_name}]->(b:{rel_def.to_table}) "
                    f"WHERE a.uri IN $subjects "
                    f"RETURN a.uri, b.uri",
                    parameters={"subjects": subjects},
                )
                for row in r:
                    triples.append({"subject": row[0], "predicate": rel_def.property_uri, "object": row[1]})
            except Exception:
                pass

        return triples

    def _get_predicates_for_type_graph(self, type_uri: str) -> List[str]:
        """Return all predicates available for a type in the graph model."""
        schema = self._graph_schema
        tbl = schema.get_node_table(type_uri)
        if not tbl:
            return []
        node_def = schema.node_tables.get(tbl)
        if not node_def:
            return []
        predicates = [RDF_TYPE, RDFS_LABEL]
        for _col, pred_uri in node_def.property_uris.items():
            predicates.append(pred_uri)
        for _rel_name, rel_def in schema.rel_tables.items():
            if rel_def.from_table == tbl:
                predicates.append(rel_def.property_uri)
        return predicates

    def _bfs_traversal_graph(
        self, seed_where: str, depth: int,
        search: str = "", entity_type: str = "",
    ) -> List[Dict[str, Any]]:
        """BFS traversal using native graph relationships."""
        schema = self._graph_schema
        conn = self._get_connection()

        seeds: Set[str] = set()
        search_lower = search.lower() if search else ""
        et_lower = entity_type.lower() if entity_type else ""

        for tbl_name, node_def in schema.node_tables.items():
            if tbl_name == schema.fallback_node_table:
                continue
            if et_lower and tbl_name.lower() != et_lower:
                continue
            try:
                if search_lower:
                    where_parts = [
                        "toLower(n.label) CONTAINS $search",
                        "toLower(n.uri) CONTAINS $search",
                    ]
                    for prop_name in node_def.properties:
                        col = GraphSchema.safe_identifier(prop_name)
                        where_parts.append(f"toLower(n.{col}) CONTAINS $search")
                    where_clause = " OR ".join(where_parts)
                    r = conn.execute(
                        f"MATCH (n:{tbl_name}) WHERE {where_clause} RETURN n.uri",
                        parameters={"search": search_lower},
                    )
                else:
                    r = conn.execute(f"MATCH (n:{tbl_name}) RETURN n.uri")
                for row in r:
                    seeds.add(row[0])
            except Exception:
                pass

        if not seeds:
            return []

        entity_levels: Dict[str, int] = {s: 0 for s in seeds}
        current_level = set(seeds)

        for lvl in range(1, depth + 1):
            if not current_level:
                break
            new_level: Set[str] = set()
            for rel_name, rel_def in schema.rel_tables.items():
                try:
                    r = conn.execute(
                        f"MATCH (a:{rel_def.from_table})-[:{rel_name}]->(b:{rel_def.to_table}) "
                        f"WHERE a.uri IN $uris "
                        f"RETURN DISTINCT b.uri",
                        parameters={"uris": list(current_level)},
                    )
                    for row in r:
                        if row[0] not in entity_levels:
                            entity_levels[row[0]] = lvl
                            new_level.add(row[0])
                except Exception:
                    pass

                try:
                    r = conn.execute(
                        f"MATCH (a:{rel_def.from_table})-[:{rel_name}]->(b:{rel_def.to_table}) "
                        f"WHERE b.uri IN $uris "
                        f"RETURN DISTINCT a.uri",
                        parameters={"uris": list(current_level)},
                    )
                    for row in r:
                        if row[0] not in entity_levels:
                            entity_levels[row[0]] = lvl
                            new_level.add(row[0])
                except Exception:
                    pass

            current_level = new_level

        return [
            {"entity": e, "min_lvl": l}
            for e, l in entity_levels.items()
        ]
