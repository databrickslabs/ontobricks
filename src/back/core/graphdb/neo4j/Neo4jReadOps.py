"""Neo4j read-side queries over the **typed property-graph** model.

Implements the named-query methods of the ``GraphDBBackend`` contract against
the property graph written by :class:`Neo4jWriteOps`: nodes keyed on ``uri``
carry a graph **marker label**, class labels (from ``rdf:type``), a ``name``
property (from ``rdfs:label``), literal properties, and outgoing relationships.

**Contract preservation:** every method still returns the same
``{subject, predicate, object}`` / entity shapes the SQL backends return — the
triples are *reconstructed* from the graph. Reverse mappings (label→class URI,
reltype→predicate URI, property-key→predicate URI) come from
:class:`Neo4jSchemaMap`.

Traversal/reasoning (``bfs_traversal``, ``transitive_closure``,
``shortest_path``, ``expand_entity_neighbors``) now use **native relationship
patterns** — the whole point of the typed model — instead of self-joining flat
triple nodes.
"""

from typing import Any, Dict, List, Optional, Set

from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL
from back.core.graphdb.neo4j.Neo4jConnection import Neo4jConnection
from back.core.graphdb.neo4j.Neo4jSchemaMap import Neo4jSchemaMap
from back.core.graphdb.neo4j.Neo4jWriteOps import sanitise_label
from back.core.logging import get_logger

logger = get_logger(__name__)

# Reserved node/label names that are never business predicates/properties.
_MARKER_INTERNAL_PROPS = {"uri"}
_SCHEMA_LABEL = "__GraphSchema"


class Neo4jReadOps:
    """Read queries against the typed property-graph Neo4j backend."""

    def __init__(self, connection: Neo4jConnection) -> None:
        self._conn = connection
        self._schema = Neo4jSchemaMap(connection)

    # ------------------------------------------------------------------
    #  SPO reconstruction core
    # ------------------------------------------------------------------

    def _reconstruct_triples_for_nodes(
        self, label: str, node_uris: Optional[List[str]] = None
    ) -> List[Dict[str, str]]:
        """Rebuild flat SPO triples for the given node URIs (or the whole graph).

        Emits, per node: one ``rdf:type`` triple per class label; one
        ``rdfs:label`` triple if it has a ``name``; one triple per literal
        property; and one triple per outgoing relationship (reversed to the
        predicate URI via the schema map).
        """
        maps = self._schema.load(label)
        label_map = maps["label_map"]        # sanitised label → class URI
        reltype_map = maps["reltype_map"]    # sanitised reltype → predicate URI
        prop_map = maps["prop_map"]          # sanitised prop key → predicate URI

        where = ""
        params: Dict[str, Any] = {}
        if node_uris is not None:
            where = "WHERE n.uri IN $uris "
            params["uris"] = node_uris

        # Pull node identity, labels, properties.
        node_rows = self._conn.run(
            f"MATCH (n:`{label}`) {where}"
            f"RETURN n.uri AS uri, labels(n) AS labels, properties(n) AS props",
            **params,
        )
        triples: List[Dict[str, str]] = []
        for r in node_rows:
            uri = r["uri"]
            for lbl in r.get("labels") or []:
                if lbl == label or lbl == _SCHEMA_LABEL:
                    continue  # marker / schema labels are not rdf:type
                class_uri = label_map.get(lbl)
                if class_uri:
                    triples.append(
                        {"subject": uri, "predicate": RDF_TYPE, "object": class_uri}
                    )
            props = r.get("props") or {}
            for key, value in props.items():
                if key in _MARKER_INTERNAL_PROPS:
                    continue
                pred = Neo4jSchemaMap.predicate_for_property(prop_map, key)
                triples.append(
                    {"subject": uri, "predicate": pred, "object": str(value)}
                )

        # Pull outgoing relationships → object-property triples.
        rel_rows = self._conn.run(
            f"MATCH (n:`{label}`)-[rel]->(m:`{label}`) "
            + ("WHERE n.uri IN $uris " if node_uris is not None else "")
            + "RETURN n.uri AS subject, type(rel) AS reltype, m.uri AS object",
            **params,
        )
        for r in rel_rows:
            pred = reltype_map.get(r["reltype"], r["reltype"])
            triples.append(
                {"subject": r["subject"], "predicate": pred, "object": r["object"]}
            )
        return triples

    # ======================================================================
    #  Basic CRUD reads
    # ======================================================================

    def query_triples(self, table_name: str) -> List[Dict[str, str]]:
        return self._reconstruct_triples_for_nodes(sanitise_label(table_name))

    def count_triples(self, table_name: str) -> int:
        """Count reconstructed triples: type-labels + name + literal props + rels."""
        label = sanitise_label(table_name)
        rows = self._conn.run(
            f"MATCH (n:`{label}`) "
            f"WITH n, [x IN labels(n) WHERE x <> $marker AND x <> $schema] AS classes "
            f"RETURN "
            f"  sum(size(classes)) AS type_triples, "
            f"  sum(size([k IN keys(n) WHERE k <> 'uri'])) AS prop_triples, "
            f"  count(n) AS nodes",
            marker=label,
            schema=_SCHEMA_LABEL,
        )
        rel_rows = self._conn.run(
            f"MATCH (:`{label}`)-[r]->(:`{label}`) RETURN count(r) AS rels"
        )
        if not rows:
            return 0
        row = rows[0]
        type_triples = int(row.get("type_triples") or 0)
        prop_triples = int(row.get("prop_triples") or 0)  # includes 'name'
        rels = int(rel_rows[0].get("rels") or 0) if rel_rows else 0
        return type_triples + prop_triples + rels

    def table_exists(self, table_name: str) -> bool:
        label = sanitise_label(table_name)
        rows = self._conn.run(
            "SHOW CONSTRAINTS YIELD name WHERE name = $cname RETURN name",
            cname=f"node_{label}_uri",
        )
        if rows:
            return True
        # Fall back to node presence (constraint may have been created under a
        # legacy name, or dropped while data remains).
        node_rows = self._conn.run(
            f"MATCH (n:`{label}`) RETURN count(n) > 0 AS present"
        )
        return bool(node_rows and node_rows[0].get("present"))

    def get_status(self, table_name: str) -> Dict[str, Any]:
        return {
            "count": self.count_triples(table_name),
            "last_modified": None,
            "path": None,
            "format": "neo4j",
        }

    # ======================================================================
    #  Statistics
    # ======================================================================

    def get_aggregate_stats(self, table_name: str) -> Dict[str, int]:
        label = sanitise_label(table_name)
        # Distinct subjects = nodes that carry at least one class label or a name
        # or an outgoing rel (i.e. real triple subjects). Distinct predicates =
        # distinct property keys + relationship types + (rdf:type if any labels).
        node_stats = self._conn.run(
            f"MATCH (n:`{label}`) "
            f"WITH n, [x IN labels(n) WHERE x <> $marker AND x <> $schema] AS classes "
            f"RETURN "
            f"  count(n) AS distinct_subjects, "
            f"  sum(size(classes)) AS type_assertion_count, "
            f"  sum(CASE WHEN n.name IS NULL THEN 0 ELSE 1 END) AS label_count, "
            f"  sum(size([k IN keys(n) WHERE k <> 'uri'])) AS prop_triples, "
            f"  collect(DISTINCT [k IN keys(n) WHERE k <> 'uri']) AS prop_key_sets, "
            f"  size(collect(DISTINCT [x IN labels(n) WHERE x <> $marker AND x <> $schema])) AS _ignore",
            marker=label,
            schema=_SCHEMA_LABEL,
        )
        rel_stats = self._conn.run(
            f"MATCH (:`{label}`)-[r]->(:`{label}`) "
            f"RETURN count(r) AS rels, collect(DISTINCT type(r)) AS rel_types"
        )
        has_labels = self._conn.run(
            f"MATCH (n:`{label}`) "
            f"WHERE any(x IN labels(n) WHERE x <> $marker AND x <> $schema) "
            f"RETURN count(n) AS c",
            marker=label,
            schema=_SCHEMA_LABEL,
        )
        row = node_stats[0] if node_stats else {}
        rel_row = rel_stats[0] if rel_stats else {}

        type_assertions = int(row.get("type_assertion_count") or 0)
        label_count = int(row.get("label_count") or 0)
        prop_triples = int(row.get("prop_triples") or 0)  # includes name
        rels = int(rel_row.get("rels") or 0)
        total = type_assertions + prop_triples + rels

        # distinct predicate URIs: property keys + rel types + rdf:type-if-present
        prop_keys: Set[str] = set()
        for ks in row.get("prop_key_sets") or []:
            prop_keys.update(k for k in ks if k != "uri")
        rel_types = set(rel_row.get("rel_types") or [])
        distinct_predicates = len(prop_keys) + len(rel_types)
        if int((has_labels[0].get("c") if has_labels else 0) or 0) > 0:
            distinct_predicates += 1  # rdf:type

        return {
            "total": total,
            "distinct_subjects": int(row.get("distinct_subjects") or 0),
            "distinct_predicates": distinct_predicates,
            "type_assertion_count": type_assertions,
            "label_count": label_count,
        }

    def get_type_distribution(self, table_name: str) -> List[Dict[str, Any]]:
        """Count nodes per class label, mapped back to class URIs."""
        label = sanitise_label(table_name)
        label_map = self._schema.load(label)["label_map"]
        rows = self._conn.run(
            f"MATCH (n:`{label}`) "
            f"UNWIND [x IN labels(n) WHERE x <> $marker AND x <> $schema] AS cls "
            f"RETURN cls AS class_label, count(*) AS cnt ORDER BY cnt DESC",
            marker=label,
            schema=_SCHEMA_LABEL,
        )
        out: List[Dict[str, Any]] = []
        for r in rows or []:
            cls = r["class_label"]
            out.append({"type_uri": label_map.get(cls, cls), "cnt": int(r["cnt"])})
        return out

    def get_predicate_distribution(self, table_name: str) -> List[Dict[str, Any]]:
        """Count per predicate: literal props + rel types + rdf:type, as URIs."""
        label = sanitise_label(table_name)
        maps = self._schema.load(label)
        prop_map, reltype_map = maps["prop_map"], maps["reltype_map"]
        counts: Dict[str, int] = {}

        prop_rows = self._conn.run(
            f"MATCH (n:`{label}`) UNWIND [k IN keys(n) WHERE k <> 'uri'] AS key "
            f"RETURN key, count(*) AS cnt"
        )
        for r in prop_rows or []:
            pred = Neo4jSchemaMap.predicate_for_property(prop_map, r["key"])
            counts[pred] = counts.get(pred, 0) + int(r["cnt"])

        rel_rows = self._conn.run(
            f"MATCH (:`{label}`)-[rel]->(:`{label}`) "
            f"RETURN type(rel) AS reltype, count(*) AS cnt"
        )
        for r in rel_rows or []:
            pred = reltype_map.get(r["reltype"], r["reltype"])
            counts[pred] = counts.get(pred, 0) + int(r["cnt"])

        type_rows = self._conn.run(
            f"MATCH (n:`{label}`) "
            f"WITH sum(size([x IN labels(n) WHERE x <> $marker AND x <> $schema])) AS c "
            f"RETURN c",
            marker=label,
            schema=_SCHEMA_LABEL,
        )
        tc = int((type_rows[0].get("c") if type_rows else 0) or 0)
        if tc:
            counts[RDF_TYPE] = counts.get(RDF_TYPE, 0) + tc

        return sorted(
            ({"predicate": k, "cnt": v} for k, v in counts.items()),
            key=lambda d: d["cnt"],
            reverse=True,
        )

    # ======================================================================
    #  Entity lookup
    # ======================================================================

    def find_subjects_by_type(
        self,
        table_name: str,
        type_uri: str,
        limit: int = 50,
        offset: int = 0,
        search: Optional[str] = None,
    ) -> List[str]:
        label = sanitise_label(table_name)
        class_label = self._class_label_for_uri(label, type_uri)
        if class_label is None:
            return []
        if search:
            cypher = (
                f"MATCH (n:`{label}`:`{class_label}`) "
                f"WHERE any(k IN keys(n) WHERE k <> 'uri' AND toLower(toString(n[k])) CONTAINS toLower($search)) "
                f"RETURN n.uri AS subject ORDER BY subject SKIP $offset LIMIT $limit"
            )
            rows = self._conn.run(
                cypher, search=search, offset=int(offset), limit=int(limit)
            )
        else:
            cypher = (
                f"MATCH (n:`{label}`:`{class_label}`) "
                f"RETURN n.uri AS subject ORDER BY subject SKIP $offset LIMIT $limit"
            )
            rows = self._conn.run(cypher, offset=int(offset), limit=int(limit))
        return [r["subject"] for r in (rows or [])]

    def resolve_subject_by_id(
        self, table_name: str, type_uri: str, id_fragment: str
    ) -> Optional[str]:
        label = sanitise_label(table_name)
        class_label = self._class_label_for_uri(label, type_uri)
        if class_label is None:
            return None
        rows = self._conn.run(
            f"MATCH (n:`{label}`:`{class_label}`) "
            f"WHERE n.uri ENDS WITH ('/' + $idf) OR n.uri ENDS WITH ('#' + $idf) "
            f"RETURN n.uri AS subject LIMIT 1",
            idf=id_fragment,
        )
        return rows[0]["subject"] if rows else None

    def get_entity_metadata(
        self, table_name: str, subjects: List[str]
    ) -> List[Dict[str, str]]:
        if not subjects:
            return []
        label = sanitise_label(table_name)
        label_map = self._schema.load(label)["label_map"]
        rows = self._conn.run(
            f"MATCH (n:`{label}`) WHERE n.uri IN $subjects "
            f"RETURN n.uri AS uri, "
            f"  [x IN labels(n) WHERE x <> $marker AND x <> $schema] AS classes, "
            f"  n.name AS name",
            subjects=subjects,
            marker=label,
            schema=_SCHEMA_LABEL,
        )
        out: List[Dict[str, str]] = []
        for r in rows or []:
            classes = r.get("classes") or []
            if not classes:
                continue  # parity with SQL backend: only typed subjects returned
            first = classes[0]
            out.append(
                {
                    "uri": r["uri"],
                    "type": label_map.get(first, first),
                    "label": r.get("name") or "",
                }
            )
        return out

    def get_triples_for_subjects(
        self, table_name: str, subjects: List[str]
    ) -> List[Dict[str, str]]:
        if not subjects:
            return []
        return self._reconstruct_triples_for_nodes(
            sanitise_label(table_name), node_uris=subjects
        )

    def get_predicates_for_type(self, table_name: str, type_uri: str) -> List[str]:
        label = sanitise_label(table_name)
        class_label = self._class_label_for_uri(label, type_uri)
        if class_label is None:
            return []
        maps = self._schema.load(label)
        prop_map, reltype_map = maps["prop_map"], maps["reltype_map"]
        # Sample one instance of the class; return its predicates (props + rels).
        rows = self._conn.run(
            f"MATCH (n:`{label}`:`{class_label}`) "
            f"WITH n LIMIT 1 "
            f"OPTIONAL MATCH (n)-[rel]->(:`{label}`) "
            f"RETURN [k IN keys(n) WHERE k <> 'uri'] AS keys, "
            f"  collect(DISTINCT type(rel)) AS reltypes, "
            f"  size([x IN labels(n) WHERE x <> $marker AND x <> $schema]) AS n_classes",
            marker=label,
            schema=_SCHEMA_LABEL,
        )
        if not rows:
            return []
        row = rows[0]
        preds: List[str] = []
        if int(row.get("n_classes") or 0) > 0:
            preds.append(RDF_TYPE)
        for k in row.get("keys") or []:
            preds.append(Neo4jSchemaMap.predicate_for_property(prop_map, k))
        for rt in row.get("reltypes") or []:
            if rt:
                preds.append(reltype_map.get(rt, rt))
        # de-dup, preserve order
        seen: Set[str] = set()
        return [p for p in preds if not (p in seen or seen.add(p))]

    # ======================================================================
    #  Pagination
    # ======================================================================

    def paginated_triples(
        self, table_name: str, conditions: List[str], limit: int, offset: int
    ) -> List[Dict[str, str]]:
        # SQL WHERE fragments are not translated (parity with prior behaviour);
        # paginate over nodes and reconstruct their triples.
        label = sanitise_label(table_name)
        if conditions:
            logger.warning(
                "paginated_triples received %d SQL conditions; Neo4j backend "
                "ignores them. Use find_subjects_by_type / find_seed_subjects.",
                len(conditions),
            )
        page = self._conn.run(
            f"MATCH (n:`{label}`) RETURN n.uri AS uri ORDER BY n.uri "
            f"SKIP $offset LIMIT $limit",
            offset=int(offset),
            limit=int(limit),
        )
        uris = [r["uri"] for r in (page or [])]
        if not uris:
            return []
        return self._reconstruct_triples_for_nodes(label, node_uris=uris)

    def paginated_count(self, table_name: str, conditions: List[str]) -> int:
        if conditions:
            logger.warning(
                "paginated_count received %d SQL conditions; Neo4j backend "
                "returns the unfiltered count.",
                len(conditions),
            )
        return self.count_triples(table_name)

    # ======================================================================
    #  Knowledge-Graph filter primitives — native relationship traversal
    # ======================================================================

    def bfs_traversal(
        self,
        table_name: str,
        seed_where: str,
        depth: int,
        search: str = "",
        entity_type: str = "",
    ) -> List[Dict[str, Any]]:
        if not search and not entity_type:
            if seed_where:
                logger.warning(
                    "bfs_traversal: Neo4j backend requires structured "
                    "search/entity_type params; SQL seed_where is not translated."
                )
            return []
        seeds = self.find_seed_subjects(
            table_name,
            entity_type=entity_type,
            field="any",
            match_type="contains",
            value=search,
        )
        if not seeds:
            return []
        label = sanitise_label(table_name)
        # Native variable-length traversal over real relationships, both directions.
        rows = self._conn.run(
            f"MATCH (seed:`{label}`) WHERE seed.uri IN $seeds "
            f"MATCH path = (seed)-[*0..{int(depth)}]-(reached:`{label}`) "
            f"WITH reached.uri AS entity, min(length(path)) AS lvl "
            f"RETURN entity, lvl AS min_lvl",
            seeds=list(seeds),
        )
        return rows or []

    def find_seed_subjects(
        self,
        table_name: str,
        entity_type: str = "",
        field: str = "any",
        match_type: str = "contains",
        value: str = "",
        limit: int = 0,
    ) -> Set[str]:
        label = sanitise_label(table_name)
        search_label = field in ("label", "any")
        search_id = field in ("id", "any")

        def _clause(expr: str) -> str:
            if match_type == "exact":
                return f"toLower({expr}) = $val"
            if match_type == "starts":
                return f"toLower({expr}) STARTS WITH $val"
            if match_type == "ends":
                return f"toLower({expr}) ENDS WITH $val"
            return f"toLower({expr}) CONTAINS $val"

        params: Dict[str, Any] = {}
        if value:
            params["val"] = value.lower()

        class_label: Optional[str] = None
        if entity_type:
            class_label = self._class_label_for_uri(label, entity_type)
            if class_label is None:
                return set()

        node_match = f"(n:`{label}`:`{class_label}`)" if class_label else f"(n:`{label}`)"

        if value:
            conds = []
            if search_label:
                conds.append(_clause("coalesce(n.name, '')"))
            if search_id:
                conds.append(_clause("n.uri"))
            if not conds:
                return set()
            cypher = (
                f"MATCH {node_match} WHERE {' OR '.join(conds)} "
                f"RETURN DISTINCT n.uri AS subject"
            )
        elif entity_type:
            cypher = f"MATCH {node_match} RETURN DISTINCT n.uri AS subject"
        else:
            return set()

        if limit and limit > 0:
            cypher += f" LIMIT {int(limit)}"
        rows = self._conn.run(cypher, **params) or []
        return {r["subject"] for r in rows}

    def find_subjects_by_patterns(
        self, table_name: str, like_patterns: List[str]
    ) -> Set[str]:
        if not like_patterns:
            return set()
        label = sanitise_label(table_name)
        clauses: List[str] = []
        params: Dict[str, Any] = {}
        for i, raw in enumerate(like_patterns):
            pkey = f"p{i}"
            params[pkey] = raw.replace("%", ".*")
            clauses.append(f"n.uri =~ ${pkey}")
        rows = self._conn.run(
            f"MATCH (n:`{label}`) WHERE {' OR '.join(clauses)} "
            f"RETURN DISTINCT n.uri AS subject",
            **params,
        )
        return {r["subject"] for r in (rows or [])}

    def expand_entity_neighbors(
        self, table_name: str, entity_uris: Set[str]
    ) -> Set[str]:
        if not entity_uris:
            return set()
        label = sanitise_label(table_name)
        # One hop over real relationships (both directions); only typed nodes
        # (carrying a class label) count as entity neighbours.
        rows = self._conn.run(
            f"MATCH (n:`{label}`)-[]-(m:`{label}`) "
            f"WHERE n.uri IN $seeds "
            f"AND any(x IN labels(m) WHERE x <> $marker AND x <> $schema) "
            f"RETURN DISTINCT m.uri AS entity",
            seeds=list(entity_uris),
            marker=label,
            schema=_SCHEMA_LABEL,
        )
        return {r["entity"] for r in (rows or [])}

    # ======================================================================
    #  Reasoning — native relationship patterns
    # ======================================================================

    def transitive_closure(
        self,
        table_name: str,
        predicate_uri: str,
        start_uri: Optional[str] = None,
        max_depth: int = 20,
    ) -> List[Dict[str, Any]]:
        label = sanitise_label(table_name)
        reltype = self._reltype_for_uri(label, predicate_uri)
        if reltype is None:
            return []
        start_clause = "WHERE a.uri = $start_uri " if start_uri else ""
        params: Dict[str, Any] = {}
        if start_uri:
            params["start_uri"] = start_uri
        # Reachable pairs 2..max_depth along the typed relationship that are not
        # already directly connected.
        cypher = (
            f"MATCH (a:`{label}`) {start_clause}"
            f"MATCH (a)-[:`{reltype}`*2..{int(max_depth)}]->(b:`{label}`) "
            f"WHERE NOT (a)-[:`{reltype}`]->(b) AND a <> b "
            f"RETURN DISTINCT a.uri AS subject, $pred AS predicate, b.uri AS object"
        )
        params["pred"] = predicate_uri
        try:
            return self._conn.run(cypher, **params) or []
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "transitive_closure failed on %s (predicate=%s): %s",
                table_name, predicate_uri, exc,
            )
            return []

    def symmetric_expand(
        self, table_name: str, predicate_uri: str
    ) -> List[Dict[str, Any]]:
        label = sanitise_label(table_name)
        reltype = self._reltype_for_uri(label, predicate_uri)
        if reltype is None:
            return []
        cypher = (
            f"MATCH (a:`{label}`)-[:`{reltype}`]->(b:`{label}`) "
            f"WHERE NOT (b)-[:`{reltype}`]->(a) "
            f"RETURN DISTINCT b.uri AS subject, $pred AS predicate, a.uri AS object"
        )
        try:
            return self._conn.run(cypher, pred=predicate_uri) or []
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "symmetric_expand failed on %s (predicate=%s): %s",
                table_name, predicate_uri, exc,
            )
            return []

    def shortest_path(
        self,
        table_name: str,
        source_uri: str,
        target_uri: str,
        max_depth: int = 10,
    ) -> List[Dict[str, Any]]:
        if source_uri == target_uri:
            return [{"hop": 0, "uri": source_uri}]
        label = sanitise_label(table_name)
        # Native shortestPath over real relationships — the typed-model win.
        cypher = (
            f"MATCH (s:`{label}` {{uri: $src}}), (t:`{label}` {{uri: $tgt}}), "
            f"p = shortestPath((s)-[*..{int(max_depth)}]-(t)) "
            f"RETURN [x IN nodes(p) | x.uri] AS uris"
        )
        try:
            rows = self._conn.run(cypher, src=source_uri, tgt=target_uri)
        except Exception as exc:  # noqa: BLE001
            logger.warning("shortest_path failed on %s: %s", table_name, exc)
            return []
        if not rows or not rows[0].get("uris"):
            return []
        return [{"hop": i, "uri": u} for i, u in enumerate(rows[0]["uris"])]

    # ------------------------------------------------------------------
    #  Admin: graph inventory + server databases (Settings UI, P5)
    # ------------------------------------------------------------------

    def list_labels(self) -> List[Dict[str, Any]]:
        """List every materialised graph (marker label) with node/edge counts.

        A "graph" is a node label backed by a ``node_<label>_uri`` uniqueness
        constraint (created by :meth:`Neo4jWriteOps.create_table`). Returns one
        row per graph: ``{label, nodes, edges}``. Powers the Settings → Neo4j
        → Objects admin tab (list + delete), parity with Lakebase objects.
        """
        constraint_rows = self._conn.run(
            "SHOW CONSTRAINTS YIELD name, labelsOrTypes "
            "WHERE name STARTS WITH 'node_' AND name ENDS WITH '_uri' "
            "RETURN labelsOrTypes AS labels"
        )
        labels: List[str] = []
        for r in constraint_rows or []:
            for lbl in r.get("labels") or []:
                if lbl and lbl != _SCHEMA_LABEL:
                    labels.append(lbl)

        out: List[Dict[str, Any]] = []
        for label in sorted(set(labels)):
            node_rows = self._conn.run(
                f"MATCH (n:`{label}`) RETURN count(n) AS nodes"
            )
            edge_rows = self._conn.run(
                f"MATCH (:`{label}`)-[r]->(:`{label}`) RETURN count(r) AS edges"
            )
            out.append(
                {
                    "label": label,
                    "nodes": int(node_rows[0]["nodes"]) if node_rows else 0,
                    "edges": int(edge_rows[0]["edges"]) if edge_rows else 0,
                }
            )
        return out

    def list_databases(self) -> List[str]:
        """List Neo4j databases available on the server (for the DB selector, P4).

        Uses ``SHOW DATABASES``; filters out system/internal databases. On
        instances that disallow the call (e.g. some Aura tiers), returns an
        empty list so the caller falls back to the configured database name.
        """
        try:
            rows = self._conn.run("SHOW DATABASES YIELD name RETURN DISTINCT name")
        except Exception as exc:  # noqa: BLE001
            logger.info("SHOW DATABASES unavailable (%s); DB selector will fallback", exc)
            return []
        names = [r["name"] for r in (rows or []) if r.get("name")]
        return sorted(n for n in names if n not in ("system",))

    # ------------------------------------------------------------------
    #  Reverse-map helpers
    # ------------------------------------------------------------------

    def _class_label_for_uri(self, graph_label: str, class_uri: str) -> Optional[str]:
        """Sanitised Neo4j label for a class URI (via schema map, else derive)."""
        label_map = self._schema.load(graph_label)["label_map"]
        for sanitised, uri in label_map.items():
            if uri == class_uri:
                return sanitised
        # Fallback: derive from the URI's local name (schema map may lag).
        from back.core.graphdb.neo4j.graph_model import label_from_class_uri

        return label_from_class_uri(class_uri)

    def _reltype_for_uri(self, graph_label: str, predicate_uri: str) -> Optional[str]:
        reltype_map = self._schema.load(graph_label)["reltype_map"]
        for sanitised, uri in reltype_map.items():
            if uri == predicate_uri:
                return sanitised
        from back.core.graphdb.neo4j.graph_model import reltype_from_predicate

        return reltype_from_predicate(predicate_uri)
