"""Neo4j write/CRUD operations on the **typed property-graph** model.

Every subject/object URI is a node (MERGE-keyed on its full URI — the URI is a
globally unique id, confirmed with Benoit 2026-07-28), ``rdf:type`` becomes a
Neo4j label, ``rdfs:label`` becomes the ``name`` property, literal-object
predicates become node properties, and URI-object predicates become real
relationships ``(s)-[:reltype]->(o)``.

Every node also carries a reserved **marker label** — the sanitised graph/table
name — so a whole domain graph is one ``MATCH (n:<graph_label>)`` away for
counting, dropping, and isolation. The forward transform is pure
(:mod:`back.core.graphdb.neo4j.graph_model`); the reverse maps needed to reconstruct exact SPO
triples on read are persisted by :class:`Neo4jSchemaMap`.

Public method signatures are unchanged from the flat-triple version so the
``GraphDBBackend`` contract and all callers keep working.
"""

from typing import Any, Callable, Dict, List, Optional

from back.core.graphdb.neo4j.Neo4jConnection import Neo4jConnection
from back.core.graphdb.neo4j.graph_model import plan_writes
from back.core.graphdb.neo4j.Neo4jSchemaMap import Neo4jSchemaMap
from back.core.logging import get_logger

logger = get_logger(__name__)


def sanitise_label(table_name: str) -> str:
    """Neo4j labels are case-sensitive identifiers; sanitise to ``[A-Za-z0-9_]``.

    Also used for the per-graph **marker label** applied to every node of a
    graph. Retained (and re-exported) for :class:`Neo4jReadOps` /
    :class:`Neo4jStore`, which key their queries on the same marker.
    """
    cleaned = "".join(c if c.isalnum() or c == "_" else "_" for c in table_name)
    if cleaned and cleaned[0].isdigit():
        cleaned = "_" + cleaned
    return cleaned or "_"


class Neo4jWriteOps:
    """Schema + CRUD writes for the typed property-graph Neo4j backend."""

    def __init__(self, connection: Neo4jConnection) -> None:
        self._conn = connection
        self._schema = Neo4jSchemaMap(connection)

    # ----------------------------------------------------------------------
    #  Schema (constraint lifecycle)
    # ----------------------------------------------------------------------

    def create_table(self, table_name: str) -> None:
        """Create a uniqueness constraint on node identity for this graph.

        One constraint per graph marker label enforces ``uri`` uniqueness and
        creates the backing index (fast MERGE + lookup).
        """
        label = sanitise_label(table_name)
        self._conn.run(
            f"CREATE CONSTRAINT node_{label}_uri IF NOT EXISTS "
            f"FOR (n:`{label}`) REQUIRE n.uri IS UNIQUE"
        )
        logger.info("Created Neo4j graph marker label + uri constraint: %s", label)

    def drop_table(self, table_name: str) -> None:
        """Drop the graph: all nodes of the marker label (+ their relationships),
        the uniqueness constraint, and the persisted schema map."""
        label = sanitise_label(table_name)
        self._conn.run(f"DROP CONSTRAINT node_{label}_uri IF EXISTS")
        self._conn.run(f"MATCH (n:`{label}`) DETACH DELETE n")
        self._schema.drop(label)
        logger.info("Dropped Neo4j graph: %s", label)

    def optimize_table(self, table_name: str) -> None:
        # Neo4j indexes online; no manual VACUUM/OPTIMIZE.
        return None

    # ----------------------------------------------------------------------
    #  Bulk writes
    # ----------------------------------------------------------------------

    def insert_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 2000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Write triples as a property graph. Returns the triple count consumed.

        Per batch, three phases keep the graph consistent regardless of triple
        ordering (a relationship may reference a node whose own triples arrive
        later): (1) MERGE all nodes + set labels/properties, (2) MERGE all
        relationships. The reverse schema maps are merged + persisted once.
        """
        if not triples:
            return 0
        label = sanitise_label(table_name)
        total = 0
        agg_label_map: Dict[str, str] = {}
        agg_reltype_map: Dict[str, str] = {}
        agg_prop_map: Dict[str, str] = {}

        for i in range(0, len(triples), batch_size):
            batch = triples[i : i + batch_size]
            planned = plan_writes(batch)

            # Phase 1: nodes (MERGE on uri, apply marker + class labels, set props)
            node_rows = [
                {
                    "uri": n.uri,
                    "labels": list(n.labels),
                    "props": n.properties,
                }
                for n in planned.nodes
            ]
            if node_rows:
                self._merge_nodes(label, node_rows)

            # Phase 2: relationships (batched — one UNWIND per reltype)
            if planned.edges:
                self._merge_edges(label, planned.edges)

            agg_label_map.update(planned.label_map)
            agg_reltype_map.update(planned.reltype_map)
            agg_prop_map.update(planned.prop_map)
            total += len(batch)
            if on_progress:
                on_progress(total, len(triples))

        # Persist reverse maps once (idempotent merge).
        self._schema.merge_and_save(
            label, agg_label_map, agg_reltype_map, agg_prop_map
        )
        logger.info("Inserted %d triples as property graph into %s", total, label)
        return total

    def _merge_nodes(self, label: str, node_rows: List[Dict[str, Any]]) -> None:
        """MERGE nodes + set their class labels without requiring APOC.

        Neo4j cannot parameterise labels in ``SET n:$x``, and APOC may be
        unavailable on Aura free tier, so we group rows by their label set and
        emit one statement per distinct label combination with the labels
        inlined (sanitised → injection-safe).
        """
        from collections import defaultdict

        by_labels: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
        for r in node_rows:
            by_labels[tuple(r["labels"])].append(
                {"uri": r["uri"], "props": r["props"]}
            )

        for labels, rows in by_labels.items():
            set_labels = "".join(f":`{lbl}`" for lbl in labels)  # sanitised upstream
            label_clause = f"SET n{set_labels} " if set_labels else ""
            self._conn.run(
                f"UNWIND $rows AS r "
                f"MERGE (n:`{label}` {{uri: r.uri}}) "
                f"SET n += r.props "
                f"{label_clause}",
                rows=rows,
            )

    def _merge_edges(self, label: str, edges: List[Any]) -> None:
        """MERGE relationships in bulk — one ``UNWIND`` statement per reltype.

        Relationship types cannot be parameterised in Cypher, so we group edges
        by their (sanitised, injection-safe) reltype and emit one batched
        statement each, turning phase 2 from O(edges) round-trips into
        O(distinct reltypes). Endpoints are matched by ``uri`` (both were
        MERGE-created in phase 1).
        """
        from collections import defaultdict

        by_reltype: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        for e in edges:
            by_reltype[e.reltype].append({"s": e.subject, "o": e.object})

        for reltype, rows in by_reltype.items():
            self._conn.run(
                f"UNWIND $rows AS r "
                f"MATCH (s:`{label}` {{uri: r.s}}), (o:`{label}` {{uri: r.o}}) "
                f"MERGE (s)-[:`{reltype}`]->(o)",
                rows=rows,
            )

    def delete_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 2000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Remove triples from the property graph. Returns the count consumed.

        Inverse of insert: for each triple, remove the matching relationship
        (URI object), unset the property (literal object), or remove the class
        label (rdf:type). Nodes that end up with only the marker label and no
        other class label are deleted (they no longer represent an entity).
        """
        if not triples:
            return 0
        label = sanitise_label(table_name)
        from back.core.graphdb.neo4j.graph_model import (
            is_uri,
            reltype_from_predicate,
            label_from_class_uri,
            _local_name,
            _sanitise_ident,
        )
        from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL

        deleted = 0
        for i in range(0, len(triples), batch_size):
            batch = triples[i : i + batch_size]
            for t in batch:
                s, p, o = t.get("subject", ""), t.get("predicate", ""), t.get("object", "")
                if not s or not p:
                    continue
                if p == RDF_TYPE:
                    lbl = label_from_class_uri(o)
                    self._conn.run(
                        f"MATCH (n:`{label}` {{uri: $s}}) REMOVE n:`{lbl}`", s=s
                    )
                elif p == RDFS_LABEL:
                    self._conn.run(
                        f"MATCH (n:`{label}` {{uri: $s}}) REMOVE n.name", s=s
                    )
                elif is_uri(o):
                    rel = reltype_from_predicate(p)
                    self._conn.run(
                        f"MATCH (:`{label}` {{uri: $s}})-[r:`{rel}`]->(:`{label}` {{uri: $o}}) "
                        f"DELETE r",
                        s=s,
                        o=o,
                    )
                else:
                    key = _sanitise_ident(_local_name(p))
                    self._conn.run(
                        f"MATCH (n:`{label}` {{uri: $s}}) REMOVE n.`{key}`", s=s
                    )
            deleted += len(batch)
            if on_progress:
                on_progress(deleted, len(triples))

        # Sweep nodes that lost all entity meaning (marker-only, no rels, no name).
        self._conn.run(
            f"MATCH (n:`{label}`) WHERE size(labels(n)) = 1 "
            f"AND NOT (n)--() AND n.name IS NULL DETACH DELETE n"
        )
        logger.info("Deleted %d triples from Neo4j graph %s", deleted, label)
        return deleted

    def delete_cohort_triples(
        self,
        table_name: str,
        cohort_uri_prefix: str,
        in_cohort_predicate: str,
    ) -> int:
        """Remove a cohort's nodes + membership edges (idempotent).

        Cohort-entity nodes have a ``uri`` starting with *cohort_uri_prefix*;
        membership edges are relationships whose type derives from
        *in_cohort_predicate* pointing at such nodes. DETACH-deleting the cohort
        nodes removes both in one step.
        """
        if not cohort_uri_prefix:
            return 0
        label = sanitise_label(table_name)
        try:
            rows = self._conn.run(
                f"MATCH (n:`{label}`) WHERE n.uri STARTS WITH $prefix "
                f"WITH n LIMIT 100000 DETACH DELETE n RETURN count(n) AS deleted",
                prefix=cohort_uri_prefix,
            )
            return int(rows[0].get("deleted", 0)) if rows else 0
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "delete_cohort_triples failed on %s (%s): %s",
                table_name,
                cohort_uri_prefix,
                exc,
            )
            return 0
