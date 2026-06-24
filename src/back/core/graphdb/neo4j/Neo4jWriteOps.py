"""Neo4j write/CRUD operations on the flat-triple model.

Owns schema management (constraint create/drop), bulk write paths
(``UNWIND`` + ``MERGE`` for inserts, ``UNWIND`` + ``MATCH`` + ``DETACH
DELETE`` for deletes), and cohort wipes. Receives a
:class:`Neo4jConnection` (composition) so all queries go through the
shared logging / auth layer.

Schema convention: one Cypher label per logical store (sanitised from
the table name). Multi-label compound patterns (e.g. ``:Triple:<store>``)
are deliberately avoided because Neo4j 5+ rejects them in
``CREATE CONSTRAINT``.
"""

from typing import Any, Callable, Dict, List, Optional

from back.core.graphdb.neo4j.Neo4jConnection import Neo4jConnection
from back.core.logging import get_logger

logger = get_logger(__name__)


def sanitise_label(table_name: str) -> str:
    """Neo4j labels are case-sensitive identifiers; sanitise to ``[A-Za-z0-9_]``."""
    return "".join(c if c.isalnum() or c == "_" else "_" for c in table_name)


class Neo4jWriteOps:
    """Schema + CRUD writes for the flat-triple Neo4j backend."""

    def __init__(self, connection: Neo4jConnection) -> None:
        self._conn = connection

    # ----------------------------------------------------------------------
    #  Schema (constraint lifecycle)
    # ----------------------------------------------------------------------

    def create_table(self, table_name: str) -> None:
        label = sanitise_label(table_name)
        cypher = (
            f"CREATE CONSTRAINT triple_{label}_spo IF NOT EXISTS "
            f"FOR (t:`{label}`) "
            f"REQUIRE (t.subject, t.predicate, t.object) IS UNIQUE"
        )
        self._conn.run(cypher)
        logger.info("Created Neo4j triple label: %s", label)

    def drop_table(self, table_name: str) -> None:
        label = sanitise_label(table_name)
        self._conn.run(f"DROP CONSTRAINT triple_{label}_spo IF EXISTS")
        self._conn.run(f"MATCH (t:`{label}`) DETACH DELETE t")
        logger.info("Dropped Neo4j triple label: %s", label)

    def optimize_table(self, table_name: str) -> None:
        # Neo4j has no manual VACUUM/OPTIMIZE; the indexer runs online.
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
        if not triples:
            return 0
        label = sanitise_label(table_name)
        total = 0
        cypher = (
            f"UNWIND $rows AS r "
            f"MERGE (t:`{label}` {{subject: r.subject, predicate: r.predicate, object: r.object}})"
        )
        for i in range(0, len(triples), batch_size):
            batch = triples[i : i + batch_size]
            rows = [
                {
                    "subject": t.get("subject", ""),
                    "predicate": t.get("predicate", ""),
                    "object": t.get("object", ""),
                }
                for t in batch
            ]
            self._conn.run(cypher, rows=rows)
            total += len(batch)
            if on_progress:
                on_progress(total, len(triples))
        logger.info("Inserted %d triples into Neo4j label %s", total, label)
        return total

    def delete_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 2000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        if not triples:
            return 0
        label = sanitise_label(table_name)
        deleted = 0
        cypher = (
            f"UNWIND $rows AS r "
            f"MATCH (t:`{label}` {{subject: r.subject, predicate: r.predicate, object: r.object}}) "
            f"DETACH DELETE t"
        )
        for i in range(0, len(triples), batch_size):
            batch = triples[i : i + batch_size]
            rows = [
                {
                    "subject": t.get("subject", ""),
                    "predicate": t.get("predicate", ""),
                    "object": t.get("object", ""),
                }
                for t in batch
            ]
            self._conn.run(cypher, rows=rows)
            deleted += len(batch)
            if on_progress:
                on_progress(deleted, len(triples))
        logger.info("Deleted %d triples from Neo4j label %s", deleted, label)
        return deleted

    def delete_cohort_triples(
        self,
        table_name: str,
        cohort_uri_prefix: str,
        in_cohort_predicate: str,
    ) -> int:
        if not cohort_uri_prefix:
            return 0
        label = sanitise_label(table_name)
        cypher = (
            f"MATCH (t:`{label}`) "
            f"WHERE t.subject STARTS WITH $prefix "
            f"   OR (t.predicate = $in_pred AND t.object STARTS WITH $prefix) "
            f"WITH t LIMIT 100000 "
            f"DETACH DELETE t "
            f"RETURN count(t) AS deleted"
        )
        try:
            rows = self._conn.run(
                cypher, prefix=cohort_uri_prefix, in_pred=in_cohort_predicate
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
