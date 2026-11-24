"""Journal of user edits for the Neo4j backend.

Neo4j has no mapped-data layer to anti-join against: a Build rewrites the graph
from the mapping, so a user's edit would be lost. The journal keeps each edit as
a ``:__Override`` node that the Build never touches, and :meth:`Store.replay_overrides`
re-applies it once the mapped triples are loaded.

One node per ``(graph, triple)`` holds the **latest** operation on that triple
(``add`` or ``del``), which keeps the contract of ``back.core.graphdb.overrides``
(a triple is never both added and hidden). Journal nodes carry no graph marker
label, so graph counts, drops and queries never see them.
"""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List

from back.core.graphdb.neo4j.Connection import Connection
from back.core.graphdb.overrides import OP_ADD, OP_DEL, ORIGIN_USER, validate_op
from back.core.logging import get_logger

logger = get_logger(__name__)

JOURNAL_LABEL = "__Override"


def _entry_id(graph: str, triple: Dict[str, str]) -> str:
    raw = "\x1f".join(
        (
            graph,
            triple.get("subject", "") or "",
            triple.get("predicate", "") or "",
            triple.get("object", "") or "",
        )
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()  # noqa: S324 - not security


class OverrideJournal:
    """Persistent record of the user's ``add`` / ``del`` edits for one Neo4j database."""

    def __init__(self, connection: Connection) -> None:
        self._conn = connection
        self._ready = False

    def _ensure(self) -> None:
        if self._ready:
            return
        self._conn.run(
            f"CREATE CONSTRAINT override_journal_id IF NOT EXISTS "
            f"FOR (j:`{JOURNAL_LABEL}`) REQUIRE j.id IS UNIQUE"
        )
        self._ready = True

    def record(
        self,
        graph: str,
        triples: List[Dict[str, str]],
        op: str,
        *,
        author: str = "",
        change_id: str = "",
    ) -> int:
        """Journal *op* for each triple, replacing any earlier operation on it."""
        validate_op(op)
        if not triples:
            return 0
        self._ensure()
        rows = [
            {
                "id": _entry_id(graph, t),
                "subject": t.get("subject", "") or "",
                "predicate": t.get("predicate", "") or "",
                "object": t.get("object", "") or "",
            }
            for t in triples
        ]
        self._conn.run(
            f"UNWIND $rows AS r "
            f"MERGE (j:`{JOURNAL_LABEL}` {{id: r.id}}) "
            f"SET j.graph = $graph, j.op = $op, j.origin = $origin, "
            f"j.subject = r.subject, j.predicate = r.predicate, j.object = r.object, "
            f"j.change_id = $change_id, j.author = $author, j.created_at = datetime()",
            rows=rows,
            graph=graph,
            op=op,
            origin=ORIGIN_USER,
            change_id=change_id or None,
            author=author or None,
        )
        return len(rows)

    def entries(self, graph: str) -> List[Dict[str, Any]]:
        """Every journaled edit of *graph*, oldest first."""
        self._ensure()
        return self._conn.run(
            f"MATCH (j:`{JOURNAL_LABEL}` {{graph: $graph}}) "
            f"RETURN j.op AS op, j.subject AS subject, j.predicate AS predicate, "
            f"j.object AS object ORDER BY j.created_at",
            graph=graph,
        )

    def counts(self, graph: str) -> Dict[str, int]:
        """``{"add": n, "del": m}`` journaled edits of *graph*."""
        self._ensure()
        rows = self._conn.run(
            f"MATCH (j:`{JOURNAL_LABEL}` {{graph: $graph}}) "
            f"RETURN j.op AS op, count(j) AS cnt",
            graph=graph,
        )
        counts = {OP_ADD: 0, OP_DEL: 0}
        for row in rows:
            counts[row.get("op")] = int(row.get("cnt") or 0)
        return counts

    def drop(self, graph: str) -> None:
        """Forget every journaled edit of *graph* (the graph itself is being deleted)."""
        self._ensure()
        self._conn.run(
            f"MATCH (j:`{JOURNAL_LABEL}` {{graph: $graph}}) DELETE j", graph=graph
        )
