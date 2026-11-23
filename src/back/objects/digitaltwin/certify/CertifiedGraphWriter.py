"""Single write path into the Certified Graph (enrichments, rules, cohorts, communities).

Every modification is written to the Delta composition (``…_cert_enriched``,
which ``…_certified`` unions in), then to the served backend when it is a
copy (Lakebase, Neo4j). A Lakehouse Certified Graph reads the composition, so
the first write is enough. Rows are grouped in *scopes* (``kind`` +
``provenance``, e.g. ``cohort`` / ``cohort:<rule>``) so a re-run replaces
exactly its own rows. Writes are refused on a read-only (IN-REVIEW or
PUBLISHED) version. An ``override`` scope replaces the values of its
(subject, predicate): ``…_certified`` hides the base values, and the backend
copy drops them before the new value is inserted.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Sequence

from back.core.errors import InfrastructureError
from back.core.graphdb.constants import CERT_KIND_OVERRIDE, GRAPH_VARIANT_CERTIFIED
from back.core.graphdb.GraphDBFactory import GraphDBFactory
from back.core.helpers.SQLHelpers import SQLHelpers
from back.core.logging import get_logger
from back.objects.digitaltwin.certify.CertifiedBuild import CertifiedBuild

logger = get_logger(__name__)

_SPO = ("subject", "predicate", "object")


class CertifiedGraphWriter:
    """Write enrichment triples into the Certified Graph of a domain version."""

    KIND_INFERENCE = "inference"
    KIND_RULE = "rule"
    KIND_COHORT = "cohort"
    KIND_COMMUNITY = "community"
    KIND_OVERRIDE = CERT_KIND_OVERRIDE

    def __init__(self, domain: Any, settings: Any) -> None:
        self.domain = domain
        self.settings = settings
        self._graph_engine = (
            GraphDBFactory._resolve_graph_backend(domain) != "databricks"
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_enrichments(
        self, triples: Iterable[Dict[str, str]], *, kind: str, run_id: str = ""
    ) -> int:
        """Add *triples* (each may carry ``provenance``); return how many were written."""
        CertifiedBuild.ensure_editable(self.domain, self.settings)
        rows = self._rows(triples)
        if not rows:
            return 0
        builder = self._builder()
        builder.insert_cert_enriched(rows, kind=kind, run_id=run_id)
        if self._graph_engine:
            self._backend_insert(rows)
        self._refresh_cache(builder)
        logger.info(
            "Certified Graph: wrote %d %s triple(s) (run %s)", len(rows), kind, run_id
        )
        return len(rows)

    def replace_scope(
        self,
        triples: Iterable[Dict[str, str]],
        *,
        kind: str,
        provenance: str,
        run_id: str = "",
    ) -> int:
        """Replace every row of one scope with *triples*; return how many were written."""
        CertifiedBuild.ensure_editable(self.domain, self.settings)
        rows = [{**r, "provenance": provenance} for r in self._rows(triples)]
        builder = self._builder()
        self._clear_scope(builder, kind, provenance)
        if rows:
            builder.insert_cert_enriched(rows, kind=kind, run_id=run_id)
            if self._graph_engine:
                if kind == self.KIND_OVERRIDE:
                    self.drop_attribute_values(
                        self._backend(), SQLHelpers.effective_graph_name(self.domain), rows
                    )
                self._backend_insert(rows)
        self._refresh_cache(builder)
        logger.info(
            "Certified Graph: scope %s/%s replaced by %d triple(s)",
            kind,
            provenance,
            len(rows),
        )
        return len(rows)

    def replace_scope_from_select(
        self,
        select_sqls: Sequence[str],
        *,
        kind: str,
        provenance: str,
        run_id: str = "",
    ) -> int:
        """Replace one scope with rows computed on the warehouse; return its size.

        Each SQL must project ``subject, predicate, object``. No row transits
        the app for a Lakehouse Certified Graph; a Lakebase / Neo4j backend is
        fed the new scope in batches.
        """
        CertifiedBuild.ensure_editable(self.domain, self.settings)
        builder = self._builder()
        self._clear_scope(builder, kind, provenance)
        for sql in select_sqls:
            builder.insert_cert_enriched_from_select(
                sql, kind=kind, provenance=provenance, run_id=run_id
            )
        if self._graph_engine:
            for batch in builder.iter_cert_enriched_scope(
                kind=kind, provenance=provenance
            ):
                self._backend_insert(batch)
        self._refresh_cache(builder)
        return builder.count_cert_enriched_scope(kind=kind, provenance=provenance)

    def remove_scope(self, *, kind: str, provenance: str) -> int:
        """Remove every row of one scope; return how many backend rows were dropped."""
        CertifiedBuild.ensure_editable(self.domain, self.settings)
        builder = self._builder()
        removed = self._clear_scope(builder, kind, provenance)
        self._refresh_cache(builder)
        return removed

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _builder(self) -> Any:
        """Delta builder with the composition guaranteed to exist."""
        builder = GraphDBFactory.get_builder(
            self.domain, self.settings, engine="delta"
        )
        builder.ensure_certified_objects()
        return builder

    def _clear_scope(self, builder: Any, kind: str, provenance: str) -> int:
        """Drop a scope from the backend copy (if any), then from the composition."""
        removed = 0
        if self._graph_engine:
            store = self._backend()
            for batch in builder.iter_cert_enriched_scope(
                kind=kind, provenance=provenance
            ):
                store.delete_triples(SQLHelpers.effective_graph_name(self.domain), batch)
                removed += len(batch)
        builder.delete_cert_enriched_scope(kind=kind, provenance=provenance)
        return removed

    @staticmethod
    def _rows(triples: Iterable[Dict[str, str]]) -> List[Dict[str, str]]:
        """Writable rows: real subjects only (no aggregate ``(batch)`` markers)."""
        return [
            {
                "subject": t.get("subject", ""),
                "predicate": t.get("predicate", ""),
                "object": t.get("object", ""),
                "provenance": t.get("provenance", "") or "",
            }
            for t in triples
            if t.get("subject") and t.get("subject") != "(batch)"
        ]

    def _backend(self) -> Any:
        store = GraphDBFactory.get_graphdb(
            self.domain, self.settings, for_write=True, variant=GRAPH_VARIANT_CERTIFIED
        )
        if store is None:
            raise InfrastructureError("Could not open the Certified Graph backend")
        return store

    def _backend_insert(self, rows: List[Dict[str, str]]) -> None:
        self._backend().insert_triples(
            SQLHelpers.effective_graph_name(self.domain),
            [{k: r[k] for k in _SPO} for r in rows],
        )

    def _refresh_cache(self, builder: Any) -> None:
        """Rebuild the Certified Graph's search cache once per operation.

        Lakehouse: its own Delta companions over ``..._certified``; Lakebase:
        the Postgres companions of the copy.
        """
        if not self._graph_engine:
            builder.rebuild_certified_companions()
            return
        store = self._backend()
        if getattr(store, "supports_adjacency", False) is True:
            from back.core.graphdb.search_cache import rebuild_graph_cache_if_enabled

            rebuild_graph_cache_if_enabled(
                store, SQLHelpers.effective_graph_name(self.domain), self.domain
            )

    @staticmethod
    def drop_attribute_values(store: Any, table: str, triples: List[Dict[str, str]]) -> None:
        """Delete the current ``(s, p, *)`` values of the literal triples in *store*.

        Uses the store's pair delete when it has one; otherwise reads the
        current rows with SQL and deletes them exactly (a store without SQL
        keeps its values). Also used on simulation sandboxes.
        """
        pairs = CertifiedGraphWriter._attribute_pairs(triples)
        if not pairs:
            return
        deleter = getattr(store, "delete_subject_predicate", None)
        if callable(deleter):
            deleter(table, pairs)
            return
        execute = getattr(store, "execute_query", None)
        sql_ref = getattr(store, "sql_table_reference", None)
        if not (callable(execute) and callable(sql_ref)):
            return
        current = CertifiedGraphWriter._current_values(execute, sql_ref(table), pairs)
        if current:
            store.delete_triples(table, current)

    @staticmethod
    def _attribute_pairs(triples: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Unique ``(subject, predicate)`` pairs of the literal-object triples."""
        from back.core.helpers import is_uri

        pairs: Dict[tuple, Dict[str, str]] = {}
        for t in triples:
            if not is_uri(t.get("object", "")):
                key = (t["subject"], t["predicate"])
                pairs.setdefault(key, {"subject": key[0], "predicate": key[1]})
        return list(pairs.values())

    @staticmethod
    def _current_values(execute: Any, relation: str, pairs: List[Dict[str, str]]):
        from back.core.helpers import SQL_DIALECT_POSTGRES, sql_literal

        rows: List[Dict[str, str]] = []
        for pair in pairs:
            s = sql_literal(pair["subject"], SQL_DIALECT_POSTGRES)
            p = sql_literal(pair["predicate"], SQL_DIALECT_POSTGRES)
            for row in execute(
                f"SELECT subject, predicate, object FROM {relation} "
                f"WHERE subject = {s} AND predicate = {p}"
            ) or []:
                obj = row.get("object", "")
                if obj not in (None, ""):
                    rows.append(
                        {
                            "subject": str(row.get("subject", "") or ""),
                            "predicate": str(row.get("predicate", "") or ""),
                            "object": str(obj),
                        }
                    )
        return rows
