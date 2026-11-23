"""Single write path into the Certified Graph (enrichments, rules, cohorts, communities).

Every modification is written to the Delta composition (``…_cert_enriched``,
which ``…_certified`` unions in), then to the served backend when it is a
copy (Lakebase, Neo4j). A Lakehouse Certified Graph reads the composition, so
the first write is enough. Writes are refused on a read-only (IN-REVIEW or
PUBLISHED) version.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List

from back.core.errors import InfrastructureError
from back.core.graphdb.constants import GRAPH_VARIANT_CERTIFIED
from back.core.graphdb.GraphDBFactory import GraphDBFactory
from back.core.helpers.SQLHelpers import SQLHelpers
from back.core.logging import get_logger
from back.objects.digitaltwin.certify.CertifiedBuild import CertifiedBuild

logger = get_logger(__name__)


class CertifiedGraphWriter:
    """Write enrichment triples into the Certified Graph of a domain version."""

    KIND_INFERENCE = "inference"
    KIND_RULE = "rule"
    KIND_COHORT = "cohort"
    KIND_COMMUNITY = "community"

    def __init__(self, domain: Any, settings: Any) -> None:
        self.domain = domain
        self.settings = settings

    def add_enrichments(
        self, triples: Iterable[Dict[str, str]], *, kind: str, run_id: str = ""
    ) -> int:
        """Add *triples* (each may carry ``provenance``); return how many were written."""
        CertifiedBuild.ensure_editable(self.domain, self.settings)
        rows = self._rows(triples)
        if not rows:
            return 0
        builder = GraphDBFactory.get_builder(
            self.domain, self.settings, engine="delta"
        )
        builder.ensure_cert_enriched_table()
        builder.insert_cert_enriched(rows, kind=kind, run_id=run_id)
        if GraphDBFactory._resolve_graph_backend(self.domain) != "databricks":
            self._insert_into_backend(rows)
        logger.info(
            "Certified Graph: wrote %d %s triple(s) (run %s)", len(rows), kind, run_id
        )
        return len(rows)

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

    def _insert_into_backend(self, rows: List[Dict[str, str]]) -> None:
        store = GraphDBFactory.get_graphdb(
            self.domain, self.settings, for_write=True, variant=GRAPH_VARIANT_CERTIFIED
        )
        if store is None:
            raise InfrastructureError("Could not open the Certified Graph backend")
        store.insert_triples(
            SQLHelpers.effective_graph_name(self.domain),
            [{k: r[k] for k in ("subject", "predicate", "object")} for r in rows],
        )
