"""Materialise inferred triples to a Delta export table and/or the Certified Graph.

Extracted from ``api.routers.internal.dtwin.materialize_inferred``. The graph
target is the Certified Graph, written through :class:`CertifiedGraphWriter`
with each triple's provenance.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List

from back.core.errors import NotFoundError, OntoBricksError, ValidationError
from back.core.helpers import is_uri
from back.core.logging import get_logger
from back.core.reasoning import ReasoningService
from back.objects.digitaltwin.models import DomainSnapshot

logger = get_logger(__name__)


class TwinInferredMaterialize:
    """Apply a completed inference task's triples to Delta and/or the graph."""

    @staticmethod
    def uri_triples(raw_triples: list) -> List[dict]:
        """Keep only relationship / class assertions (URI object).

        Used by the batch Delta/graph materialise UI, which only writes
        object-property edges.  Entity business rules that set datatype
        attributes use :meth:`writable_triples` instead.
        """
        return [
            t
            for t in raw_triples
            if is_uri(t.get("subject", ""))
            and is_uri(t.get("predicate", ""))
            and is_uri(t.get("object", ""))
        ]

    @staticmethod
    def writable_triples(raw_triples: list) -> List[dict]:
        """Keep triples writable to the flat store (URI s/p, non-empty object).

        Accepts both URI objects (relationships / class assertions) and
        literal objects (set-attribute data properties).  Empty objects
        (dangling relationship placeholders) are dropped.
        """
        out: List[dict] = []
        for t in raw_triples:
            subject = str(t.get("subject", "") or "")
            predicate = str(t.get("predicate", "") or "")
            obj = t.get("object", "")
            if obj is None:
                continue
            obj_s = str(obj)
            if not (is_uri(subject) and is_uri(predicate) and obj_s != ""):
                continue
            row = dict(t)
            row["subject"] = subject
            row["predicate"] = predicate
            row["object"] = obj_s
            out.append(row)
        return out

    @staticmethod
    def run(
        domain,
        settings,
        *,
        task_id: str,
        do_delta: bool,
        do_graph: bool,
        mat_table: str,
        get_task,
        get_client: Callable,
    ) -> Dict[str, Any]:
        if not task_id:
            raise ValidationError("Missing task_id")
        if not do_delta and not do_graph:
            raise ValidationError("Select at least one materialisation target")

        task = get_task(task_id)
        if not task or not task.result:
            raise NotFoundError("Inference results were not found for this task")

        raw_triples = task.result.get("inferred_triples", [])
        if not raw_triples:
            raise ValidationError("There are no inferred triples to materialise")

        uri_triples = TwinInferredMaterialize.uri_triples(raw_triples)
        domain.ensure_generated_content()
        domain_snap = DomainSnapshot(domain)
        result: Dict[str, Any] = {}

        if do_delta and mat_table and len(mat_table.split(".")) == 3 and uri_triples:
            try:
                client = get_client(domain_snap, settings)
                if client is None:
                    result["materialize_error"] = "Databricks credentials not configured"
                else:
                    count = ReasoningService.materialize_to_delta(
                        client, mat_table, uri_triples
                    )
                    result["materialize_count"] = count
                    result["materialize_table"] = mat_table
            except Exception as exc:
                logger.exception("Materialise to Delta failed: %s", exc)
                result["materialize_error"] = "Materialise to Delta failed"
                result["materialize_table"] = mat_table

        if do_graph and uri_triples:
            from back.objects.digitaltwin.certify import CertifiedGraphWriter

            try:
                result["materialize_graph_count"] = CertifiedGraphWriter(
                    domain_snap, settings
                ).add_enrichments(
                    uri_triples,
                    kind=CertifiedGraphWriter.KIND_INFERENCE,
                    run_id=task_id,
                )
            except OntoBricksError:
                raise
            except Exception as exc:
                logger.exception("Materialise to the Certified Graph failed: %s", exc)
                result["materialize_graph_error"] = "Materialise to graph failed"

        result["success"] = True
        return result
