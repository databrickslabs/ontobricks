"""Materialise inferred triples to Delta and/or the graph store.

Extracted from ``api.routers.internal.dtwin.materialize_inferred``.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List

from back.core.errors import NotFoundError, ValidationError
from back.core.helpers import is_uri
from back.core.logging import get_logger
from back.core.reasoning import InferredTriple, ReasoningResult, ReasoningService
from back.objects.digitaltwin.models import DomainSnapshot

logger = get_logger(__name__)


class TwinInferredMaterialize:
    """Apply a completed inference task's triples to Delta and/or the graph."""

    @staticmethod
    def uri_triples(raw_triples: list) -> List[dict]:
        return [
            t
            for t in raw_triples
            if is_uri(t.get("subject", ""))
            and is_uri(t.get("predicate", ""))
            and is_uri(t.get("object", ""))
        ]

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
        get_store: Callable,
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
            try:
                store = get_store(domain_snap, settings, for_write=True)
                if store is None:
                    result["materialize_graph_error"] = "Graph store not available"
                else:
                    svc = ReasoningService(domain_snap, store)
                    inferred = [
                        InferredTriple(
                            subject=t.get("subject", ""),
                            predicate=t.get("predicate", ""),
                            object=t.get("object", ""),
                            provenance=t.get("provenance", ""),
                        )
                        for t in uri_triples
                    ]
                    count = svc.materialize_inferred(
                        ReasoningResult(inferred_triples=inferred)
                    )
                    result["materialize_graph_count"] = count
            except Exception as exc:
                logger.exception("Materialise to graph failed: %s", exc)
                result["materialize_graph_error"] = "Materialise to graph failed"

        result["success"] = True
        return result
