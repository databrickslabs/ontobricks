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
    def _same_table(a: str, b: str) -> bool:
        def norm(x: str) -> str:
            return (x or "").replace("`", "").strip().lower()

        return bool(a) and norm(a) == norm(b)

    @staticmethod
    def _delta_target_is_graph_store(store, svc, mat_table: str) -> bool:
        """True when the "write to Delta" target IS the graph store's inferred
        table — on a Lakehouse backend the two sinks coincide, so writing both
        would duplicate every triple."""
        resolver = getattr(store, "inferred_table_fqn", None)
        if not callable(resolver):
            return False
        try:
            target = resolver(svc._get_graph_name())
        except Exception:  # noqa: BLE001 — resolution is best-effort
            return False
        return TwinInferredMaterialize._same_table(target, mat_table)

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

        svc = None
        if do_graph and uri_triples:
            store = get_store(domain_snap, settings, for_write=True)
            if store is None:
                result["materialize_graph_error"] = "Graph store not available"
            else:
                svc = ReasoningService(domain_snap, store)

        # Lakehouse backend: the graph store's inferred table and the Delta
        # target are the same physical table, so running both sinks would write
        # every triple twice. Collapse to the graph path — it populates the
        # shared table AND rebuilds the adjacency index.
        collapse = (
            do_delta
            and svc is not None
            and bool(mat_table)
            and len(mat_table.split(".")) == 3
            and TwinInferredMaterialize._delta_target_is_graph_store(
                svc._store, svc, mat_table
            )
        )
        if collapse:
            do_delta = False

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

        if svc is not None and uri_triples:
            try:
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
                if collapse:
                    # Single shared write — report it under the Delta key too so
                    # the UI's "written to Delta" line reflects the same table.
                    result["materialize_count"] = count
                    result["materialize_table"] = mat_table
            except Exception as exc:
                logger.exception("Materialise to graph failed: %s", exc)
                result["materialize_graph_error"] = "Materialise to graph failed"

        result["success"] = True
        return result
