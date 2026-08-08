"""Scheduled cohort materialisation.

Runs one saved cohort rule of a domain and writes its members to the
graph, to a Unity Catalog table, or both. The two output flags override
the rule's own ``output`` config for this schedule only — the saved rule
is never mutated.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List

from back.core.errors import InfrastructureError, NotFoundError, ValidationError
from back.core.logging import get_logger

from .context import RunOutcome, TaskContext

logger = get_logger(__name__)


def normalize_config(config: Dict[str, Any]) -> Dict[str, Any]:
    cfg = config or {}
    output_graph = bool(cfg.get("output_graph", True))
    output_uc = bool(cfg.get("output_uc", True))
    if not output_graph and not output_uc:
        raise ValidationError("Pick at least one output target")
    return {"output_graph": output_graph, "output_uc": output_uc}


def run(ctx: TaskContext) -> RunOutcome:
    """Materialise the cohort rule named by ``ctx.target_key``."""
    from back.objects.digitaltwin import CohortService

    start = time.time()
    rule_id = ctx.target_key

    ctx.progress(5, "Loading domain from registry...")
    domain = ctx.domain

    rules = list(getattr(domain, "cohort_rules", []) or [])
    if not any((r.get("id") == rule_id) for r in rules):
        raise NotFoundError(
            f"Cohort rule '{rule_id}' not found in domain '{ctx.domain_name}'"
        )

    store = ctx.graph_store
    if not store:
        raise InfrastructureError("Could not initialize graph backend")

    graph_name = ctx.graph_name
    client = ctx.warehouse_client

    ctx.advance("Running cohort engine...")

    def _label_resolver(uris):
        try:
            metadata = store.get_entity_metadata(graph_name, list(uris))
        except Exception:
            return {}
        return {row.get("uri", ""): row.get("label", "") for row in metadata or []}

    ctx.advance("Writing cohort outputs...")
    result = CohortService(domain).materialize(
        rule_id,
        store,
        graph_name,
        client=client,
        domain_version=str(ctx.loaded_version or ""),
        member_label_resolver=_label_resolver,
        output_graph=bool(ctx.config.get("output_graph", True)),
        output_uc=bool(ctx.config.get("output_uc", True)),
    )

    materialized_triples = int(result.get("materialized_triples") or 0)
    uc_rows_written = int(result.get("uc_rows_written") or 0)

    bits: List[str] = []
    if materialized_triples:
        bits.append(f"{materialized_triples} triples")
    if uc_rows_written:
        bits.append(f"{uc_rows_written} UC rows")
    if not bits:
        bits.append("0 outputs (rule produced no cohorts)")

    duration = time.time() - start
    status = "success"
    message = f"Materialised {' / '.join(bits)} in {duration:.1f}s"
    graph_err = result.get("materialize_graph_error")
    uc_err = result.get("materialize_uc_error")
    if graph_err or uc_err:
        status = "error"
        message = (graph_err or uc_err) or message

    return RunOutcome(
        status=status,
        message=message,
        count=materialized_triples + uc_rows_written,
        detail={
            "materialized_triples": materialized_triples,
            "uc_rows_written": uc_rows_written,
        },
        task_result={
            "materialized_triples": materialized_triples,
            "uc_rows_written": uc_rows_written,
            "duration_seconds": duration,
        },
    )
