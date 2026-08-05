"""Scheduled inference run (SWRL / reasoning + materialisation).

Runs the selected reasoning phases against the domain's graph and
optionally writes the inferred triples back — to the graph store's
inferred companion, to a Delta table, or both. Without a materialise
target a scheduled run would compute inferences and throw them away, so
the config requires at least one.

:meth:`DigitalTwin.run_inference_task` owns the TaskManager task, so
this type is registered with ``delegates_task_lifecycle``.
"""

from __future__ import annotations

from typing import Any, Dict

from back.core.errors import ValidationError
from back.core.logging import get_logger

from .context import TaskContext

logger = get_logger(__name__)

#: The reasoning phases a schedule can toggle, in the order the
#: Reasoning tab shows them. Values are the defaults for a new schedule.
PHASES: Dict[str, bool] = {
    "tbox": True,
    "swrl": True,
    "graph": True,
    "decision_tables": False,
    "sparql_rules": False,
    "aggregate_rules": False,
}


def normalize_config(config: Dict[str, Any]) -> Dict[str, Any]:
    cfg = config or {}
    raw_phases = cfg.get("phases")
    if raw_phases is None:
        phases = dict(PHASES)
    else:
        # A phases dict that was sent is authoritative: a phase the caller
        # left out is off, not back on at its default.
        phases = {name: bool(raw_phases.get(name)) for name in PHASES}
    if not any(phases.values()):
        raise ValidationError("Enable at least one reasoning phase")

    materialize_graph = bool(cfg.get("materialize_graph", True))
    materialize_delta = bool(cfg.get("materialize_delta", False))
    materialize_table = (cfg.get("materialize_table") or "").strip()

    if not materialize_graph and not materialize_delta:
        raise ValidationError(
            "Pick at least one materialisation target — a scheduled run with "
            "no target would discard everything it infers"
        )
    if materialize_delta and len(materialize_table.split(".")) != 3:
        raise ValidationError(
            "The Delta target must be a fully qualified catalog.schema.table"
        )
    if not materialize_delta:
        materialize_table = ""

    return {
        "phases": phases,
        "materialize_graph": materialize_graph,
        "materialize_delta": materialize_delta,
        "materialize_table": materialize_table,
    }


def run(ctx: TaskContext) -> None:
    """Run the configured reasoning phases and materialise the results."""
    from back.objects.digitaltwin import DigitalTwin

    cfg = normalize_config(ctx.config)

    ctx.progress(5, "Loading domain from registry...")
    snapshot = ctx.snapshot

    options: Dict[str, Any] = dict(cfg["phases"])
    options["append_graph"] = cfg["materialize_graph"]
    options["materialize"] = cfg["materialize_delta"]
    options["materialize_table"] = cfg["materialize_table"]

    DigitalTwin.run_inference_task(
        ctx.tm,
        ctx.task_id,
        ctx.settings,
        snapshot,
        options,
        build_kind="scheduled",
    )
