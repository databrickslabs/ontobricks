"""The scheduler's task-type registry.

Every recurring job the scheduler can run is one :class:`TaskTypeSpec`
entry here. A spec declares how the type is labelled, whether it needs a
target inside the domain, how its options are validated, and what to
execute — nothing else in the stack branches on the type name.

Adding a task type means writing one module with ``run(ctx)`` and
``normalize_config(config)`` and registering it below. The store, the
scheduler, the routes, and the settings UI all pick it up from the
registry.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from back.core.errors import ValidationError

from . import analytics, build, cohort, reasoning
from .context import RunOutcome, TaskContext, load_domain_headless

TASK_BUILD = "build"
TASK_COHORT = "cohort"
TASK_ANALYTICS = "analytics"
TASK_REASONING = "reasoning"


@dataclass(frozen=True)
class TaskTypeSpec:
    """Everything the scheduler needs to know about one kind of job."""

    key: str
    label: str
    #: ``task_type`` tag on the TaskManager task, e.g. ``scheduled_build``.
    task_tag: str
    #: Progress steps shown while the task runs.
    steps: List[Dict[str, str]]
    #: Validates and fills in a config dict; raises ``ValidationError``.
    normalize_config: Callable[[Dict[str, Any]], Dict[str, Any]]
    #: Executes one run. Returns a :class:`RunOutcome` unless the type
    #: delegates the task lifecycle, in which case it returns ``None``.
    run: Callable[[TaskContext], Optional[RunOutcome]]
    #: Types keyed on a sub-object of the domain (a cohort rule id).
    needs_target: bool = False
    target_label: str = ""
    #: True when :attr:`run` hands off to a service that completes or
    #: fails the TaskManager task itself. The harness then derives the
    #: outcome from the finished task instead of completing it twice.
    delegates_task_lifecycle: bool = False
    #: Optional hook run after the outcome is known, success or failure.
    on_finish: Optional[Callable[[TaskContext, RunOutcome, float], None]] = None
    #: Keys in the run-history ``detail`` blob this type populates.
    detail_keys: List[str] = field(default_factory=list)
    #: For delegating types, the key in the finished task's result that
    #: holds the generic "how much did this run write" counter.
    count_key: str = ""


TASK_TYPES: Dict[str, TaskTypeSpec] = {
    TASK_BUILD: TaskTypeSpec(
        key=TASK_BUILD,
        label="Knowledge Graph Build",
        task_tag="scheduled_build",
        steps=[
            {"name": "prepare", "description": "Loading domain and generating SQL"},
            {"name": "view", "description": "Creating Triple-Store VIEW in Unity Catalog"},
            {"name": "graph", "description": "Populating the graph store"},
        ],
        normalize_config=build.normalize_config,
        run=build.run,
        on_finish=build.on_finish,
    ),
    TASK_COHORT: TaskTypeSpec(
        key=TASK_COHORT,
        label="Cohort Materialisation",
        task_tag="scheduled_cohort",
        steps=[
            {"name": "prepare", "description": "Loading domain and rule"},
            {"name": "engine", "description": "Running cohort engine"},
            {"name": "write", "description": "Writing cohort outputs"},
        ],
        normalize_config=cohort.normalize_config,
        run=cohort.run,
        needs_target=True,
        target_label="Cohort rule",
        detail_keys=["materialized_triples", "uc_rows_written"],
    ),
    TASK_ANALYTICS: TaskTypeSpec(
        key=TASK_ANALYTICS,
        label="Graph Analytics",
        task_tag="scheduled_analytics",
        steps=[
            {"name": "prepare", "description": "Loading domain from registry"},
            {"name": "job", "description": "Running the Databricks analytics job"},
        ],
        normalize_config=analytics.normalize_config,
        run=analytics.run,
        delegates_task_lifecycle=True,
        detail_keys=["node_count", "duration_ms"],
        count_key="node_count",
    ),
    TASK_REASONING: TaskTypeSpec(
        key=TASK_REASONING,
        label="Inference",
        task_tag="scheduled_reasoning",
        steps=[
            {"name": "prepare", "description": "Loading domain from registry"},
            {"name": "infer", "description": "Running inference phases"},
            {"name": "write", "description": "Materialising inferred triples"},
        ],
        normalize_config=reasoning.normalize_config,
        run=reasoning.run,
        delegates_task_lifecycle=True,
        detail_keys=["inferred_count", "append_graph_count", "materialize_count"],
        count_key="inferred_count",
    ),
}


def get_task_type(key: str) -> TaskTypeSpec:
    """Return the spec for *key*, or raise ``ValidationError``."""
    spec = TASK_TYPES.get((key or "").strip())
    if spec is None:
        known = ", ".join(sorted(TASK_TYPES))
        raise ValidationError(f"Unknown schedule type '{key}' (expected one of: {known})")
    return spec


def task_type_catalog() -> List[Dict[str, Any]]:
    """Serialisable description of every task type, for the settings UI."""
    return [
        {
            "key": spec.key,
            "label": spec.label,
            "needs_target": spec.needs_target,
            "target_label": spec.target_label,
            "detail_keys": list(spec.detail_keys),
        }
        for spec in TASK_TYPES.values()
    ]


__all__ = [
    "RunOutcome",
    "TASK_ANALYTICS",
    "TASK_BUILD",
    "TASK_COHORT",
    "TASK_REASONING",
    "TASK_TYPES",
    "TaskContext",
    "TaskTypeSpec",
    "get_task_type",
    "load_domain_headless",
    "task_type_catalog",
]
