"""Planner -> Generator -> Evaluator (PGE) mapping agent.

This package replaces the single-loop ReAct ``agent_auto_assignment`` with a
three-stage pipeline:

* **Planner** — proposes a :class:`SourceModel` (table roles, canonical IDs,
  join keys, ordered mapping plan).
* **Generator** — produces individual entity/relationship mappings given the
  plan.
* **Evaluator** — checks each submitted mapping; stage 1 is deterministic
  (pure SQL counts), stage 2 is semantic.

Sprint 1 lays the foundation: the typed contracts plus the deterministic
evaluator.  Subsequent sprints add the LLM-backed Planner, Generator,
semantic Evaluator, and the orchestrating loop.
"""

from agents.agent_mapping_pge.contracts import (
    CanonicalId,
    EvalFailure,
    EvalReport,
    JoinKey,
    MappingPlan,
    RetryState,
    SkipItem,
    SourceModel,
    TableRole,
    TableRoleCandidate,
)

__all__ = [
    "CanonicalId",
    "EvalFailure",
    "EvalReport",
    "JoinKey",
    "MappingPlan",
    "RetryState",
    "SkipItem",
    "SourceModel",
    "TableRole",
    "TableRoleCandidate",
]
