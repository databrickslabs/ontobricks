"""OntoBricks PGE intrinsic-evaluation toolkit.

A usecase-agnostic, gold-free scorecard for the PGE pipeline (ontology
generation + mapping generation).  Intrinsic structural/self-consistency
metrics + an advisory LLM-judge — never a stored reference answer (D1).

Public surface:

* :func:`agents.pge_eval.scorecard.score_artifact` — the offline-testable
  scoring core (D6).
* :func:`agents.pge_eval.ontology_metrics.evaluate_ontology` — Stage-1
  deterministic ontology checks, shared with the owl-generator Evaluator
  stage (§3.5).
"""

from agents.pge_eval.scorecard import score_artifact  # noqa: F401

__all__ = ["score_artifact"]
