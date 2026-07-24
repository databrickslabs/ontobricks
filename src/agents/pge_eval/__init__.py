"""OntoBricks PGE intrinsic-evaluation primitives.

This package holds usecase-agnostic, gold-free structural metrics for the PGE
pipeline.  This PR introduces only the **ontology** slice consumed by the
owl-generator Evaluator stage:

* :func:`agents.pge_eval.ontology_metrics.evaluate_ontology` — Stage-1
  deterministic ontology checks (orphan classes, dangling domain/range,
  naming, duplicates, footprint coverage), computed purely from the generated
  ontology + source metadata (no stored reference answer).

The full scorecard (mapping metrics, gate tiers, baseline regression, LLM
judge, CLI) lands in a separate change.  Importers should depend on the
concrete submodule (``agents.pge_eval.ontology_metrics``) rather than this
package root to avoid coupling to modules introduced later.
"""
