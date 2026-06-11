"""In-app scorecard hooks — run the PGE intrinsic evaluator *inside* the
Databricks app, right after ontology generation or mapping generation.

These are thin, fail-safe wrappers around
:func:`agents.pge_eval.scorecard.score_artifact`:

* **Deterministic only** — ``no_judge=True``: no extra LLM/network calls are
  added to the user-facing generation/mapping latency.
* **No baseline side-effects** — ``use_baseline=False``: the app server never
  reads/writes the Tier-3 ``logs/goals`` baseline store (that is a CI/CLI
  concern). The in-app scorecard is a per-run quality snapshot surfaced to
  the user.
* **Never raises** — scoring must never break a generation/mapping run; any
  failure logs a warning and returns ``None``.

The result is the §3.6 scorecard dict, attached to the background task's
``result`` so the UI can surface verdict + metrics.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from back.core.logging import get_logger

logger = get_logger(__name__)


def _now():
    t = datetime.now(timezone.utc)
    return t.strftime("%Y%m%dT%H%M%S_%f"), t.isoformat()


def _turtle_to_ontology(owl_content: str) -> Dict[str, Any]:
    """Parse generated Turtle into the registry ontology shape.

    Lazy imports keep the pure scorecard modules free of back/ deps.
    """
    from back.core.w3c.owl.OntologyParser import OntologyParser
    from back.objects.ontology.Ontology import Ontology

    cleaned = Ontology.clean_owl_output(owl_content or "")
    parser = OntologyParser(cleaned)
    return {"classes": parser.get_classes(), "properties": parser.get_properties()}


def score_generated_ontology(
    owl_content: str,
    metadata: Optional[dict],
) -> Optional[Dict[str, Any]]:
    """Score a freshly generated ontology (Stage-1 focus). Returns the §3.6
    scorecard dict, or ``None`` on any failure."""
    try:
        from agents.pge_eval.scorecard import score_artifact

        ontology = _turtle_to_ontology(owl_content)
        artifact = {
            "ontology": ontology,
            "metadata": metadata or {"tables": []},
            "mapping_run_log": [],
            "mapping_evaluations": {},
            "entity_mappings": [],
        }
        run_id, ts = _now()
        scorecard = score_artifact(
            artifact,
            no_judge=True,
            use_baseline=False,
            mode="live",
            run_id=run_id,
            timestamp=ts,
        )
        logger.info(
            "in-app ontology scorecard: verdict=%s (orphans=%s, dangling=%s, "
            "naming=%s, dupes=%s)",
            scorecard["verdict"],
            scorecard["stages"]["ontology"]["metrics"]["orphan_class_count"],
            scorecard["stages"]["ontology"]["metrics"]["dangling_domain_range_count"],
            scorecard["stages"]["ontology"]["metrics"]["naming_violation_count"],
            scorecard["stages"]["ontology"]["metrics"]["duplicate_class_count"],
        )
        return scorecard
    except Exception as exc:  # noqa: BLE001 — scoring must never break generation
        logger.warning("in-app ontology scoring failed (ignored): %s", exc)
        return None


def score_mapping_run(
    *,
    ontology: dict,
    metadata: Optional[dict],
    mapping_run_log: Optional[List[dict]],
    mapping_evaluations: Optional[Dict[str, dict]],
    entity_mappings: Optional[List[dict]],
    relationship_mappings: Optional[List[dict]] = None,
    usage: Optional[dict] = None,
) -> Optional[Dict[str, Any]]:
    """Score a completed mapping-PGE run (Stage-2 + Stage-1 + pipeline).
    Returns the §3.6 scorecard dict, or ``None`` on any failure."""
    try:
        from agents.pge_eval.scorecard import score_artifact

        artifact = {
            "ontology": ontology or {},
            "metadata": metadata or {"tables": []},
            "mapping_run_log": mapping_run_log or [],
            "mapping_evaluations": mapping_evaluations or {},
            "entity_mappings": entity_mappings or [],
            "relationship_mappings": relationship_mappings or [],
            "usage": usage or {},
        }
        run_id, ts = _now()
        scorecard = score_artifact(
            artifact,
            no_judge=True,
            use_baseline=False,
            mode="live",
            run_id=run_id,
            timestamp=ts,
        )
        m = scorecard["stages"]["mapping"]["metrics"]
        logger.info(
            "in-app mapping scorecard: verdict=%s (entity_completeness=%s, "
            "rel_completeness=%s, id_integrity=%s, sql_failures=%s)",
            scorecard["verdict"],
            m["entity_completeness"],
            m["relationship_completeness"],
            m["id_integrity"],
            m["sql_exec_failures"],
        )
        return scorecard
    except Exception as exc:  # noqa: BLE001 — scoring must never break mapping
        logger.warning("in-app mapping scoring failed (ignored): %s", exc)
        return None
