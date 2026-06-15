"""Scorecard assembly + verdict (§3.6).

``score_artifact`` is the single offline-testable code path (D6): it ingests
a captured ``AgentResult`` artifact (plus the generated ontology and source
metadata) and emits the §3.6 scorecard JSON.  Every deterministic metric is
computed with zero LLM calls; the advisory judge is the only network path
and runs only when ``no_judge`` is False.

Live mode (``scripts/goals_eval.py run``) is a thin wrapper: it produces the
artifact first, then calls this.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional

from agents.pge_eval import gates as gates_mod
from agents.pge_eval.baseline import DEFAULT_BASELINE_DIR, load_baseline
from agents.pge_eval.mapping_metrics import evaluate_mapping
from agents.pge_eval.normalize import normalize_metadata, normalize_ontology
from agents.pge_eval.ontology_metrics import evaluate_ontology
from agents.pge_eval.pipeline_metrics import evaluate_pipeline

SCHEMA_VERSION = "1.0"


def _digest(obj: Any) -> str:
    payload = json.dumps(obj, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def _ontology_digest(ontology: dict) -> str:
    norm = normalize_ontology(ontology)
    sig = {
        "classes": sorted(
            (c.get("name", ""), tuple(sorted(c.get("data_properties", []))))
            for c in norm.classes
        ),
        "object_properties": sorted(
            (op.get("name", ""), op.get("domain", ""), op.get("range", ""))
            for op in norm.object_properties
        ),
    }
    return _digest(sig)


def _metadata_digest(metadata: dict) -> str:
    tables = normalize_metadata(metadata)
    sig = sorted((t["name"], tuple(sorted(t["columns"]))) for t in tables)
    return _digest(sig)


def _resolve_inputs(artifact: dict, ontology, metadata):
    if ontology is None:
        ontology = artifact.get("ontology") or {}
    if metadata is None:
        metadata = (
            artifact.get("metadata")
            or artifact.get("source_metadata")
            or {}
        )
    return ontology, metadata


def score_artifact(
    artifact: dict,
    *,
    ontology: Optional[dict] = None,
    metadata: Optional[dict] = None,
    gate_ratios: bool = False,
    no_judge: bool = True,
    mode: str = "score-only",
    run_id: Optional[str] = None,
    timestamp: Optional[str] = None,
    endpoint: Optional[str] = None,
    host: Optional[str] = None,
    token: Optional[str] = None,
    baseline_dir: str = DEFAULT_BASELINE_DIR,
    baseline: Optional[Dict[str, Any]] = None,
    use_baseline: bool = True,
    ratio_threshold: float = gates_mod.DEFAULT_RATIO_THRESHOLD,
) -> Dict[str, Any]:
    """Score a captured artifact and return the §3.6 scorecard dict.

    Deterministic unless ``no_judge`` is False.  ``run_id``/``timestamp`` are
    stamped by the caller (kept out of the deterministic core so unit tests
    are reproducible).
    """
    ontology, metadata = _resolve_inputs(artifact, ontology, metadata)

    onto_metrics, stage1_issues, footprint = evaluate_ontology(ontology, metadata)
    map_metrics, map_extras = evaluate_mapping(artifact, ontology)
    pipeline = evaluate_pipeline(artifact, footprint, map_extras["mapped_cols"])

    # ---- advisory judge (only LLM path) ----------------------------
    if no_judge:
        onto_judge = {"score": None, "flags": []}
        map_judge = {"score": None, "flags": []}
    else:
        from agents.pge_eval.judge import run_judge

        verdicts = run_judge(
            host=host or "",
            token=token or "",
            endpoint_name=endpoint or "",
            ontology=ontology,
            artifact=artifact,
            stage1_issues=stage1_issues,
        )
        onto_judge = verdicts["ontology"]
        map_judge = verdicts["mapping"]

    stages = {
        "ontology": {"metrics": onto_metrics, "judge": onto_judge},
        "mapping": {
            "metrics": map_metrics,
            "judge": map_judge,
            # Persisted so Tier-3 can tell an inactive-1.0 band (no band declared)
            # from an active measurement, and not flag the first real band reading
            # as a regression.
            "band_active": bool(map_extras.get("band_active")),
        },
        "pipeline": pipeline,
    }

    # ---- gates -----------------------------------------------------
    active_conditionals = {"band_active": bool(map_extras.get("band_active"))}
    tier1 = gates_mod.evaluate_tier1(stages, active_conditionals=active_conditionals)
    tier2 = gates_mod.evaluate_tier2(
        stages, gate_ratios=gate_ratios, threshold=ratio_threshold
    )

    if baseline is None and use_baseline:
        baseline = load_baseline(baseline_dir, exclude_run_id=run_id)
    tier3 = gates_mod.evaluate_tier3(stages, baseline)

    passed = tier1["passed"] and tier2["passed"] and tier3["passed"]
    verdict = "GREEN" if passed else "RED"

    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "timestamp": timestamp,
        "mode": mode,
        "inputs": {
            "source_metadata_digest": _metadata_digest(metadata),
            "ontology_digest": _ontology_digest(ontology),
            "endpoint": None if no_judge else endpoint,
        },
        "stages": stages,
        "stage1_issues": stage1_issues,
        "gates": {
            "tier1_absolute": tier1,
            "tier2_ratio": tier2,
            "tier3_regression": tier3,
        },
        "verdict": verdict,
        "exit_code": 0 if verdict == "GREEN" else 1,
    }
