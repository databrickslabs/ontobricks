"""Pipeline-level metrics (deterministic, no LLM).

* ``coverage_loss`` — source concepts the ontology surfaced but that never
  reached a mapping (ontology footprint − mapped footprint).  The gap
  between the two complementary denominators of D2.
* ``convergence`` — effort signals (mean generator attempts, planner
  reinvocations, total tokens, wall-clock).

Both pipeline metrics are **tracked/advisory only** — they are reported on the
scorecard for inspection and trend-watching but are not wired into any gate
tier (no ``METRIC_SPECS`` entry references the ``pipeline`` stage). Treat them
as observability, not pass/fail.

Usecase-agnostic.
"""

from __future__ import annotations

from typing import Any, Dict, List, Set

from agents.pge_eval.normalize import normalize_name


def _surfaced_column_keys(footprint: Dict[str, Any]) -> Set[str]:
    """Normalised column-name keys of every ontology-covered column.

    ``footprint['covered_columns']`` holds ``table::col`` keys; the loss
    comparison works at the column-name level so it matches the mapped
    footprint (which has no reliable table qualifier).
    """
    out: Set[str] = set()
    for key in footprint.get("covered_columns", set()):
        col = key.split("::", 1)[-1]
        if col:
            out.add(col)
    return out


def evaluate_pipeline(
    artifact: dict,
    ontology_footprint: Dict[str, Any],
    mapped_cols: Set[str],
) -> Dict[str, Any]:
    surfaced = _surfaced_column_keys(ontology_footprint)
    lost = {c for c in surfaced if c not in mapped_cols}
    coverage_loss = len(lost)

    # ---- convergence -----------------------------------------------
    run_log = artifact.get("mapping_run_log", []) or []
    attempt_counts: List[int] = [
        len(entry.get("attempts", []) or [])
        for entry in run_log
        if entry.get("attempts")
    ]
    mean_attempts = (
        round(sum(attempt_counts) / len(attempt_counts), 6) if attempt_counts else 0.0
    )

    stats = artifact.get("stats", {}) or {}
    planner_reinvocations = int(
        stats.get("planner_reinvocations", artifact.get("planner_reinvocations", 0)) or 0
    )

    usage = artifact.get("usage", {}) or {}
    total_tokens = int(
        usage.get("total_tokens", 0)
        or (usage.get("prompt_tokens", 0) + usage.get("completion_tokens", 0))
    )

    wall_clock_s = float(artifact.get("elapsed_s", 0.0) or 0.0)
    if not wall_clock_s:
        step_ms = sum(
            int(s.get("duration_ms", 0) or 0) for s in artifact.get("steps", []) or []
        )
        wall_clock_s = round(step_ms / 1000.0, 3)

    return {
        "coverage_loss": coverage_loss,
        "convergence": {
            "mean_generator_attempts": mean_attempts,
            "planner_reinvocations": planner_reinvocations,
            "total_tokens": total_tokens,
            "wall_clock_s": wall_clock_s,
        },
    }
