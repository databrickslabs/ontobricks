"""The three gate tiers (§3.4) + metric directionality.

* **Tier 1 — absolute hard gates** (always active).  Integrity / hygiene /
  executability invariants that hold for any domain; non-zero exit on fail.
* **Tier 2 — ratio thresholds**.  Warn by default; promotable to hard gates
  per run via ``--gate-ratios``.  The 0.90 default is a starting heuristic,
  overridable, never an absolute truth.
* **Tier 3 — self-baseline regression** (active when a baseline exists).
  Any Tier-1/Tier-2 metric that drops vs the last accepted baseline beyond
  its tolerance fails the run, even if still above its absolute bar.

The LLM judge is Tier-exempt — it never appears here.

No domain identifiers, table names, or counts are encoded; every threshold
is a generic structural bar.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

DEFAULT_RATIO_THRESHOLD = 0.90

# Directionality for the Tier-3 regression gate.
HIGHER_BETTER = "higher_better"
LOWER_BETTER = "lower_better"


# Each spec: stage, key, tier, direction, plus tier-specific config.
# ``conditional`` marks a metric that only gates when active (set at runtime).
METRIC_SPECS: List[Dict[str, Any]] = [
    # ---- Tier-1 absolute (ontology) ----
    {"stage": "ontology", "key": "orphan_class_count", "tier": 1,
     "direction": LOWER_BETTER, "op": "==", "bound": 0, "tol": 0},
    {"stage": "ontology", "key": "dangling_domain_range_count", "tier": 1,
     "direction": LOWER_BETTER, "op": "==", "bound": 0, "tol": 0},
    {"stage": "ontology", "key": "naming_violation_count", "tier": 1,
     "direction": LOWER_BETTER, "op": "==", "bound": 0, "tol": 0},
    {"stage": "ontology", "key": "duplicate_class_count", "tier": 1,
     "direction": LOWER_BETTER, "op": "==", "bound": 0, "tol": 0},
    # ---- Tier-1 absolute (mapping) ----
    {"stage": "mapping", "key": "dangling_target_pct_max", "tier": 1,
     "direction": LOWER_BETTER, "op": "<", "bound": 0.05, "tol": 0.01},
    {"stage": "mapping", "key": "dangling_source_pct_max", "tier": 1,
     "direction": LOWER_BETTER, "op": "<", "bound": 0.05, "tol": 0.01},
    {"stage": "mapping", "key": "id_integrity", "tier": 1,
     "direction": HIGHER_BETTER, "op": "==", "bound": 1.0, "tol": 0.0},
    {"stage": "mapping", "key": "sql_exec_failures", "tier": 1,
     "direction": LOWER_BETTER, "op": "==", "bound": 0, "tol": 0},
    {"stage": "mapping", "key": "cross_source_band_compliance", "tier": 1,
     "direction": HIGHER_BETTER, "op": "==", "bound": 1.0, "tol": 0.0,
     "conditional": "band_active"},
    # ---- Tier-2 ratio ----
    {"stage": "ontology", "key": "table_footprint_coverage", "tier": 2,
     "direction": HIGHER_BETTER, "tol": 0.02},
    {"stage": "ontology", "key": "column_footprint_coverage", "tier": 2,
     "direction": HIGHER_BETTER, "tol": 0.02},
    {"stage": "mapping", "key": "entity_completeness", "tier": 2,
     "direction": HIGHER_BETTER, "tol": 0.02},
    {"stage": "mapping", "key": "relationship_completeness", "tier": 2,
     "direction": HIGHER_BETTER, "tol": 0.02},
    {"stage": "mapping", "key": "attribute_coverage", "tier": 2,
     "direction": HIGHER_BETTER, "tol": 0.02},
]


def get_metric(stages: Dict[str, Any], stage: str, key: str) -> Any:
    return ((stages.get(stage, {}) or {}).get("metrics", {}) or {}).get(key)


def _abs_pass(op: str, value: float, bound: float) -> bool:
    if value is None:
        return False
    if op == "==":
        return value == bound
    if op == "<":
        return value < bound
    if op == "<=":
        return value <= bound
    if op == ">=":
        return value >= bound
    raise ValueError(f"unknown op {op!r}")


def evaluate_tier1(
    stages: Dict[str, Any], *, active_conditionals: Optional[Dict[str, bool]] = None
) -> Dict[str, Any]:
    active_conditionals = active_conditionals or {}
    failures: List[Dict[str, Any]] = []
    for spec in METRIC_SPECS:
        if spec["tier"] != 1:
            continue
        cond = spec.get("conditional")
        if cond and not active_conditionals.get(cond, False):
            continue
        value = get_metric(stages, spec["stage"], spec["key"])
        if not _abs_pass(spec["op"], value, spec["bound"]):
            failures.append(
                {
                    "metric": f"{spec['stage']}.{spec['key']}",
                    "observed": value,
                    "expected": f"{spec['op']} {spec['bound']}",
                }
            )
    return {"passed": not failures, "failures": failures}


def evaluate_tier2(
    stages: Dict[str, Any],
    *,
    gate_ratios: bool,
    threshold: float = DEFAULT_RATIO_THRESHOLD,
) -> Dict[str, Any]:
    warnings: List[Dict[str, Any]] = []
    for spec in METRIC_SPECS:
        if spec["tier"] != 2:
            continue
        value = get_metric(stages, spec["stage"], spec["key"])
        if value is None or value < threshold:
            warnings.append(
                {
                    "metric": f"{spec['stage']}.{spec['key']}",
                    "observed": value,
                    "expected": f">= {threshold}",
                }
            )
    # When --gate-ratios is set, the warnings become hard failures.
    passed = (not warnings) if gate_ratios else True
    return {"gated": gate_ratios, "passed": passed, "warnings": warnings}


def evaluate_tier3(
    stages: Dict[str, Any],
    baseline: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Compare every Tier-1/Tier-2 metric against the baseline scorecard.

    A metric *regresses* when it moves the wrong way beyond its tolerance.
    Direction-aware: higher-better metrics regress on a drop, lower-better
    metrics regress on a rise.
    """
    if not baseline:
        return {"baseline_run_id": None, "passed": True, "regressions": []}

    base_stages = baseline.get("stages", {})
    base_id = baseline.get("run_id")
    regressions: List[Dict[str, Any]] = []
    for spec in METRIC_SPECS:
        value = get_metric(stages, spec["stage"], spec["key"])
        base_value = get_metric(base_stages, spec["stage"], spec["key"])
        if value is None or base_value is None:
            continue
        tol = spec.get("tol", 0)
        regressed = False
        if spec["direction"] == HIGHER_BETTER:
            regressed = value < base_value - tol
        else:  # LOWER_BETTER
            regressed = value > base_value + tol
        if regressed:
            regressions.append(
                {
                    "metric": f"{spec['stage']}.{spec['key']}",
                    "observed": value,
                    "baseline": base_value,
                    "direction": spec["direction"],
                    "tolerance": tol,
                }
            )
    return {
        "baseline_run_id": base_id,
        "passed": not regressions,
        "regressions": regressions,
    }
