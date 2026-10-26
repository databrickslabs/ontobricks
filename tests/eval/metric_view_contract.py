"""Shared scoring for the agent_mapping_pge metric-view SQL contract.

A metric view is not a plain table: dimensions are projected directly, every
measure must be wrapped in ``MEASURE()``, and any query that projects a measure
must ``GROUP BY`` its dimensions (never ``SELECT *``). The measure columns can
never be the entity id. This module scores a produced SQL string (plus the
resolved id column) against the per-example constraints declared in
``tests/eval/datasets/agent_mapping_pge/metric_view.jsonl``.

The scorer is pure and deterministic — it is the *judge*, shared by:

* the **dry-run** path, which feeds it a reference SQL built by
  ``build_metric_view_base_sql`` to prove the judge accepts a correct answer, and
* the **live** path, which feeds it the SQL the EntityGenerator actually emits.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

# One observation = the SQL the agent produced + the id column it chose.
Observation = Tuple[str, Optional[str]]

VALID_CONSTRAINTS = {
    "metric_sql_no_select_star",
    "metric_measure_wrapped",
    "metric_dimension_not_wrapped",
    "metric_group_by",
    "metric_group_by_includes",
    "metric_no_group_by_without_measure",
    "metric_id_is_dimension",
    "metric_id_not_measure",
}


def load_examples(path: Path) -> List[Dict[str, Any]]:
    """Load and validate the metric-view JSONL dataset."""
    examples = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    examples = [
        ex
        for ex in examples
        if ex.get("input", {}).get("source", {}).get("object_kind") == "metric_view"
    ]
    if len(examples) < 10:
        raise ValueError(
            f"{path} has {len(examples)} metric-view examples; minimum is 10"
        )
    ids = [str(ex.get("id", "")) for ex in examples]
    if any(not i for i in ids) or len(ids) != len(set(ids)):
        raise ValueError(f"{path} contains missing or duplicate ids")
    for ex in examples:
        for constraint in ex.get("expected", {}).get("constraints", []):
            if constraint.get("kind") not in VALID_CONSTRAINTS:
                raise ValueError(
                    f"{ex['id']}: unknown constraint {constraint.get('kind')}"
                )
    return examples


# --------------------------------------------------------------------------
# SQL introspection helpers (regex-level; no SQL parser dependency)
# --------------------------------------------------------------------------


def _has_select_star(sql: str) -> bool:
    return bool(re.search(r"(?i)\bselect\s+\*", sql or ""))


def _measure_wrapped(sql: str, col: str) -> bool:
    return bool(
        re.search(rf"(?i)MEASURE\(\s*`?{re.escape(col)}`?\s*\)", sql or "")
    )


def _column_referenced(sql: str, col: str) -> bool:
    return bool(
        re.search(
            rf"(?i)(?<![A-Za-z0-9_])`?{re.escape(col)}`?(?![A-Za-z0-9_])", sql or ""
        )
    )


def _group_by_columns(sql: str) -> Optional[List[str]]:
    """Return the lower-cased column names in the GROUP BY, or ``None`` if absent."""
    match = re.search(r"(?is)\bgroup\s+by\b(.*)$", sql or "")
    if not match:
        return None
    tail = match.group(1)
    # Cut at the next major clause if any.
    tail = re.split(r"(?is)\b(order\s+by|having|limit|window|qualify)\b", tail)[0]
    cols = [c.strip().strip("`") for c in tail.split(",") if c.strip()]
    return [c.lower() for c in cols if c]


def score_example(example: Dict[str, Any], sql: str, id_column: Optional[str]) -> Dict[str, float]:
    """Score one produced SQL against the example's metric-view constraints.

    Returns a dict of per-kind scores plus a ``weighted`` aggregate (the mean of
    every constraint's satisfaction, all constraints weighted equally).
    """
    constraints = example.get("expected", {}).get("constraints", [])
    checks: List[float] = []
    gb = _group_by_columns(sql)
    id_lower = (id_column or "").lower()

    for constraint in constraints:
        kind = constraint["kind"]
        value = str(constraint.get("value", ""))

        if kind == "metric_sql_no_select_star":
            ok = not _has_select_star(sql)
        elif kind == "metric_measure_wrapped":
            ok = _measure_wrapped(sql, value)
        elif kind == "metric_dimension_not_wrapped":
            ok = _column_referenced(sql, value) and not _measure_wrapped(sql, value)
        elif kind == "metric_group_by":
            want = {v.strip().lower() for v in value.split(",") if v.strip()}
            ok = gb is not None and set(gb) == want
        elif kind == "metric_group_by_includes":
            ok = gb is not None and value.lower() in set(gb)
        elif kind == "metric_no_group_by_without_measure":
            ok = gb is None
        elif kind == "metric_id_is_dimension":
            ok = id_lower == value.lower()
        elif kind == "metric_id_not_measure":
            ok = id_lower != value.lower()
        else:  # pragma: no cover — guarded by load_examples
            ok = False
        checks.append(1.0 if ok else 0.0)

    weighted = sum(checks) / len(checks) if checks else 1.0
    return {"weighted": weighted, "constraints_total": float(len(checks))}


# --------------------------------------------------------------------------
# Reference (dry-run) observation
# --------------------------------------------------------------------------


def reference_observation(example: Dict[str, Any]) -> Observation:
    """Build a self-consistent correct answer used to validate the judge.

    Uses the production ``build_metric_view_base_sql`` builder and picks the
    first dimension as the id column — exactly what a well-behaved agent should
    produce for a metric view.
    """
    from back.core.databricks.uc.metric_sql import build_metric_view_base_sql

    source = example["input"]["source"]
    sql = build_metric_view_base_sql(source)
    dims = [
        c["name"]
        for c in source.get("columns", [])
        if c.get("role", "dimension") != "measure"
    ]
    id_column = dims[0] if dims else None
    return sql, id_column


# --------------------------------------------------------------------------
# Contract runner
# --------------------------------------------------------------------------


def run_contract(
    *,
    agent_name: str,
    dataset_path: Path,
    threshold: float,
    dry_run: bool,
    live_runner: Optional[Callable[[Dict[str, Any]], Observation]] = None,
    run_name: str = "metric-view-baseline",
    mlflow_experiment: Optional[str] = None,
    mlflow_tracking_uri: Optional[str] = None,
) -> float:
    """Score every metric-view example and return the aggregate.

    In ``dry_run`` mode the reference builder feeds the judge (proves the judge
    accepts a correct answer). Otherwise ``live_runner`` must return the
    ``(sql, id_column)`` the agent produced for each example.
    """
    examples = load_examples(dataset_path)
    results: List[Dict[str, float]] = []
    per_tag: Dict[str, List[float]] = {}

    for example in examples:
        if dry_run:
            observation = reference_observation(example)
        elif live_runner is not None:
            observation = live_runner(example)
        else:
            raise ValueError("Live mode requires a live runner")
        scores = score_example(example, *observation)
        results.append(scores)
        for tag in example.get("tags", []):
            per_tag.setdefault(tag, []).append(scores["weighted"])
        state = "PASS" if scores["weighted"] >= threshold else "FAIL"
        print(f"[{state}] {example['id']}: {scores['weighted']:.3f}")

    aggregate = sum(r["weighted"] for r in results) / len(results)
    print(f"Aggregate: {aggregate:.3f} (threshold {threshold:.3f})")
    for tag, vals in sorted(per_tag.items()):
        print(f"  tag {tag}: {sum(vals) / len(vals):.3f} (n={len(vals)})")

    if not dry_run and mlflow_experiment:
        import mlflow

        if mlflow_tracking_uri:
            mlflow.set_tracking_uri(mlflow_tracking_uri)
        mlflow.set_experiment(mlflow_experiment)
        with mlflow.start_run(run_name=run_name) as run:
            mlflow.log_metric("metric_view_sql_correctness", aggregate)
            for tag, vals in per_tag.items():
                mlflow.log_metric(f"tag_{tag}", sum(vals) / len(vals))
            mlflow.log_artifact(str(dataset_path))
            print(f"MLflow run: {run.info.run_id}")

    if aggregate < threshold:
        raise SystemExit(
            f"Metric-view contract {aggregate:.3f} is below {threshold:.3f}"
        )
    return aggregate
