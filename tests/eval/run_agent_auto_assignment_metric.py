"""Metric-view SQL-generation contract eval for agent_auto_assignment.

Same judge as ``run_agent_mapping_pge_metric.py``: Auto-Map must wrap
measures in ``MEASURE()``, ``GROUP BY`` dimensions, never ``SELECT *``,
and never key an entity on a measure.

Dry run (default)::

    uv run --frozen python tests/eval/run_agent_auto_assignment_metric.py
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))
if str(ROOT / "tests" / "eval") not in sys.path:
    sys.path.insert(0, str(ROOT / "tests" / "eval"))

from metric_view_contract import run_contract  # noqa: E402

DATASET = ROOT / "tests/eval/datasets/agent_auto_assignment/metric_view.jsonl"
THRESHOLDS = ROOT / "tests/eval/thresholds.yaml"


def _threshold() -> float:
    import yaml

    data = yaml.safe_load(THRESHOLDS.read_text(encoding="utf-8"))
    return float(data["auto_assignment"]["metric_view_sql_correctness"])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--run-name", default="auto-assignment-metric-view")
    parser.add_argument(
        "--mlflow-experiment",
        default="/Shared/ontobricks/agents/auto_assignment",
    )
    parser.add_argument(
        "--mlflow-tracking-uri",
        default=os.getenv("MLFLOW_TRACKING_URI", "databricks"),
    )
    args = parser.parse_args()
    if args.live:
        parser.error(
            "Live Auto-Map scoring is not wired yet; dry-run validates the "
            "judge against build_metric_view_base_sql."
        )

    run_contract(
        agent_name="auto_assignment",
        dataset_path=DATASET,
        threshold=_threshold(),
        dry_run=True,
        run_name=args.run_name,
        mlflow_experiment=args.mlflow_experiment,
        mlflow_tracking_uri=args.mlflow_tracking_uri,
    )


if __name__ == "__main__":
    main()
