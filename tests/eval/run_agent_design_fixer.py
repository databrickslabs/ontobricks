"""Eval runner for agent_design_fixer.

Dry run (default) scores the baseline dataset contract without calling the
LLM::

    uv run --frozen python tests/eval/run_agent_design_fixer.py

Live scoring (agent output vs. judges) is not wired yet; ``--live`` exits
with an explicit error rather than reporting a fake score.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))
if str(ROOT / "tests" / "eval") not in sys.path:
    sys.path.insert(0, str(ROOT / "tests" / "eval"))

from judges.design_fixer import score_dataset_row  # noqa: E402

DATASET = ROOT / "tests/eval/datasets/agent_design_fixer/baseline.jsonl"
THRESHOLDS = ROOT / "tests/eval/thresholds.yaml"
MIN_ROWS = 20


def _load_rows(path: Path) -> list[dict]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def _threshold() -> float:
    import yaml

    data = yaml.safe_load(THRESHOLDS.read_text(encoding="utf-8"))
    return float(data["design_fixer"]["schema_valid"])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--run-name", default="design-fixer-schema")
    parser.add_argument(
        "--mlflow-experiment",
        default="/Shared/ontobricks/agents/design_fixer",
    )
    parser.add_argument(
        "--mlflow-tracking-uri",
        default=os.getenv("MLFLOW_TRACKING_URI", "databricks"),
    )
    args = parser.parse_args()
    if args.live:
        parser.error(
            "Live design-fixer scoring is not wired yet; dry-run "
            "validates the dataset contract."
        )

    rows = _load_rows(DATASET)
    if len(rows) < MIN_ROWS:
        raise SystemExit(f"{DATASET} has {len(rows)} rows; need ≥ {MIN_ROWS}")
    scores = [score_dataset_row(row) for row in rows]
    mean = sum(scores) / len(scores)
    threshold = _threshold()
    print(
        f"design_fixer schema_valid={mean:.3f} "
        f"n={len(rows)} threshold={threshold:.3f} "
        f"run={args.run_name} experiment={args.mlflow_experiment} "
        f"tracking={args.mlflow_tracking_uri}"
    )
    if mean < threshold:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
