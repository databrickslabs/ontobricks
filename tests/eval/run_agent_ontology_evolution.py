"""Eval runner for Ontology → Evolve (``agent_ontology_evolution``).

Modes:

* default (dry run): validates the dataset contract; no network, no LLM.
* ``--deterministic``: runs the coverage check offline on the fixture domain
  variants with the manual intent from each row, and scores readiness,
  states, gap and decision. No warehouse or LLM.
* ``--live``: the agent against a real LLM endpoint, the TPC-H glossary
  Genie Page and the TPC-H S tables (see ``evolution_agent_live.py``), dev
  rows only unless ``--split`` says otherwise, ``--repeats`` times.
  ``--results-table`` stores each repeat's summary and records in UC.

::

    uv run --frozen python tests/eval/run_agent_ontology_evolution.py
    uv run --frozen python tests/eval/run_agent_ontology_evolution.py --deterministic
    uv run --frozen python tests/eval/run_agent_ontology_evolution.py --live \
        --endpoint databricks-claude-sonnet-5-5 --repeats 3
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
for extra in (ROOT / "src", ROOT / "tests" / "eval"):
    if str(extra) not in sys.path:
        sys.path.insert(0, str(extra))

from judges.ontology_evolution import (  # noqa: E402
    judge_abstention,
    judge_decision,
    judge_false_ready,
    judge_gap,
    judge_intent,
    judge_ops,
    judge_ready,
    judge_states,
    score_dataset_row,
)

DATASET = ROOT / "tests/eval/datasets/agent_ontology_evolution/baseline.jsonl"
MIN_ROWS = 20


def load_rows(path: Path = DATASET) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def dry_run(rows: list[dict]) -> int:
    scores = [score_dataset_row(r) for r in rows]
    bad = [r.get("id") for r, s in zip(rows, scores) if s < 1.0]
    holdout = sum("holdout" in r["tags"] for r in rows)
    print(
        f"ontology_evolution schema_valid={sum(scores) / len(scores):.3f} "
        f"n={len(rows)} holdout={holdout}"
    )
    if bad:
        print(f"invalid rows: {bad}")
        return 1
    return 0


def _threshold(name: str) -> float:
    import yaml

    data = yaml.safe_load(
        (ROOT / "tests/eval/thresholds.yaml").read_text(encoding="utf-8")
    )
    return float(data["ontology_evolution"][name])


def deterministic(rows: list[dict]) -> int:
    """Score coverage, gap, ops, validation and decision offline."""
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from tests.eval.evolution_offline import observe

    judges = {
        "ready": judge_ready,
        "states": judge_states,
        "gap_correct": judge_gap,
        "ops": judge_ops,
        "decision": judge_decision,
        "abstention": judge_abstention,
        "false_ready": judge_false_ready,
    }
    totals = {name: 0.0 for name in judges}
    totals["validation_failure"] = 0.0
    failures = []
    for row in rows:
        observed = observe(row)
        scores = {name: fn(row, observed) for name, fn in judges.items()}
        expected_failure = row["expected"].get("validation_failure")
        scores["validation_failure"] = (
            1.0
            if not expected_failure
            or expected_failure in observed["validation_failures"]
            else 0.0
        )
        for name, value in scores.items():
            totals[name] += value
        bad = [
            n for n, v in scores.items() if (v < 1.0 if n != "false_ready" else v > 0.0)
        ]
        if bad:
            failures.append((row["id"], bad, observed))
    n = len(rows)
    means = {name: total / n for name, total in totals.items()}
    print(
        "ontology_evolution deterministic "
        + " ".join(f"{k}={v:.3f}" for k, v in means.items())
        + f" n={n}"
    )
    for row_id, bad, observed in failures:
        print(
            f"  FAIL {row_id}: {bad} observed={json.dumps(observed, default=str)[:400]}"
        )
    gates = (
        means["gap_correct"] >= _threshold("gap_correct")
        and means["false_ready"] <= _threshold("false_ready")
        and means["abstention"] >= _threshold("abstention")
    )
    return 0 if gates and not failures else 1


def live(endpoint: str, split: str, repeats: int, table: str) -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from tests.eval.evolution_agent_live import run, store

    report = run(endpoint, "dev" if split == "all" else split, repeats, judge_intent)
    if table:
        store(report, table)
    means = {
        k: sum(r["summary"][k] for r in report["repeats"]) / len(report["repeats"])
        for k in (
            "intent_accuracy_baseline",
            "intent_accuracy_pages_with",
            "proposal_valid",
        )
    }
    print(
        "ontology_evolution live " + " ".join(f"{k}={v:.3f}" for k, v in means.items())
    )
    gates = (
        means["intent_accuracy_pages_with"] >= _threshold("intent_accuracy")
        and means["intent_accuracy_baseline"] >= _threshold("intent_accuracy")
        and means["proposal_valid"] >= _threshold("proposal_valid")
    )
    return 0 if gates else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--deterministic", action="store_true")
    mode.add_argument("--live", action="store_true")
    parser.add_argument("--split", choices=("dev", "holdout", "all"), default="all")
    parser.add_argument("--endpoint", default="databricks-claude-sonnet-5-5")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--results-table", default="")
    args = parser.parse_args()

    if args.live:
        return live(args.endpoint, args.split, args.repeats, args.results_table)
    rows = load_rows()
    if len(rows) < MIN_ROWS:
        raise SystemExit(f"{DATASET} has {len(rows)} rows; need >= {MIN_ROWS}")
    if args.split != "all":
        rows = [r for r in rows if args.split in r["tags"]]

    if args.deterministic:
        return deterministic(rows)
    return dry_run(rows)


if __name__ == "__main__":
    sys.exit(main())
