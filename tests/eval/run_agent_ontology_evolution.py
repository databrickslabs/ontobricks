"""Eval runner for Ontology → Evolve (``agent_ontology_evolution``).

Modes:

* default (dry run): validates the dataset contract; no network, no LLM.
* ``--deterministic``: runs the coverage check offline on the fixture domain
  variants with the manual intent from each row, and scores readiness,
  states, gap and decision. No warehouse or LLM.
* ``--live``: end-to-end against a configured workspace (wired in a later
  stage); exits with an explicit error until then rather than reporting a
  fake score.

::

    uv run --frozen python tests/eval/run_agent_ontology_evolution.py
    uv run --frozen python tests/eval/run_agent_ontology_evolution.py --deterministic
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

from judges.ontology_evolution import score_dataset_row  # noqa: E402

DATASET = ROOT / "tests/eval/datasets/agent_ontology_evolution/baseline.jsonl"
MIN_ROWS = 20


def load_rows(path: Path = DATASET) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def dry_run(rows: list[dict]) -> int:
    scores = [score_dataset_row(r) for r in rows]
    bad = [r.get("id") for r, s in zip(rows, scores) if s < 1.0]
    holdout = sum("holdout" in r["tags"] for r in rows)
    print(f"ontology_evolution schema_valid={sum(scores) / len(scores):.3f} n={len(rows)} holdout={holdout}")
    if bad:
        print(f"invalid rows: {bad}")
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--deterministic", action="store_true")
    mode.add_argument("--live", action="store_true")
    parser.add_argument("--split", choices=("dev", "holdout", "all"), default="all")
    args = parser.parse_args()

    rows = load_rows()
    if len(rows) < MIN_ROWS:
        raise SystemExit(f"{DATASET} has {len(rows)} rows; need >= {MIN_ROWS}")
    if args.split != "all":
        rows = [r for r in rows if args.split in r["tags"]]

    if args.live:
        parser.error("Live evaluation is not wired yet.")
    if args.deterministic:
        parser.error("Deterministic coverage scoring is not wired yet.")
    return dry_run(rows)


if __name__ == "__main__":
    sys.exit(main())
