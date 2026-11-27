"""Eval runner for agent_complex_rule_builder.

Dry run (default, used by CI) checks the dataset contract without calling the
LLM: every ``expected.reference`` compiles for its kind and the ideal output
built from each row scores 1.0 with the judges::

    uv run --frozen python tests/eval/run_agent_complex_rule_builder.py

Live run calls the agent once per row against a serving endpoint, scores each
turn with ``tests/eval/judges/complex_rule_builder.py`` and logs the run to
MLflow (the run URI is printed — paste it into the PR)::

    uv run --frozen python tests/eval/run_agent_complex_rule_builder.py --live \\
        --endpoint us.anthropic.claude-sonnet-5 --run-name baseline
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[2]
for extra in (ROOT / "src", ROOT / "tests" / "eval"):
    if str(extra) not in sys.path:
        sys.path.insert(0, str(extra))

from back.core.reasoning.rules.rule_kind import sparql_rule_kind  # noqa: E402
from judges.complex_rule_builder import (  # noqa: E402
    ONTOLOGIES,
    WEIGHTS,
    query_errors,
    reference_output,
    score_row,
)

DATASET = ROOT / "tests/eval/datasets/agent_complex_rule_builder/baseline.jsonl"
REGRESSION = ROOT / "tests/eval/datasets/agent_complex_rule_builder/regression.jsonl"
THRESHOLDS = ROOT / "tests/eval/thresholds.yaml"
AGENT = "complex_rule_builder"
MIN_ROWS = 10


def load_rows(path: Path = DATASET) -> List[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def load_dataset(name: str) -> List[dict]:
    """``baseline``, ``regression`` or ``all`` (both files)."""
    if name == "all":
        return load_rows(DATASET) + load_rows(REGRESSION)
    return load_rows(REGRESSION if name == "regression" else DATASET)


def threshold() -> float:
    import yaml

    data = yaml.safe_load(THRESHOLDS.read_text(encoding="utf-8"))
    return float(data[AGENT]["aggregate"])


def contract_errors(rows: List[dict]) -> List[str]:
    """Dataset problems that would make a live score meaningless."""
    errors: List[str] = []
    if len(load_rows(DATASET)) < MIN_ROWS:
        errors.append(f"baseline has {len(load_rows(DATASET))} rows; need >= {MIN_ROWS}")
    for row in rows:
        rid = row.get("id", "?")
        inp, exp = row.get("input") or {}, row.get("expected") or {}
        if inp.get("ontology") not in ONTOLOGIES:
            errors.append(f"{rid}: unknown ontology fixture {inp.get('ontology')!r}")
        if not inp.get("user_message"):
            errors.append(f"{rid}: missing user_message")
        if exp.get("draft") and not exp.get("reference"):
            errors.append(f"{rid}: a row expecting a draft needs a reference query")
        if exp.get("reference"):
            for err in query_errors(exp["reference"]):
                errors.append(f"{rid}: reference does not compile: {err}")
            form = sparql_rule_kind(exp["reference"])
            if exp.get("kind") and form != exp["kind"]:
                errors.append(f"{rid}: reference is a {form}, expected {exp['kind']}")
        if score_row(reference_output(row), exp)["weighted"] < 1.0:
            errors.append(f"{rid}: the ideal output does not score 1.0")
    return errors


def _agent_turn(row: dict, host: str, token: str, endpoint: str, endpoint_kind: str) -> Dict[str, Any]:
    from agents.agent_complex_rule_builder import run_agent

    inp = row["input"]
    onto = ONTOLOGIES[inp["ontology"]]
    result = run_agent(
        host=host,
        token=token,
        endpoint_name=endpoint,
        classes=onto["classes"],
        properties=onto["properties"],
        base_uri=onto["base_uri"],
        user_message=inp["user_message"],
        current_draft=inp.get("current_draft"),
        conversation_history=inp.get("history"),
        endpoint_kind=endpoint_kind,
    )
    return {
        "success": result.success,
        "error": result.error,
        "reply": result.reply,
        "draft": result.draft,
        "ready": result.ready,
        "options": result.options,
        "tools": [s.tool_name for s in result.steps if s.step_type == "tool_call"],
        "usage": result.usage,
    }


def run_live(args) -> float:
    rows = load_dataset(args.dataset)
    results: List[Dict[str, Any]] = []
    for row in rows:
        t0 = time.time()
        output = _agent_turn(row, args.host, args.token, args.endpoint, args.endpoint_kind)
        elapsed = time.time() - t0
        scores = score_row(output, row["expected"])
        results.append({"id": row["id"], "tags": row.get("tags", []), "latency_s": elapsed,
                        "scores": scores, "output": output})
        dims = " ".join(f"{k}={v:.2f}" for k, v in scores.items() if v is not None and k != "weighted")
        print(f"  {row['id']:<45} weighted={scores['weighted']:.2f}  {dims}  ({elapsed:.1f}s)")

    transport = [r["id"] for r in results if "LLM request failed" in (r["output"].get("error") or "")]
    if transport:
        # An unreachable endpoint says nothing about the agent: do not score or log it.
        raise SystemExit(f"LLM unreachable on {len(transport)} row(s), e.g. {transport[0]}; nothing logged.")

    aggregate = sum(r["scores"]["weighted"] for r in results) / len(results)
    averages = {}
    for dim in WEIGHTS:
        vals = [r["scores"][dim] for r in results if r["scores"][dim] is not None]
        averages[dim] = sum(vals) / len(vals) if vals else 0.0
    latencies = sorted(r["latency_s"] for r in results)
    p95 = latencies[max(0, int(round(0.95 * len(latencies))) - 1)]
    print(f"\naggregate={aggregate:.3f} threshold={threshold():.3f} latency_p95={p95:.1f}s")
    print("  " + "  ".join(f"{k}={v:.2f}" for k, v in averages.items()))

    report = ROOT / f".planning/complex-rule-builder-removal/eval-{args.run_name}.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    print(f"report: {report}")
    _log_mlflow(args, aggregate, averages, p95, report)
    return aggregate


def _log_mlflow(args, aggregate: float, averages: Dict[str, float], p95: float, report: Path) -> None:
    if not args.mlflow_experiment:
        return
    try:
        import mlflow

        mlflow.set_tracking_uri(args.mlflow_tracking_uri)
        experiment = mlflow.set_experiment(args.mlflow_experiment)
        with mlflow.start_run(run_name=args.run_name) as run:
            mlflow.log_params({"endpoint": args.endpoint, "dataset": args.dataset,
                               "dataset_rows": len(load_dataset(args.dataset))})
            mlflow.log_metric("aggregate_score", aggregate)
            mlflow.log_metric("latency_p95_seconds", p95)
            for dim, val in averages.items():
                mlflow.log_metric(f"avg_{dim}", val)
            mlflow.log_artifact(str(DATASET))
            if REGRESSION.exists():
                mlflow.log_artifact(str(REGRESSION))
            mlflow.log_artifact(str(report))
            host = (args.host or "").rstrip("/")
            print(f"MLflow run: {host}/ml/experiments/{experiment.experiment_id}/runs/{run.info.run_id}")
    except Exception as exc:  # noqa: BLE001 — logging must not hide the score
        print(f"[warn] MLflow logging skipped: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Eval harness — agent_complex_rule_builder")
    parser.add_argument("--live", action="store_true", help="Call the agent (needs host/token/endpoint).")
    parser.add_argument("--endpoint", default=os.getenv("ONTOBRICKS_LLM_ENDPOINT", ""))
    parser.add_argument("--endpoint-kind", default="")
    parser.add_argument("--host", default=os.getenv("DATABRICKS_HOST", ""))
    parser.add_argument("--token", default=os.getenv("DATABRICKS_TOKEN", ""))
    parser.add_argument("--run-name", default="eval")
    parser.add_argument("--dataset", choices=("baseline", "regression", "all"), default="all")
    parser.add_argument("--mlflow-experiment", default=f"/Shared/ontobricks/agents/{AGENT}")
    parser.add_argument("--mlflow-tracking-uri", default=os.getenv("MLFLOW_TRACKING_URI", "databricks"))
    args = parser.parse_args()

    rows = load_dataset("all")
    errors = contract_errors(rows)
    if errors:
        print("\n".join(errors))
        raise SystemExit(1)
    print(f"{AGENT}: dataset contract OK (n={len(rows)})")
    if not args.live:
        return
    if not (args.host and args.token and args.endpoint):
        parser.error("--live needs --host, --token and --endpoint (or the env vars).")
    aggregate = run_live(args)
    if aggregate < threshold():
        raise SystemExit(1)


if __name__ == "__main__":
    main()
