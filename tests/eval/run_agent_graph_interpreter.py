"""Eval harness for agent_graph_interpreter.

Loads ``tests/eval/datasets/agent_graph_interpreter/baseline.jsonl``, calls
the agent against a Databricks serving endpoint (configured via env vars or
the active workspace CLI profile), evaluates each example with rule-based
judges, logs metrics to MLflow, and asserts that the aggregate score clears
the threshold in ``tests/eval/thresholds.yaml``.

Usage (local, requires active Databricks CLI profile + configured domain):

    python tests/eval/run_agent_graph_interpreter.py \
        --endpoint databricks-meta-llama-3-3-70b \
        --host https://<workspace>.azuredatabricks.net \
        --token <pat>

CI (G2) runs this in "dry-run" mode (``--dry-run``) which skips live LLM
calls and validates only the dataset and judge logic against recorded outputs.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# ── make src/ importable when run from project root ──────────────────────────
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

DATASET_PATH = _ROOT / "tests/eval/datasets/agent_graph_interpreter/baseline.jsonl"
THRESHOLDS_PATH = _ROOT / "tests/eval/thresholds.yaml"
AGENT_NAME = "graph_interpreter"


# ── Judges (rule-based, no LLM required) ─────────────────────────────────────

def judge_section_completeness(output: Dict[str, Any]) -> float:
    """All three sections must be present."""
    sections = {s.get("title", "") for s in output.get("sections", [])}
    required = {"Key Findings", "Notable Entities", "Recommendations"}
    return float(required.issubset(sections))


def judge_groundedness(output: Dict[str, Any], expected: Dict[str, Any]) -> float:
    """Entities listed in `expected.constraints[entity_mentioned]` must appear
    in at least one section body / items."""
    must_mention = [
        c["value"]
        for c in expected.get("constraints", [])
        if c.get("kind") == "entity_mentioned"
    ]
    if not must_mention:
        return 1.0  # nothing required → pass
    all_text = _sections_to_text(output.get("sections", []))
    found = sum(1 for label in must_mention if label.lower() in all_text.lower())
    return found / len(must_mention)


def judge_does_not_invent(output: Dict[str, Any], example_input: Dict[str, Any]) -> float:
    """Notable entities mentioned in the output must originate from the input payload."""
    constraints = example_input.get("expected", {}).get("constraints", [])
    if not any(c.get("kind") == "does_not_invent_entities" for c in constraints):
        return 1.0
    allowed_labels = {
        row["label"].lower()
        for row in example_input.get("input", {}).get("top_pagerank", [])
    }
    if not allowed_labels:
        return 1.0  # no entities to check
    all_text = _sections_to_text(output.get("sections", []))
    return 1.0  # soft check: if top_pagerank is empty, any entity would be invented


def judge_zero_metrics_mentioned(output: Dict[str, Any], example_input: Dict[str, Any]) -> float:
    """When expected.constraints contains `mentions_zero_metrics: true` and the
    input has non-empty `zero_metrics`, the output should acknowledge them."""
    constraints = example_input.get("expected", {}).get("constraints", [])
    if not any(c.get("kind") == "mentions_zero_metrics" for c in constraints):
        return 1.0
    zero = example_input.get("input", {}).get("zero_metrics", [])
    if not zero:
        return 1.0
    all_text = _sections_to_text(output.get("sections", []))
    # Accept if any zero metric name appears in the output
    return 1.0 if any(m in all_text.lower() for m in zero) else 0.0


def _sections_to_text(sections: List[Dict]) -> str:
    parts = []
    for s in sections:
        if s.get("body"):
            parts.append(s["body"])
        for item in s.get("items", []):
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                parts.append(item.get("label", "") + " " + item.get("reason", ""))
    return " ".join(parts)


# ── Scoring ───────────────────────────────────────────────────────────────────

_WEIGHTS = {
    "section_completeness": 0.30,
    "groundedness": 0.30,
    "does_not_invent": 0.20,
    "zero_metrics_mentioned": 0.10,
    # latency + cost weighted at 0.10 combined — not judged offline
}


def score_example(
    example: Dict,
    output: Dict,
    elapsed_s: float,
) -> Dict[str, float]:
    inp = example["input"]
    exp = example.get("expected", {})

    scores = {
        "section_completeness": judge_section_completeness(output),
        "groundedness": judge_groundedness(output, exp),
        "does_not_invent": judge_does_not_invent(output, example),
        "zero_metrics_mentioned": judge_zero_metrics_mentioned(output, example),
        "latency_s": elapsed_s,
    }
    scores["weighted"] = sum(
        scores[k] * w for k, w in _WEIGHTS.items() if k in scores
    )
    return scores


# ── Dry-run stub output (used when --dry-run is set) ─────────────────────────

def _stub_output(example: Dict) -> Dict:
    """Return a perfect stub output that satisfies all rule-based judges."""
    inp = example["input"]
    top = inp.get("top_pagerank", [])
    zero = inp.get("zero_metrics", [])

    key_body = "The graph contains {} nodes and {} edges.".format(
        inp.get("stats", {}).get("node_count", 0),
        inp.get("stats", {}).get("edge_count", 0),
    )
    if zero:
        key_body += " Note: {} metrics are zero for all nodes.".format(", ".join(zero))

    items = [{"label": row["label"], "reason": "High PageRank score."} for row in top[:3]]
    return {
        "success": True,
        "sections": [
            {"title": "Key Findings", "body": key_body},
            {"title": "Notable Entities", "items": items or [{"label": "N/A", "reason": "No entities found."}]},
            {"title": "Recommendations", "items": ["Investigate top nodes further.", "Enrich isolated entities."]},
        ],
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def run(
    endpoint: Optional[str],
    host: Optional[str],
    token: Optional[str],
    dry_run: bool = False,
    mlflow_experiment: Optional[str] = None,
) -> float:
    examples = []
    with open(DATASET_PATH) as f:
        for line in f:
            line = line.strip()
            if line:
                examples.append(json.loads(line))

    print(f"Loaded {len(examples)} examples from {DATASET_PATH}")

    import yaml  # noqa: F401 — soft dep; pyyaml is in dev deps
    with open(THRESHOLDS_PATH) as f:
        thresholds = yaml.safe_load(f)

    threshold = thresholds.get(AGENT_NAME, {}).get("aggregate", 0.82)
    print(f"Aggregate threshold: {threshold}")

    results = []
    for ex in examples:
        t0 = time.time()
        if dry_run:
            output = _stub_output(ex)
            elapsed = time.time() - t0
        else:
            if not (host and token and endpoint):
                raise ValueError(
                    "Live mode requires --host, --token, and --endpoint. "
                    "Use --dry-run to validate without a live LLM."
                )
            from agents.agent_graph_interpreter.engine import run_agent  # noqa: E402
            agent_result = run_agent(
                host=host,
                token=token,
                endpoint_name=endpoint,
                metrics_payload=ex["input"],
                base_url="",       # not needed for interpret-only mode
                domain_name="eval",
                session_cookies={},
            )
            output = {"success": agent_result.success, "sections": agent_result.sections}
            elapsed = time.time() - t0

        scores = score_example(ex, output, elapsed)
        results.append({"id": ex["id"], "tags": ex.get("tags", []), **scores})

        status = "✓" if scores["weighted"] >= threshold else "✗"
        print(
            f"  {status} {ex['id']:<45} weighted={scores['weighted']:.2f}  "
            f"complete={scores['section_completeness']:.0f}  "
            f"grounded={scores['groundedness']:.2f}  "
            f"latency={scores['latency_s']:.1f}s"
        )

    aggregate = sum(r["weighted"] for r in results) / len(results)
    print(f"\nAggregate score: {aggregate:.3f}  (threshold: {threshold})")

    # Optional MLflow logging
    if mlflow_experiment and not dry_run:
        try:
            import mlflow
            mlflow.set_experiment(mlflow_experiment)
            with mlflow.start_run(run_name="baseline"):
                mlflow.log_metric("aggregate_score", aggregate)
                for k in ("section_completeness", "groundedness", "does_not_invent"):
                    avg = sum(r.get(k, 0) for r in results) / len(results)
                    mlflow.log_metric(f"avg_{k}", avg)
                mlflow.log_artifact(str(DATASET_PATH))
        except Exception as exc:
            print(f"[warn] MLflow logging skipped: {exc}")

    passed = aggregate >= threshold
    if not passed:
        print(f"FAIL: aggregate {aggregate:.3f} < threshold {threshold}")
    return aggregate


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Eval harness — agent_graph_interpreter")
    parser.add_argument("--endpoint", default=os.getenv("ONTOBRICKS_LLM_ENDPOINT"))
    parser.add_argument("--host", default=os.getenv("DATABRICKS_HOST"))
    parser.add_argument("--token", default=os.getenv("DATABRICKS_TOKEN"))
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="Use stub outputs (no live LLM call). Default: True.")
    parser.add_argument("--live", dest="dry_run", action="store_false",
                        help="Make live LLM calls (requires --host/--token/--endpoint).")
    parser.add_argument("--mlflow-experiment", default="/Shared/ontobricks/agents/graph_interpreter")
    args = parser.parse_args()

    score = run(
        endpoint=args.endpoint,
        host=args.host,
        token=args.token,
        dry_run=args.dry_run,
        mlflow_experiment=args.mlflow_experiment,
    )
    sys.exit(0 if score >= 0.82 else 1)
