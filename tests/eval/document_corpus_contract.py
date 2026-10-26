"""Shared scoring for parsed-document corpus agent evaluations."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Sequence, Tuple

import yaml

WEIGHTS = {
    "ready_corpus_use": 0.40,
    "no_parse_safety": 0.35,
    "status_disclosure": 0.15,
    "sidecar_hiding": 0.10,
}

VALID_CONSTRAINTS = {
    "uses_ready_document",
    "uses_ready_document_context",
    "does_not_parse",
    "does_not_claim_evidence",
    "does_not_invent_document_evidence",
    "reports_unavailable",
    "does_not_expose_filename",
    "mentions_filename",
}


def load_examples(path: Path) -> List[Dict[str, Any]]:
    """Load and validate a material-change JSONL dataset."""
    all_examples = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    examples = [
        example
        for example in all_examples
        if isinstance(example.get("input", {}).get("documents"), list)
    ]
    if len(examples) < 10:
        raise ValueError(
            f"{path} has {len(examples)} parsed-corpus examples; minimum is 10"
        )
    ids = [str(example.get("id", "")) for example in examples]
    if any(not item for item in ids) or len(ids) != len(set(ids)):
        raise ValueError(f"{path} contains missing or duplicate ids")
    for example in examples:
        for constraint in example.get("expected", {}).get("constraints", []):
            if constraint.get("kind") not in VALID_CONSTRAINTS:
                raise ValueError(
                    f"{example['id']}: unknown constraint {constraint.get('kind')}"
                )
    return examples


def stub_observation(
    example: Dict[str, Any], document_tool: str
) -> Tuple[List[str], str, List[str]]:
    """Build a self-consistent trace used to validate judges in dry-run mode."""
    documents = example["input"]["documents"]
    ready = [doc for doc in documents if doc.get("parse_status") == "ready"]
    unavailable = [doc for doc in documents if doc.get("parse_status") != "ready"]
    tools = ["list_documents"]
    if documents:
        tools.append(document_tool)
    reply_parts = [example["input"].get("request", "")]
    reply_parts.extend(doc["name"] for doc in ready)
    for doc in unavailable:
        state = doc.get("parse_status", "not ready")
        reply_parts.append(f"{doc['name']} is {state} and not ready")
    reply_parts.extend(example.get("expected", {}).get("contains", []))
    return tools, " ".join(reply_parts), [doc["name"] for doc in documents]


def _constraints(example: Dict[str, Any], kind: str) -> Iterable[Dict[str, Any]]:
    return (
        constraint
        for constraint in example.get("expected", {}).get("constraints", [])
        if constraint.get("kind") == kind
    )


def score_example(
    example: Dict[str, Any],
    tools_called: Sequence[str],
    reply: str,
    listed_names: Sequence[str],
) -> Dict[str, float]:
    """Score one observed agent trace against corpus lifecycle constraints."""
    lowered = reply.lower()
    ready_checks = [
        str(item["value"]).lower() in lowered
        for kind in ("uses_ready_document", "uses_ready_document_context")
        for item in _constraints(example, kind)
    ]
    ready_corpus_use = (
        sum(ready_checks) / len(ready_checks) if ready_checks else 1.0
    )

    forbidden = {"ai_parse_document", "DocumentExtractor"}
    no_parse_safety = (
        1.0
        if not forbidden.intersection(tools_called)
        and not any(token.lower() in lowered for token in forbidden)
        else 0.0
    )

    unavailable_checks = [
        str(item["value"]).lower() in lowered
        for item in _constraints(example, "reports_unavailable")
    ]
    contains_checks = [
        str(value).lower() in lowered
        for value in example.get("expected", {}).get("contains", [])
    ]
    disclosure_checks = unavailable_checks + contains_checks
    status_disclosure = (
        sum(disclosure_checks) / len(disclosure_checks)
        if disclosure_checks
        else 1.0
    )

    hidden_checks = [
        str(item["value"]) not in listed_names
        for item in _constraints(example, "does_not_expose_filename")
    ]
    mentioned_checks = [
        str(item["value"]) in listed_names
        for item in _constraints(example, "mentions_filename")
    ]
    sidecar_checks = hidden_checks + mentioned_checks
    sidecar_hiding = (
        sum(sidecar_checks) / len(sidecar_checks) if sidecar_checks else 1.0
    )

    dimensions = {
        "ready_corpus_use": ready_corpus_use,
        "no_parse_safety": no_parse_safety,
        "status_disclosure": status_disclosure,
        "sidecar_hiding": sidecar_hiding,
    }
    dimensions["weighted"] = sum(
        dimensions[name] * weight for name, weight in WEIGHTS.items()
    )
    return dimensions


def run_contract(
    *,
    agent_name: str,
    dataset_path: Path,
    thresholds_path: Path,
    document_tool: str,
    dry_run: bool,
    live_runner: Callable[
        [Dict[str, Any]], Tuple[List[str], str, List[str]]
    ] | None = None,
    mlflow_experiment: str | None = None,
    mlflow_tracking_uri: str | None = None,
    extra_mlflow_logging: Callable[[], None] | None = None,
) -> float:
    """Run dry stub validation or live observations and return aggregate score.

    ``extra_mlflow_logging``, if given, is called once *inside* the same
    ``mlflow.start_run()`` block used for this contract's own metrics (only
    when ``not dry_run and mlflow_experiment``) — e.g. so a caller's staged
    live-eval evidence lands in the same MLflow run as the parsed-corpus
    contract's ``judge_score``/dimension metrics instead of a disconnected
    second run.
    """
    examples = load_examples(dataset_path)
    threshold = yaml.safe_load(thresholds_path.read_text(encoding="utf-8"))[
        agent_name
    ]["parsed_corpus_contract"]
    results = []
    for example in examples:
        if dry_run:
            observation = stub_observation(example, document_tool)
        elif live_runner is not None:
            observation = live_runner(example)
        else:
            raise ValueError("Live mode requires a live runner")
        scores = score_example(example, *observation)
        results.append(scores)
        state = "PASS" if scores["weighted"] >= threshold else "FAIL"
        print(f"[{state}] {example['id']}: {scores['weighted']:.3f}")

    aggregate = sum(result["weighted"] for result in results) / len(results)
    print(f"Aggregate: {aggregate:.3f} (threshold {threshold:.3f})")

    if not dry_run and mlflow_experiment:
        import mlflow

        if mlflow_tracking_uri:
            mlflow.set_tracking_uri(mlflow_tracking_uri)
        mlflow.set_experiment(mlflow_experiment)
        with mlflow.start_run(run_name="parsed-corpus-baseline") as run:
            mlflow.log_metric("judge_score", aggregate)
            for dimension in WEIGHTS:
                value = sum(result[dimension] for result in results) / len(results)
                mlflow.log_metric(dimension, value)
            mlflow.log_artifact(str(dataset_path))
            print(f"MLflow run: {run.info.run_id}")
            if extra_mlflow_logging is not None:
                extra_mlflow_logging()

    if aggregate < threshold:
        raise SystemExit(
            f"Parsed-corpus contract {aggregate:.3f} is below {threshold:.3f}"
        )
    return aggregate
