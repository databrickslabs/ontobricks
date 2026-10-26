"""Parsed-document corpus contract evaluation for agent_mapping_pge."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))
if str(ROOT / "tests" / "eval") not in sys.path:
    sys.path.insert(0, str(ROOT / "tests" / "eval"))

from document_corpus_contract import run_contract  # noqa: E402

DATASET = ROOT / "tests/eval/datasets/agent_mapping_pge/baseline.jsonl"
THRESHOLDS = ROOT / "tests/eval/thresholds.yaml"


class _NoDataClient:
    """SQL client used by corpus-tool evals that require no source queries."""

    def execute_query(self, _query: str):
        return []


def _live_runner(
    example: Dict[str, Any], *, host: str, token: str, endpoint: str
) -> Tuple[List[str], str, List[str]]:
    from agents.agent_mapping_pge.planner import run_planner

    documents = example["input"]["documents"]
    result = run_planner(
        host=host,
        token=token,
        endpoint_name=endpoint,
        client=_NoDataClient(),
        metadata={"tables": []},
        ontology={"entities": [], "relationships": []},
        documents=documents,
        max_iterations=6,
    )
    tools_called = [
        step.tool_name
        for step in result.steps
        if step.step_type == "tool_call" and step.tool_name
    ]
    observed_text = " ".join(step.content for step in result.steps)
    return tools_called, observed_text, [doc["name"] for doc in documents]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--host", default=os.getenv("DATABRICKS_HOST"))
    parser.add_argument("--token", default=os.getenv("DATABRICKS_TOKEN"))
    parser.add_argument("--endpoint", default=os.getenv("ONTOBRICKS_LLM_ENDPOINT"))
    parser.add_argument(
        "--mlflow-experiment",
        default="/Shared/ontobricks/agents/mapping_pge",
    )
    parser.add_argument(
        "--mlflow-tracking-uri",
        default=os.getenv("MLFLOW_TRACKING_URI", "databricks"),
    )
    args = parser.parse_args()
    if args.live and not (args.host and args.token and args.endpoint):
        parser.error("--live requires host, token, and endpoint")

    live = None
    if args.live:
        live = lambda example: _live_runner(  # noqa: E731
            example, host=args.host, token=args.token, endpoint=args.endpoint
        )
    run_contract(
        agent_name="mapping_pge",
        dataset_path=DATASET,
        thresholds_path=THRESHOLDS,
        document_tool="get_documents_context",
        dry_run=not args.live,
        live_runner=live,
        mlflow_experiment=args.mlflow_experiment,
        mlflow_tracking_uri=args.mlflow_tracking_uri,
    )


if __name__ == "__main__":
    main()
