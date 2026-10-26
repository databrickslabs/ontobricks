"""Metric-view SQL-generation contract eval for agent_mapping_pge.

Complements ``run_agent_mapping_pge.py`` (the parsed-document corpus contract).
This runner scores the SQL the EntityGenerator produces for a Unity Catalog
**metric view** against the metric-aware constraints in
``datasets/agent_mapping_pge/metric_view.jsonl`` — measures wrapped in
``MEASURE()``, a ``GROUP BY`` over dimensions, never ``SELECT *``, and the
entity id drawn from a dimension rather than a measure.

Dry run (default) — proves the judge accepts a correct reference answer::

    uv run --frozen python tests/eval/run_agent_mapping_pge_metric.py

Live run — scores the real agent against live metric views (needs a warehouse
where the dataset's metric views exist + Foundation Model access)::

    uv run --frozen python tests/eval/run_agent_mapping_pge_metric.py --live \\
        --host "$DATABRICKS_HOST" --token "$DATABRICKS_TOKEN" \\
        --endpoint "$ONTOBRICKS_LLM_ENDPOINT" --warehouse "$DATABRICKS_WAREHOUSE_ID" \\
        --run-name metric-view-baseline
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))
if str(ROOT / "tests" / "eval") not in sys.path:
    sys.path.insert(0, str(ROOT / "tests" / "eval"))

from metric_view_contract import run_contract  # noqa: E402

DATASET = ROOT / "tests/eval/datasets/agent_mapping_pge/metric_view.jsonl"
THRESHOLDS = ROOT / "tests/eval/thresholds.yaml"


def _threshold() -> float:
    import yaml

    data = yaml.safe_load(THRESHOLDS.read_text(encoding="utf-8"))
    return float(data["mapping_pge"]["metric_view_sql_correctness"])


def _source_model_slice(source: Dict[str, Any]) -> Dict[str, Any]:
    """Shape a dataset ``source`` into a single-candidate SourceModel slice."""
    return {
        "candidate_tables": [
            {
                "full_name": source.get("full_name"),
                "object_kind": source.get("object_kind"),
                "columns": source.get("columns", []),
            }
        ],
        "canonical_id": None,
        "relevant_joins": [],
    }


def _live_runner(
    example: Dict[str, Any], *, host: str, token: str, endpoint: str, client: Any
) -> Tuple[str, Optional[str]]:
    from agents.agent_mapping_pge.generators.entity import run_entity_generator

    inp = example["input"]
    result = run_entity_generator(
        host=host,
        token=token,
        endpoint_name=endpoint,
        client=client,
        ontology_class=inp["ontology_class"],
        source_model_slice=_source_model_slice(inp["source"]),
    )
    mapping = result.mapping or {}
    return mapping.get("sql_query", ""), mapping.get("id_column")


def _build_client(host: str, token: str, warehouse: Optional[str]) -> Any:
    from back.core.databricks.DatabricksClient import DatabricksClient

    return DatabricksClient(host=host, token=token, warehouse_id=warehouse)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--host", default=os.getenv("DATABRICKS_HOST"))
    parser.add_argument("--token", default=os.getenv("DATABRICKS_TOKEN"))
    parser.add_argument("--endpoint", default=os.getenv("ONTOBRICKS_LLM_ENDPOINT"))
    parser.add_argument("--warehouse", default=os.getenv("DATABRICKS_WAREHOUSE_ID"))
    parser.add_argument("--run-name", default="metric-view-baseline")
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
        client = _build_client(args.host, args.token, args.warehouse)
        live = lambda example: _live_runner(  # noqa: E731
            example,
            host=args.host,
            token=args.token,
            endpoint=args.endpoint,
            client=client,
        )

    run_contract(
        agent_name="mapping_pge",
        dataset_path=DATASET,
        threshold=_threshold(),
        dry_run=not args.live,
        live_runner=live,
        run_name=args.run_name,
        mlflow_experiment=args.mlflow_experiment,
        mlflow_tracking_uri=args.mlflow_tracking_uri,
    )


if __name__ == "__main__":
    main()
