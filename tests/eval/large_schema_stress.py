"""Large-schema stress harness for the staged ontology Generate flow (#173).

Issue #173 reports Generate failing on wide tables (164 / 360 / 400+
columns) and on 100+ tables. This harness runs the REAL production entry
points — ``staged.detect_entities`` followed by ``infer_relations`` ->
``infer_attributes`` -> ``infer_axioms``, chained through draft checkpoints
exactly like ``GenerateWorkflow.run_completion`` — on synthetic schemas of
those shapes, and reports per stage where (and why) a run stops.

The LLM call is only *observed*, never altered: every
``staged.call_serving_endpoint`` call is wrapped to record its stage, input
size, ``finish_reason`` and token usage. That answers the question the
offline contract cannot: on a real endpoint, which stage fails first, and
does the paged completion actually page?

Live (needs a Databricks Foundation Model endpoint)::

    uv run --frozen python tests/eval/large_schema_stress.py \\
        --host "$DATABRICKS_HOST" --token "$DATABRICKS_TOKEN" \\
        --endpoint databricks-claude-sonnet-4 [--scenario wide_table_360] \\
        [--mlflow-experiment /Shared/ontobricks/agents/owl_generator]

With ``--mlflow-experiment`` the per-scenario metrics and the full JSON
report are logged to one MLflow run whose URI is printed — the evidence
the AI-feature eval gate asks for. The host and token are never logged.

Offline, :func:`run_scenario` accepts an ``llm`` callable in place of the
real endpoint (see ``tests/eval/test_large_schema_stress.py``).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agents.agent_owl_generator import staged  # noqa: E402
from back.objects.ontology.generate.GenerateDraft import (  # noqa: E402
    CHECKPOINT_DONE,
    GenerateDraft,
)

# name -> (table count, columns per table). Shapes taken from #173.
SCENARIOS: Dict[str, tuple] = {
    "wide_table_164": (1, 164),
    "wide_table_360": (1, 360),
    "many_tables_100x18": (100, 18),
}

_ENTITY_BASES = [
    "customer",
    "order",
    "order_line",
    "invoice",
    "payment",
    "shipment",
    "carrier",
    "warehouse",
    "product",
    "supplier",
    "contract",
    "claim",
    "policy",
    "agent",
    "branch",
    "employee",
    "department",
    "account",
    "transaction",
    "vehicle",
    "route",
    "port",
    "container",
    "insurer",
    "region",
]
_TABLE_SUFFIXES = ["", "_history", "_snapshot", "_audit"]
_COLUMN_STEMS = [
    "code",
    "name",
    "status",
    "type",
    "category",
    "amount",
    "currency",
    "quantity",
    "weight",
    "volume",
    "start_date",
    "end_date",
    "created_at",
    "updated_at",
    "country",
    "city",
    "postal_code",
    "score",
    "rate",
    "flag",
]
_COLUMN_TYPES = [
    "string",
    "string",
    "string",
    "string",
    "string",
    "decimal(18,2)",
    "string",
    "int",
    "double",
    "double",
    "date",
    "date",
    "timestamp",
    "timestamp",
    "string",
    "string",
    "string",
    "double",
    "double",
    "boolean",
]

_SUBSTAGES = (
    ("relations", staged.infer_relations),
    ("attributes", staged.infer_attributes),
    ("axioms", staged.infer_axioms),
)


def _columns(table: str, count: int, peers: List[str]) -> List[Dict[str, str]]:
    """``count`` plausible columns: a key, FKs to peer tables, then
    ``<subject>_<stem>`` columns cycling through the entity vocabulary so a
    wide table reads like a real denormalised fact table."""
    cols = [{"name": f"{table}_id", "type": "bigint", "comment": "Primary key"}]
    cols += [
        {"name": f"{peer}_id", "type": "bigint", "comment": f"FK to {peer}"}
        for peer in peers
    ]
    i = 0
    while len(cols) < count:
        subject = _ENTITY_BASES[(i // len(_COLUMN_STEMS)) % len(_ENTITY_BASES)]
        stem_idx = i % len(_COLUMN_STEMS)
        name = f"{subject}_{_COLUMN_STEMS[stem_idx]}"
        if i >= len(_ENTITY_BASES) * len(_COLUMN_STEMS):
            name = f"{name}_{i // (len(_ENTITY_BASES) * len(_COLUMN_STEMS))}"
        cols.append({"name": name, "type": _COLUMN_TYPES[stem_idx], "comment": ""})
        i += 1
    return cols[:count]


def synthetic_metadata(n_tables: int, n_cols: int) -> Dict[str, Any]:
    """Domain metadata in the shape ``agents.tools.metadata`` reads."""
    names = [f"{base}{suffix}" for suffix in _TABLE_SUFFIXES for base in _ENTITY_BASES][
        :n_tables
    ]
    if n_tables == 1:
        names = ["shipment_fact"]
    tables = []
    for idx, name in enumerate(names):
        peers = (
            [names[(idx + k) % len(names)] for k in (1, 2)] if len(names) > 2 else []
        )
        tables.append(
            {
                "name": name,
                "full_name": f"main.stress.{name}",
                "comment": f"Synthetic {name} table ({n_cols} columns)",
                "columns": _columns(name, n_cols, peers),
            }
        )
    return {"tables": tables}


class _CallRecorder:
    """Wraps ``call_serving_endpoint``: forwards every call unchanged and
    records, per ``trace_name`` stage, the input size and the response's
    ``finish_reason`` / usage."""

    def __init__(self, target: Callable[..., Dict[str, Any]]):
        self._target = target
        self.calls: List[Dict[str, Any]] = []

    def __call__(self, host, token, endpoint_name, messages, **kwargs):
        input_chars = sum(len(str(m.get("content") or "")) for m in messages)
        resp = self._target(host, token, endpoint_name, messages, **kwargs)
        choice = (resp.get("choices") or [{}])[0]
        usage = resp.get("usage") or {}
        self.calls.append(
            {
                "stage": str(kwargs.get("trace_name", "")).rsplit(".", 1)[-1],
                "input_chars": input_chars,
                "messages": len(messages),
                "finish_reason": choice.get("finish_reason"),
                "prompt_tokens": int(usage.get("prompt_tokens") or 0),
                "completion_tokens": int(usage.get("completion_tokens") or 0),
            }
        )
        return resp

    def stage_stats(self, stage: str) -> Dict[str, int]:
        calls = [c for c in self.calls if c["stage"] == stage]
        return {
            "llm_calls": len(calls),
            "truncated_calls": sum(c["finish_reason"] == "length" for c in calls),
            "max_input_chars": max((c["input_chars"] for c in calls), default=0),
            "max_messages": max((c["messages"] for c in calls), default=0),
            "prompt_tokens": sum(c["prompt_tokens"] for c in calls),
            "completion_tokens": sum(c["completion_tokens"] for c in calls),
        }


def detection_visibility(metadata: Dict[str, Any]) -> Dict[str, int]:
    """What Stage-1 detection's ``get_metadata`` overview actually shows the
    model for this schema (deterministic — the real tool, no LLM): it caps
    tables and columns per table, so the rest is only reachable if the
    model chooses to call ``get_table_detail``."""
    from agents.tools.context import ToolContext
    from agents.tools.metadata import tool_get_metadata

    raw = tool_get_metadata(ToolContext(host="", token="", metadata=metadata))
    shown = json.loads(raw).get("tables") or []
    tables = metadata.get("tables") or []
    return {
        "tables_total": len(tables),
        "tables_visible": len(shown),
        "columns_total": sum(len(t.get("columns") or []) for t in tables),
        "columns_visible": sum(int(t.get("column_count") or 0) for t in shown),
        "metadata_tool_chars": len(raw),
    }


def _failure(report: Dict[str, Any], stage: str, result: Any) -> Dict[str, Any]:
    report.update(
        stage_reached=stage,
        success=False,
        rejected=bool(getattr(result, "rejected", False)),
        error=str(getattr(result, "error", ""))[:500],
        user_message=str(getattr(result, "user_message", ""))[:500],
    )
    return report


def run_scenario(
    name: str,
    *,
    host: str,
    token: str,
    endpoint: str,
    llm: Optional[Callable[..., Dict[str, Any]]] = None,
    max_candidates: int = 0,
) -> Dict[str, Any]:
    """Run detect -> relations -> attributes -> axioms for one scenario.

    Stops at the first stage that does not succeed (completion ordering is
    strict) and returns a JSON-serialisable report. ``llm`` replaces the
    real endpoint (offline tests); by default the real
    ``staged.call_serving_endpoint`` is called.
    """
    n_tables, n_cols = SCENARIOS[name]
    metadata = synthetic_metadata(n_tables, n_cols)
    recorder = _CallRecorder(llm or staged.call_serving_endpoint)
    report: Dict[str, Any] = {
        "scenario": name,
        "tables": n_tables,
        "columns_per_table": n_cols,
        "detect_visibility": detection_visibility(metadata),
        "stages": {},
    }
    options = {"max_classes": max_candidates} if max_candidates else None
    with patch.object(staged, "call_serving_endpoint", side_effect=recorder):
        detection = staged.detect_entities(
            host=host,
            token=token,
            endpoint_name=endpoint,
            metadata=metadata,
            guidelines="Build a domain ontology for this logistics/insurance schema.",
            options=options,
            selected_tables=[t["full_name"] for t in metadata["tables"]],
            registry={},
        )
        report["stages"]["detect"] = {
            **recorder.stage_stats("detect"),
            "items": len(detection.candidate_entities),
            "success": detection.success,
        }
        if not detection.success:
            return _failure(report, "detect", detection)

        draft = GenerateDraft.new(
            source_fingerprint=f"sha256:stress-{name}",
            existing_anchors=[],
            candidate_entities=detection.candidate_entities,
        )
        for substage, fn in _SUBSTAGES:
            result = fn(host=host, token=token, endpoint_name=endpoint, draft=draft)
            items = (result.result or {}).get(substage) or []
            report["stages"][substage] = {
                **recorder.stage_stats(substage),
                "items": len(items),
                "success": result.success,
            }
            if not result.success:
                return _failure(report, substage, result)
            draft = draft.with_checkpoint(
                substage, CHECKPOINT_DONE, result=result.result
            )

    report.update(
        stage_reached="done", success=True, rejected=False, error="", user_message=""
    )
    return report


def _print_report(report: Dict[str, Any]) -> None:
    state = "PASS" if report["success"] else f"FAIL at {report['stage_reached']}"
    print(f"\n[STRESS] {report['scenario']} — {state}")
    vis = report["detect_visibility"]
    print(
        f"  detect overview shows {vis['tables_visible']}/{vis['tables_total']} tables, "
        f"{vis['columns_visible']}/{vis['columns_total']} columns "
        f"({vis['metadata_tool_chars']} chars)"
    )
    for stage, stats in report["stages"].items():
        print(
            f"  {stage:<10} ok={stats['success']!s:<5} items={stats['items']:<4} "
            f"calls={stats['llm_calls']:<3} truncated={stats['truncated_calls']:<2} "
            f"max_input_chars={stats['max_input_chars']:<7} "
            f"tokens(in/out)={stats['prompt_tokens']}/{stats['completion_tokens']}"
        )
    if not report["success"]:
        print(f"  error: {report['error']}")
        if report["user_message"]:
            print(f"  user sees: {report['user_message']}")


def _log_mlflow(
    reports: List[Dict[str, Any]], experiment: str, tracking_uri: str
) -> str:
    import mlflow

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment)
    with mlflow.start_run(run_name="large_schema_stress_issue_173") as run:
        for report in reports:
            prefix = report["scenario"]
            mlflow.log_metric(f"{prefix}.success", 1.0 if report["success"] else 0.0)
            for key, value in report["detect_visibility"].items():
                mlflow.log_metric(f"{prefix}.detect_visibility.{key}", float(value))
            for stage, stats in report["stages"].items():
                for key in (
                    "items",
                    "llm_calls",
                    "truncated_calls",
                    "max_input_chars",
                    "prompt_tokens",
                    "completion_tokens",
                ):
                    mlflow.log_metric(f"{prefix}.{stage}.{key}", float(stats[key]))
        mlflow.log_dict({"reports": reports}, "large_schema_stress.json")
        return f"runs:/{run.info.run_id}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--host", default=os.getenv("DATABRICKS_HOST"))
    parser.add_argument("--token", default=os.getenv("DATABRICKS_TOKEN"))
    parser.add_argument("--endpoint", default=os.getenv("ONTOBRICKS_LLM_ENDPOINT"))
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), action="append")
    parser.add_argument("--max-candidates", type=int, default=0)
    parser.add_argument("--mlflow-experiment", default="")
    parser.add_argument(
        "--mlflow-tracking-uri", default=os.getenv("MLFLOW_TRACKING_URI", "databricks")
    )
    parser.add_argument("--json-out", default="")
    args = parser.parse_args()
    if not (args.host and args.token and args.endpoint):
        parser.error("requires --host, --token and --endpoint (or the env vars)")

    reports = []
    for name in args.scenario or sorted(SCENARIOS):
        report = run_scenario(
            name,
            host=args.host,
            token=args.token,
            endpoint=args.endpoint,
            max_candidates=args.max_candidates,
        )
        _print_report(report)
        reports.append(report)
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(reports, indent=2), encoding="utf-8")
    if args.mlflow_experiment:
        uri = _log_mlflow(reports, args.mlflow_experiment, args.mlflow_tracking_uri)
        print(f"\n[STRESS] MLflow run: {uri}")
    sys.exit(0 if all(r["success"] for r in reports) else 1)


if __name__ == "__main__":
    main()
