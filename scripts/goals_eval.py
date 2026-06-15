"""goals_eval — OntoBricks PGE intrinsic-evaluation CLI.

Two subcommands:

    score   evaluate a captured AgentResult artifact (cheap, deterministic,
            re-runnable). Consumes the JSON dumped by scripts/smoke_pge.py.

                $ .venv/bin/python scripts/goals_eval.py score <artifact.json> \
                    [--no-judge] [--gate-ratios]

    run     run the mapping PGE pipeline live, dump an artifact, then score it.
            A thin wrapper around score-only (D6).

                $ .venv/bin/python scripts/goals_eval.py run [--gate-ratios] \
                    [--no-judge]

Flags:
    --no-judge      skip the advisory LLM-judge (the ONLY LLM/network path).
                    Deterministic metrics always run with zero LLM calls.
    --gate-ratios   promote Tier-2 ratio warnings to hard gates for this run.

The process exit code is the scorecard verdict: 0 == GREEN, non-zero == RED.

The scorecard is usecase-agnostic and uses no gold/reference labels.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Make ``src/`` importable without a packaged install.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from agents.pge_eval.baseline import DEFAULT_BASELINE_DIR, save_scorecard  # noqa: E402
from agents.pge_eval.scorecard import score_artifact  # noqa: E402

LLM_ENDPOINT = os.environ.get("PGE_EVAL_ENDPOINT", "databricks-claude-opus-4-7")


def _now_ids():
    t = time.time()
    dt = datetime.fromtimestamp(t, tz=timezone.utc)
    # Microsecond-precise run_id so rapid successive runs never collide
    # (a collision would make a run baseline against itself and skip Tier-3).
    run_id = dt.strftime("%Y%m%dT%H%M%S_%f")
    ts = dt.isoformat()
    return run_id, ts


def _load_json(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def _resolve_judge_creds(args):
    """Return (host, token, endpoint) for the judge, or (None, None, None).

    Only touched when the judge is enabled — keeps ``--no-judge`` offline.
    """
    endpoint = args.endpoint or LLM_ENDPOINT
    try:
        from back.core.databricks.DatabricksClient import DatabricksClient

        client = DatabricksClient()
        return client.host, client.token, endpoint
    except Exception as exc:  # noqa: BLE001
        print(f"  (judge disabled — no Databricks credentials: {exc})", file=sys.stderr)
        return None, None, None


def _emit(scorecard: dict, out_path: str) -> None:
    text = json.dumps(scorecard, indent=2, default=str)
    if out_path:
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w") as f:
            f.write(text)
        print(f"Scorecard written to {out_path}", file=sys.stderr)
    print(text)


def _score_common(artifact, args, *, mode, ontology=None, metadata=None):
    run_id, ts = _now_ids()

    host = token = endpoint = None
    if not args.no_judge:
        host, token, endpoint = _resolve_judge_creds(args)
        if not host:
            # No creds resolved → degrade to deterministic-only, still no net.
            args.no_judge = True

    scorecard = score_artifact(
        artifact,
        ontology=ontology,
        metadata=metadata,
        gate_ratios=args.gate_ratios,
        no_judge=args.no_judge,
        mode=mode,
        run_id=run_id,
        timestamp=ts,
        endpoint=endpoint,
        host=host,
        token=token,
        baseline_dir=args.baseline_dir,
        use_baseline=not args.no_baseline,
    )

    if not args.no_save:
        path = save_scorecard(scorecard, args.baseline_dir)
        print(f"  (scorecard persisted to {path})", file=sys.stderr)

    _emit(scorecard, args.out)
    return scorecard


def cmd_score(args) -> int:
    artifact = _load_json(args.artifact)
    ontology = _load_json(args.ontology) if args.ontology else None
    metadata = _load_json(args.metadata) if args.metadata else None
    scorecard = _score_common(
        artifact, args, mode="score-only", ontology=ontology, metadata=metadata
    )
    return int(scorecard["exit_code"])


def cmd_run(args) -> int:
    """Live mode: run the mapping PGE for ANY domain, dump an artifact, score it.

    Domain-agnostic: the ontology + source metadata come from a registry export
    (``--registry-json`` [+``--version``]) or plain JSON files (``--ontology``
    [+``--metadata``]) — nothing about any specific domain is hard-coded.
    """
    from back.core.databricks.DatabricksClient import DatabricksClient
    from agents.agent_mapping_pge.engine import run_agent
    from agents.pge_eval.loaders import load_run_inputs

    registry_json = args.registry_json or os.environ.get("PGE_EVAL_REGISTRY_JSON")
    ontology, metadata = load_run_inputs(
        registry_json=registry_json,
        version=args.version,
        ontology_path=args.ontology,
        metadata_path=args.metadata,
    )

    client = DatabricksClient()
    t0 = time.time()
    result = run_agent(
        host=client.host,
        token=client.token,
        endpoint_name=args.endpoint or LLM_ENDPOINT,
        client=client,
        metadata=metadata,
        ontology=ontology,
        documents=[],
        on_step=lambda m, p: print(f"  [{p:3d}%] {m}", file=sys.stderr),
        skip_semantic_critic=args.no_judge,
    )
    elapsed = time.time() - t0

    artifact = {
        "success": result.success,
        "iterations": result.iterations,
        "error": result.error,
        "usage": result.usage,
        "stats": result.stats,
        "entity_mappings": result.entity_mappings,
        "relationship_mappings": result.relationship_mappings,
        "source_model": result.source_model,
        "mapping_evaluations": result.mapping_evaluations,
        "mapping_run_log": result.mapping_run_log,
        "steps": [
            {"step_type": s.step_type, "tool_name": s.tool_name, "duration_ms": s.duration_ms}
            for s in result.steps
        ],
        "ontology": ontology,
        "metadata": metadata,
        "elapsed_s": round(elapsed, 3),
    }
    scorecard = _score_common(
        artifact, args, mode="live", ontology=ontology, metadata=metadata
    )
    return int(scorecard["exit_code"])


def _add_common_flags(p):
    p.add_argument("--no-judge", action="store_true",
                   help="skip the advisory LLM-judge (no network calls)")
    p.add_argument("--gate-ratios", action="store_true",
                   help="promote Tier-2 ratio warnings to hard gates")
    p.add_argument("--endpoint", default=None, help="serving endpoint for the judge")
    p.add_argument("--baseline-dir", dest="baseline_dir", default=DEFAULT_BASELINE_DIR,
                   help="directory for Tier-3 self-baseline scorecards")
    p.add_argument("--no-baseline", action="store_true",
                   help="skip the Tier-3 self-baseline regression gate")
    p.add_argument("--no-save", action="store_true",
                   help="do not persist this scorecard to the baseline dir")
    p.add_argument("--out", default=None, help="also write the scorecard JSON here")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="goals_eval", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_score = sub.add_parser("score", help="score a captured artifact")
    p_score.add_argument("artifact", help="path to a smoke_pge AgentResult artifact JSON")
    p_score.add_argument("--ontology", default=None,
                         help="ontology JSON (defaults to artifact['ontology'])")
    p_score.add_argument("--metadata", default=None,
                         help="source metadata JSON (defaults to artifact['metadata'])")
    _add_common_flags(p_score)
    p_score.set_defaults(func=cmd_score)

    p_run = sub.add_parser("run", help="run the PGE pipeline live, then score it")
    p_run.add_argument("--registry-json", dest="registry_json", default=None,
                       help="exported registry version dump for ANY domain "
                            "({versions:{<ver>:{ontology,metadata}}}); "
                            "defaults to $PGE_EVAL_REGISTRY_JSON")
    p_run.add_argument("--version", default=None,
                       help="version key to pick from --registry-json "
                            "(required only when the dump has >1 version)")
    p_run.add_argument("--ontology", default=None,
                       help="ontology JSON (registry or agent shape) — "
                            "alternative to --registry-json")
    p_run.add_argument("--metadata", default=None,
                       help="source metadata JSON (used with --ontology)")
    _add_common_flags(p_run)
    p_run.set_defaults(func=cmd_run)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
