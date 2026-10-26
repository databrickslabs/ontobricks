"""Parsed-document corpus + staged-contract evaluation for agent_owl_generator.

``--live`` invokes the real production entry points against the real
Databricks Foundation Model endpoint for BOTH contracts:

* Parsed-corpus rows (``input.documents``) drive ``staged.detect_entities()``
  — the only entry point that still reads source material at all (SPEC §3;
  the deprecated one-shot ``agent_owl_generator.engine.run_agent`` bridge is
  never imported or called here or anywhere else in this eval).
* Staged ``detect``-tagged rows drive the same ``detect_entities()`` real
  call, scored through the identical constraint checks
  ``tests/eval/staged_contract.py`` uses offline
  (``score_staged_examples_live``), plus a representative
  ``infer_relations`` -> ``infer_attributes`` -> ``infer_axioms`` completion
  chain against the real endpoint.
* Before any of the above runs, ``_init_live_tracing`` calls
  ``agents.tracing.setup_tracing()`` so the ``@trace_agent``/``@trace_llm``/
  ``@trace_tool`` decorators on the staged entry points and
  ``call_serving_endpoint`` actually emit MLflow spans instead of silently
  no-opping (a prior gap: live runs carried zero trace evidence).

Deterministic/offline mode (default, no ``--live``) is unaffected: both
contracts stay fully scripted/stub-based, no network required, and tracing
is never initialised.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))
if str(ROOT / "tests" / "eval") not in sys.path:
    sys.path.insert(0, str(ROOT / "tests" / "eval"))

import mlflow  # noqa: E402

from agents.tracing import setup_tracing  # noqa: E402
from document_corpus_contract import run_contract  # noqa: E402
from staged_contract import score_staged_examples, score_staged_examples_live  # noqa: E402

DATASET = ROOT / "tests/eval/datasets/agent_owl_generator/baseline.jsonl"
THRESHOLDS = ROOT / "tests/eval/thresholds.yaml"

# Minimum required "staged" material-change examples for the three-stage
# Generate design (docs/superpowers/specs/2026-09-20-three-stage-ontology-
# generate-design.md). These describe the target contract for the staged
# `detect_entities` / `infer_relations` / `infer_attributes` / `infer_axioms`
# entry points ahead of their implementation (Task 3 of the
# `staged-ontology-generate` plan) and are not yet scored by
# `document_corpus_contract.score_example`, which only judges parsed-corpus
# tool-call traces. This check keeps the staged rows represented and
# structurally executable now, without weakening the existing parsed-corpus
# contract check above.
#
# Raised 17 -> 18 (this revision, inverse-relation collapse): new
# regression row `staged-no-inverse-relations-001` proves
# `schemas.drop_inverse_relations` keeps the active-voice direction only
# (`drops_inverse_relations` constraint kind) — see SPEC.md §6.
#
# Raised 18 -> 28 (semantic relation dedup): ten `staged-relation-dedup-*`
# rows prove paraphrases/inverses collapse to one active-voice relation,
# distinct predicates on the same pair survive, and existing ontology
# relations are never restated — see SPEC.md §6.
#
# Raised 28 -> 31 (single-inheritance axioms): extra subClassOf parents
# against an existing hierarchy are dropped — see SPEC.md §6.
_MIN_STAGED_EXAMPLES = 31
_REQUIRED_STAGED_CONSTRAINT_FIELDS = {"kind", "value"}

# Every required staged topic must be exercised by at least one staged
# example's constraint `kind` (see SPEC.md §5 proposed staged dimensions).
# This locks in coverage for the review-flagged contract gaps — a rejected
# stage output must never be silently rewritten in-request, no entry point
# may perform one-shot generation as a default, and a fully-anchored source
# (every selected entity already a locked anchor) must succeed with an
# explicit empty candidate list rather than a malformed-JSON rejection (the
# live Stage-1 detection failure this fixes) — so a future dataset edit
# cannot silently drop any of them while still satisfying the count floor
# above.
_REQUIRED_STAGED_CONSTRAINT_KINDS = {
    "stage_no_rewrite_after_reject",
    "stage_no_one_shot_default",
    "empty_candidates_when_fully_anchored",
    # Live id-bracketing bug fix (this revision): a bracketed id (the exact
    # live-bug shape, e.g. `[cand-6]`) must always be rejected reject-only —
    # locked in so this coverage cannot silently disappear from the dataset.
    "rejects_bracketed_id_reference",
    "drops_inverse_relations",
    "collapses_passive_paraphrase",
    "collapses_inverse_paraphrase",
    "keeps_distinct_predicates_same_pair",
    "keeps_existing_ontology_relation",
    "drops_extra_subclassof",
}


def _validate_staged_examples(path: Path) -> int:
    """Validate staged-contract dataset rows and return how many were found.

    A "staged" row is any dataset line tagged ``"staged"``. Unlike the
    parsed-corpus rows consumed by ``document_corpus_contract.load_examples``,
    staged rows describe the not-yet-implemented staged entry-point contract
    (see SPEC.md §3a/§6a) and are validated structurally rather than by
    replaying an agent trace.
    """
    lines = [
        line
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    all_examples = [json.loads(line) for line in lines]
    staged = [
        example
        for example in all_examples
        if "staged" in example.get("tags", [])
    ]
    if len(staged) < _MIN_STAGED_EXAMPLES:
        raise ValueError(
            f"{path} has {len(staged)} staged examples; minimum is "
            f"{_MIN_STAGED_EXAMPLES}"
        )
    ids = [str(example.get("id", "")) for example in staged]
    if any(not item for item in ids) or len(ids) != len(set(ids)):
        raise ValueError(f"{path} contains missing or duplicate staged ids")
    seen_kinds: set = set()
    for example in staged:
        stage = example.get("input", {}).get("stage")
        if not stage:
            raise ValueError(f"{example['id']}: staged example missing input.stage")
        constraints = example.get("expected", {}).get("constraints", [])
        if not constraints:
            raise ValueError(
                f"{example['id']}: staged example has no expected constraints"
            )
        for constraint in constraints:
            if not _REQUIRED_STAGED_CONSTRAINT_FIELDS.issubset(constraint):
                raise ValueError(
                    f"{example['id']}: staged constraint missing "
                    f"{_REQUIRED_STAGED_CONSTRAINT_FIELDS}: {constraint}"
                )
            seen_kinds.add(constraint["kind"])
    missing_kinds = _REQUIRED_STAGED_CONSTRAINT_KINDS - seen_kinds
    if missing_kinds:
        raise ValueError(
            f"{path} is missing required staged constraint kinds: "
            f"{sorted(missing_kinds)}"
        )
    return len(staged)


def _live_runner(
    example: Dict[str, Any], *, host: str, token: str, endpoint: str
) -> Tuple[List[str], str, List[str]]:
    """Exercise the real ``staged.detect_entities()`` entry point for one
    parsed-corpus material-change row (``input.documents``) against the real
    endpoint.

    ``detect_entities`` is now the *only* production entry point that reads
    source material at all (SPEC §3) — the deprecated one-shot
    ``agent_owl_generator.engine.run_agent`` bridge this runner used to call
    has no remaining production caller and is intentionally not imported
    here. The observed trace is built the same way as before: the tool
    surface is unchanged by staging (``list_documents``/``read_document``),
    so tool-call names plus every tool-result payload (which always echoes
    back the requested ``filename``) drive the same
    ``document_corpus_contract.score_example`` dimensions
    (``ready_corpus_use``, ``no_parse_safety``, ``status_disclosure``,
    ``sidecar_hiding``) — plus the detected candidates' own labels/
    descriptions/evidence, standing in for the free-text "reply" the old
    one-shot bridge produced, since ``detect_entities`` returns structured
    candidates rather than prose.
    """
    from agents.agent_owl_generator import staged
    from agents.agent_owl_generator.tools import TOOL_HANDLERS

    documents = example["input"]["documents"]
    by_name = {doc["name"]: doc for doc in documents}

    def list_documents(_ctx, **_kwargs):
        files = [
            {
                "name": doc["name"],
                "size": len(doc.get("content", "")),
                "parse_status": doc["parse_status"],
            }
            for doc in documents
        ]
        return json.dumps({"files": files, "count": len(files)})

    def read_document(_ctx, *, filename: str = "", **_kwargs):
        doc = by_name.get(filename)
        if doc is None:
            return json.dumps({"filename": filename, "error": "Document not found"})
        if doc["parse_status"] != "ready":
            return json.dumps(
                {
                    "filename": filename,
                    "parse_status": doc["parse_status"],
                    "error": doc.get("error") or "Document parsing is not ready",
                }
            )
        return json.dumps(
            {
                "filename": filename,
                "parse_status": "ready",
                "content": doc.get("content", ""),
                "size": len(doc.get("content", "")),
                "truncated": bool(doc.get("truncated")),
            }
        )

    originals = {
        "list_documents": TOOL_HANDLERS["list_documents"],
        "read_document": TOOL_HANDLERS["read_document"],
    }
    TOOL_HANDLERS.update(
        {"list_documents": list_documents, "read_document": read_document}
    )
    try:
        result = staged.detect_entities(
            host=host,
            token=token,
            endpoint_name=endpoint,
            metadata={"tables": []},
            guidelines=example["input"]["request"],
            existing_anchors=[],
            selected_docs=[doc["name"] for doc in documents],
            registry={},
        )
    finally:
        TOOL_HANDLERS.update(originals)

    tools_called = [
        step.tool_name
        for step in result.steps
        if step.step_type == "tool_call" and step.tool_name
    ]
    candidate_text = " ".join(
        " ".join(
            [candidate.canonical_label, candidate.description]
            + candidate.alternate_labels
            + [
                f"{evidence.get('source', '')} {evidence.get('excerpt', '')}"
                for evidence in candidate.evidence
            ]
        )
        for candidate in result.candidate_entities
    )
    observed_text = " ".join(
        [candidate_text, result.error]
        + [step.content for step in result.steps if step.step_type == "tool_result"]
    )
    return tools_called, observed_text, [doc["name"] for doc in documents]


def _init_live_tracing(tracking_uri: str | None, experiment_name: str) -> bool:
    """Initialise MLflow tracing (``agents.tracing.setup_tracing``) BEFORE
    any live Foundation Model call.

    Final-review live-eval investigation finding: ``--live`` previously
    never called ``setup_tracing()``, so the ``@trace_agent``/``@trace_llm``/
    ``@trace_tool`` decorators already on
    ``staged.detect_entities``/``infer_relations``/``infer_attributes``/
    ``infer_axioms``/``call_serving_endpoint`` silently no-opped
    (``agents.tracing._TRACING_READY`` stayed ``False`` for the whole
    process) and the live MLflow run carried zero trace/span evidence. This
    must run before ``run_contract`` — which calls ``live_runner`` once per
    parsed-corpus example from inside its own example loop, *before* it sets
    up its own MLflow run — so tracing is enabled for every live call this
    eval makes, not only the ones after ``run_contract``'s own
    ``mlflow.start_run()`` block starts.

    Never receives ``host``/``token``: ``setup_tracing()``'s only parameter
    is the experiment name, so no secret can be logged through this call by
    construction (mirrors ``agents.tracing._safe_inputs``'s own
    token/host/client exclusion for span inputs).
    """
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)
    ready = setup_tracing(experiment_name=experiment_name)
    print(
        f"[TRACING] setup_tracing(experiment_name={experiment_name!r}) -> "
        f"{'enabled' if ready else 'DISABLED (see log above for reason)'}"
    )
    return ready


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--host", default=os.getenv("DATABRICKS_HOST"))
    parser.add_argument("--token", default=os.getenv("DATABRICKS_TOKEN"))
    parser.add_argument("--endpoint", default=os.getenv("ONTOBRICKS_LLM_ENDPOINT"))
    parser.add_argument(
        "--mlflow-experiment",
        default="/Shared/ontobricks/agents/owl_generator",
    )
    parser.add_argument(
        "--mlflow-tracking-uri",
        default=os.getenv("MLFLOW_TRACKING_URI", "databricks"),
    )
    args = parser.parse_args()
    if args.live and not (args.host and args.token and args.endpoint):
        parser.error("--live requires host, token, and endpoint")

    staged_count = _validate_staged_examples(DATASET)
    print(
        f"[STAGED] validated {staged_count} staged contract examples "
        "(structure); scoring them behaviourally against the staged entry "
        "points (SPEC.md §3a/§6a)…"
    )
    # Behavioural, deterministic scoring of the staged contract now that
    # detect_entities / infer_relations / infer_attributes / infer_axioms
    # exist at runtime (Task 3). No live LLM required — staged LLM calls are
    # scripted, so the deterministic contract (parsing, closure, ordering,
    # staleness, reject-only, staged-only surface) is what gets scored. This
    # is the enforced CI gate and is unaffected by --live (never weakened).
    score_staged_examples(DATASET, THRESHOLDS)

    live = None
    extra_mlflow_logging = None
    if args.live:
        _init_live_tracing(args.mlflow_tracking_uri, args.mlflow_experiment)

        live = lambda example: _live_runner(  # noqa: E731
            example, host=args.host, token=args.token, endpoint=args.endpoint
        )

        def extra_mlflow_logging() -> None:
            """Score the staged detect + completion-chain dimensions against
            the real endpoint and log them into the SAME MLflow run as the
            parsed-corpus contract above (called inside its
            ``mlflow.start_run()`` block — see ``run_contract``)."""
            import mlflow

            print(
                "[STAGED LIVE] scoring staged detect_entities/infer_* "
                "against the real endpoint…"
            )
            live_scores = score_staged_examples_live(
                DATASET,
                THRESHOLDS,
                host=args.host,
                token=args.token,
                endpoint=args.endpoint,
            )
            for name, value in live_scores.items():
                mlflow.log_metric(f"staged_live_{name}", value)

    run_contract(
        agent_name="owl_generator",
        dataset_path=DATASET,
        thresholds_path=THRESHOLDS,
        document_tool="read_document",
        dry_run=not args.live,
        live_runner=live,
        mlflow_experiment=args.mlflow_experiment,
        mlflow_tracking_uri=args.mlflow_tracking_uri,
        extra_mlflow_logging=extra_mlflow_logging,
    )


if __name__ == "__main__":
    main()
