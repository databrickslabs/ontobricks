"""Rule-based judges for ``agent_ontology_evolution``.

Each judge takes one dataset row and an ``observed`` dict produced by the
runner and returns a score in ``[0, 1]``. ``score_dataset_row`` checks the
row contract itself, so a malformed dataset fails before anything runs.
"""

from __future__ import annotations

from typing import Any, Dict

VARIANTS = {
    "base",
    "broken_mapping",
    "nofk",
    "orphan",
    "dup",
    "ambiguous_supplier",
    "restricted",
    "missing_source",
}
DECISIONS = {
    "PASS",
    "NO_CHANGE",
    "BLOCKED",
    "ABSTAIN",
    "INCONCLUSIVE",
    "STALE",
    "CANCELLED",
}
EVOLVE_OPS = {"add_relationship", "set_relationship_mapping"}
NEGATIVE_DECISIONS = {"BLOCKED", "ABSTAIN", "INCONCLUSIVE", "STALE", "CANCELLED"}


def score_dataset_row(row: Dict[str, Any]) -> float:
    """1.0 when the row has the fields every judge relies on, else 0.0."""
    try:
        inp, exp, tags = row["input"], row["expected"], row["tags"]
        intent = inp["intent"]
    except (KeyError, TypeError):
        return 0.0
    checks = (
        isinstance(row.get("id"), str) and bool(row["id"]),
        isinstance(inp.get("question"), str) and bool(inp["question"]),
        inp.get("domain_variant") in VARIANTS,
        isinstance(intent.get("filters"), list),
        isinstance(exp.get("ready"), bool),
        exp.get("decision") in DECISIONS,
        set(exp.get("proposal_ops") or []) <= EVOLVE_OPS,
        ("dev" in tags) != ("holdout" in tags),
    )
    return 1.0 if all(checks) else 0.0


def judge_gap(row: Dict[str, Any], observed: Dict[str, Any]) -> float:
    return 1.0 if observed.get("gap") == row["expected"].get("gap") else 0.0


def judge_ready(row: Dict[str, Any], observed: Dict[str, Any]) -> float:
    return 1.0 if bool(observed.get("ready")) == row["expected"]["ready"] else 0.0


def judge_false_ready(row: Dict[str, Any], observed: Dict[str, Any]) -> float:
    """1.0 when the runner wrongly reported ready (lower aggregate is better)."""
    return 1.0 if observed.get("ready") and not row["expected"]["ready"] else 0.0


def judge_states(row: Dict[str, Any], observed: Dict[str, Any]) -> float:
    expected = row["expected"].get("states") or {}
    if not expected:
        return 1.0
    got = observed.get("states") or {}
    hits = sum(1 for k, v in expected.items() if got.get(k) == v)
    return hits / len(expected)


def judge_decision(row: Dict[str, Any], observed: Dict[str, Any]) -> float:
    return 1.0 if observed.get("decision") == row["expected"]["decision"] else 0.0


def judge_abstention(row: Dict[str, Any], observed: Dict[str, Any]) -> float:
    if row["expected"]["decision"] not in NEGATIVE_DECISIONS:
        return 1.0
    return 1.0 if observed.get("decision") in NEGATIVE_DECISIONS else 0.0


def judge_ops(row: Dict[str, Any], observed: Dict[str, Any]) -> float:
    ops = list(observed.get("proposal_ops") or [])
    forbidden = set(row["expected"].get("forbidden_ops") or [])
    if forbidden & set(ops) or not set(ops) <= EVOLVE_OPS:
        return 0.0
    return (
        1.0 if sorted(ops) == sorted(row["expected"].get("proposal_ops") or []) else 0.0
    )


def judge_gold(
    row: Dict[str, Any], observed: Dict[str, Any], gold: Dict[str, list]
) -> float:
    """Execution-based: candidate result set equals the gold set."""
    gid = row["expected"].get("gold_question_id")
    expected_ids = row["expected"].get("gold_ids_override")
    if expected_ids is None:
        if not gid:
            return 1.0
        expected_ids = gold.get(gid)
    if expected_ids is None:
        return 0.0
    return (
        1.0
        if sorted(observed.get("candidate_ids") or []) == sorted(expected_ids)
        else 0.0
    )


def _filter_key(flt: Dict[str, Any], known_classes) -> tuple:
    cls = str(flt.get("class") or "")
    # A concept the ontology lacks has no attribute vocabulary to match.
    attr = str(flt.get("attribute") or "") if cls in known_classes else "*"
    return (
        cls,
        attr,
        str(flt.get("op") or "eq").lower(),
        str(flt.get("value") or "").strip(),
    )


def judge_intent(
    expected: Dict[str, Any], observed: Dict[str, Any], known_classes=()
) -> float:
    """1.0 when output class, filters, aggregate function and path hint all match.

    Mentions and ``max_edges`` are ignored: they do not change what is asked.
    Filter values must match exactly, so a rewritten or escaped value fails.
    """
    if not isinstance(observed, dict):
        return 0.0
    known = set(known_classes)
    same = (
        (expected.get("output_class") or None) == (observed.get("output_class") or None)
        and sorted(_filter_key(f, known) for f in expected.get("filters") or [])
        == sorted(_filter_key(f, known) for f in observed.get("filters") or [])
        and str((expected.get("aggregate") or {}).get("fn") or "").lower()
        == str((observed.get("aggregate") or {}).get("fn") or "").lower()
        and set(expected.get("path_hint") or []) == set(observed.get("path_hint") or [])
    )
    return 1.0 if same else 0.0
