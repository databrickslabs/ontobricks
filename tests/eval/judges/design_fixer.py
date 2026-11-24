"""Rule-based judges for agent_design_fixer (dataset contract)."""

from __future__ import annotations

_ALLOWED_OPS = frozenset(
    {
        "clear_parent",
        "set_parent",
        "clear_domain",
        "set_domain",
        "clear_range",
        "set_range",
        "remove_group_member",
        "attach_attribute",
        "remove_property",
        "set_label",
        "set_description",
        "rename",
        "merge_into",
    }
)
_AI_RULES = frozenset(
    {
        "DC-W01",
        "DC-W02",
        "DC-W03",
        "DC-W04",
        "DC-W05",
        "DC-I01",
        "DC-I04",
    }
)


def score_dataset_row(row: dict) -> float:
    """1.0 when expected FixPlan constraints are well-formed; else 0.0."""
    inp = row.get("input") or {}
    expected = row.get("expected") or {}
    finding = inp.get("finding") or {}
    if finding.get("rule") not in _AI_RULES:
        return 0.0
    if not isinstance(finding.get("name"), str) or not finding["name"].strip():
        return 0.0
    ops = expected.get("operations") or []
    if not isinstance(ops, list) or not ops:
        return 0.0
    for op in ops:
        if op.get("op") not in _ALLOWED_OPS:
            return 0.0
        if not isinstance(op.get("rationale"), str) or not op["rationale"].strip():
            return 0.0
    if finding.get("rule") == "DC-W03":
        alts = expected.get("must_include_ops") or []
        if "remove_property" not in alts:
            return 0.0
    for forbidden in expected.get("forbidden_ops") or []:
        if any(op.get("op") == forbidden for op in ops):
            return 0.0
    return 1.0
