"""Typed FixPlan helpers for Sanity Checks Auto-fix."""

from __future__ import annotations

import itertools
import re
from typing import Any, Dict, Optional

_OP_SEQ = itertools.count(1)

ALLOWED_OPS = frozenset(
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

RULE_PLANNER: Dict[str, str] = {
    "DC-E01": "rule",
    "DC-E02": "rule",
    "DC-E03": "rule",
    "DC-E04": "rule",
    "DC-E05": "rule",
    "DC-E06": "none",
    "DC-W01": "ai",
    "DC-W02": "ai",
    "DC-W03": "ai",
    "DC-W04": "ai",
    "DC-W05": "ai",
    "DC-I01": "ai",
    "DC-I02": "rule",
    "DC-I03": "rule",
    "DC-I04": "ai",
    "DC-I05": "none",
}

AI_RULES = frozenset(r for r, p in RULE_PLANNER.items() if p == "ai")
RULE_RULES = frozenset(r for r, p in RULE_PLANNER.items() if p == "rule")
NO_AUTOFIX_RULES = frozenset(r for r, p in RULE_PLANNER.items() if p == "none")


def finding_id(finding: Dict[str, Any]) -> str:
    rule = finding.get("rule") or ""
    kind = finding.get("kind") or ""
    name = finding.get("name") or finding.get("uri") or ""
    domain = finding.get("domain") or ""
    base = f"{rule}:{kind}:{name}"
    return f"{base}:{domain}" if domain else base


def humanize_id(name: str) -> str:
    spaced = re.sub(r"[_\-]+", " ", name or "")
    spaced = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", spaced)
    spaced = re.sub(r"\s+", " ", spaced).strip()
    return spaced[:1].upper() + spaced[1:] if spaced else ""


def to_pascal_case(name: str) -> str:
    parts = re.split(
        r"[_\-\s]+", re.sub(r"(?<=[a-z])(?=[A-Z])", " ", name or "")
    )
    return "".join(p[:1].upper() + p[1:] for p in parts if p)


def to_camel_case(name: str) -> str:
    pascal = to_pascal_case(name)
    return pascal[:1].lower() + pascal[1:] if pascal else ""


def make_operation(
    *,
    op: str,
    target: Dict[str, Any],
    rationale: str,
    source: str,
    finding_id: str,
    params: Optional[Dict[str, Any]] = None,
    selected: bool = True,
    group: Optional[str] = None,
) -> Dict[str, Any]:
    if op not in ALLOWED_OPS:
        raise ValueError(f"Unknown op: {op}")
    out: Dict[str, Any] = {
        "id": f"op-{next(_OP_SEQ)}",
        "op": op,
        "target": dict(target),
        "params": dict(params or {}),
        "rationale": rationale,
        "source": source,
        "finding_id": finding_id,
        "selected": selected,
    }
    if group:
        out["group"] = group
    return out


def empty_plan(rule: str, summary: str) -> Dict[str, Any]:
    return {
        "rule": rule,
        "summary": summary,
        "operations": [],
        "alternatives": [],
    }
