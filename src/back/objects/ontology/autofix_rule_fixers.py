"""Deterministic FixPlan builders for mechanical Sanity Checks rules."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.objects.ontology.autofix_ops import (
    RULE_RULES,
    empty_plan,
    finding_id,
    make_operation,
    to_camel_case,
    to_pascal_case,
)


def propose_rule_fix(
    finding: Dict[str, Any],
    classes: List[Dict[str, Any]],
    properties: List[Dict[str, Any]],
    groups: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Build a FixPlan for one mechanical-rule finding."""
    rule = finding.get("rule") or ""
    if rule not in RULE_RULES:
        return empty_plan(rule, "Not a mechanical Auto-fix rule.")

    handlers = {
        "DC-E01": _fix_clear_parent,
        "DC-E02": _fix_clear_domain,
        "DC-E03": _fix_clear_range,
        "DC-E04": _fix_remove_group_member,
        "DC-E05": _fix_clear_parent,
        "DC-I02": _fix_class_naming,
        "DC-I03": _fix_property_naming,
    }
    handler = handlers.get(rule)
    if handler is None:
        return empty_plan(rule, "No rule fixer registered.")
    return handler(finding, classes, properties, groups)


def _target(finding: Dict[str, Any]) -> Dict[str, Any]:
    target: Dict[str, Any] = {
        "kind": finding.get("kind") or "",
        "name": finding.get("name") or "",
    }
    if finding.get("domain"):
        target["domain"] = finding["domain"]
    return target


def _single_op_plan(
    finding: Dict[str, Any],
    *,
    op: str,
    rationale: str,
    params: Optional[Dict[str, Any]] = None,
    summary: str,
) -> Dict[str, Any]:
    plan = empty_plan(finding.get("rule") or "", summary)
    plan["operations"].append(
        make_operation(
            op=op,
            target=_target(finding),
            params=params,
            rationale=rationale,
            source="rule",
            finding_id=finding_id(finding),
        )
    )
    return plan


def _fix_clear_parent(
    finding: Dict[str, Any],
    classes: List[Dict[str, Any]],
    properties: List[Dict[str, Any]],
    groups: List[Dict[str, Any]],
) -> Dict[str, Any]:
    name = finding.get("name") or ""
    return _single_op_plan(
        finding,
        op="clear_parent",
        rationale=f"Clear the broken or cyclic parent entity on '{name}'.",
        summary=f"Clear parent entity on '{name}'.",
    )


def _fix_clear_domain(
    finding: Dict[str, Any],
    classes: List[Dict[str, Any]],
    properties: List[Dict[str, Any]],
    groups: List[Dict[str, Any]],
) -> Dict[str, Any]:
    name = finding.get("name") or ""
    missing = finding.get("domain") or ""
    return _single_op_plan(
        finding,
        op="clear_domain",
        rationale=f"Drop the link from '{name}' to the missing entity '{missing}'.",
        summary=f"Unlink '{name}' from missing entity '{missing}'.",
    )


def _fix_clear_range(
    finding: Dict[str, Any],
    classes: List[Dict[str, Any]],
    properties: List[Dict[str, Any]],
    groups: List[Dict[str, Any]],
) -> Dict[str, Any]:
    name = finding.get("name") or ""
    return _single_op_plan(
        finding,
        op="clear_range",
        rationale=f"Clear the broken range reference on '{name}'.",
        summary=f"Clear range on '{name}'.",
    )


def _fix_remove_group_member(
    finding: Dict[str, Any],
    classes: List[Dict[str, Any]],
    properties: List[Dict[str, Any]],
    groups: List[Dict[str, Any]],
) -> Dict[str, Any]:
    group_name = finding.get("name") or ""
    member = finding.get("member") or ""
    if not member:
        return empty_plan(
            finding.get("rule") or "",
            "Broken group member finding is missing the member name.",
        )
    return _single_op_plan(
        finding,
        op="remove_group_member",
        params={"group": group_name, "member": member},
        rationale=f"Remove unknown member '{member}' from group '{group_name}'.",
        summary=f"Remove '{member}' from group '{group_name}'.",
    )


def _existing_names(
    classes: List[Dict[str, Any]], properties: List[Dict[str, Any]]
) -> set[str]:
    names = {str(c.get("name") or "") for c in classes}
    names.update(str(p.get("name") or "") for p in properties)
    names.discard("")
    return names


def _fix_class_naming(
    finding: Dict[str, Any],
    classes: List[Dict[str, Any]],
    properties: List[Dict[str, Any]],
    groups: List[Dict[str, Any]],
) -> Dict[str, Any]:
    name = finding.get("name") or ""
    new_name = to_pascal_case(name)
    if not new_name or new_name == name:
        return empty_plan(
            finding.get("rule") or "",
            f"'{name}' already looks like PascalCase.",
        )
    if new_name in _existing_names(classes, properties):
        return empty_plan(
            finding.get("rule") or "",
            f"Rename collision: '{new_name}' already exists.",
        )
    return _single_op_plan(
        finding,
        op="rename",
        params={"new_name": new_name},
        rationale=f"Rename entity '{name}' to PascalCase '{new_name}'.",
        summary=f"Rename '{name}' → '{new_name}'.",
    )


def _fix_property_naming(
    finding: Dict[str, Any],
    classes: List[Dict[str, Any]],
    properties: List[Dict[str, Any]],
    groups: List[Dict[str, Any]],
) -> Dict[str, Any]:
    name = finding.get("name") or ""
    new_name = to_camel_case(name)
    if not new_name or new_name == name:
        return empty_plan(
            finding.get("rule") or "",
            f"'{name}' already looks like camelCase.",
        )
    if new_name in _existing_names(classes, properties):
        return empty_plan(
            finding.get("rule") or "",
            f"Rename collision: '{new_name}' already exists.",
        )
    return _single_op_plan(
        finding,
        op="rename",
        params={"new_name": new_name},
        rationale=(
            f"Rename {'attribute' if finding.get('kind') == 'attribute' else 'relationship'} "
            f"'{name}' to camelCase '{new_name}'."
        ),
        summary=f"Rename '{name}' → '{new_name}'.",
    )
