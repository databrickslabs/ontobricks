"""Read-only tools for ``agent_design_fixer`` plus terminal ``submit_fix_plan``."""

from __future__ import annotations

import json
import re
from typing import Any, Callable, Dict, List

from agents.tools.context import ToolContext
from agents.tracing import trace_tool
from back.objects.ontology.autofix_ops import ALLOWED_OPS

__all__ = ["ToolContext", "TOOL_DEFINITIONS", "TOOL_HANDLERS"]


def _tool_def(name: str, description: str, properties: dict, required: list) -> dict:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {
                "type": "object",
                "properties": properties,
                "required": required,
                "additionalProperties": False,
            },
        },
    }


TOOL_DEFINITIONS: List[dict] = [
    _tool_def(
        "list_classes",
        "List ontology classes with name, label, parent, and attribute names.",
        {},
        [],
    ),
    _tool_def(
        "list_properties",
        "List ontology properties with name, type, domain, and range.",
        {},
        [],
    ),
    _tool_def(
        "get_item",
        "Return the full record for one class, property, attribute, or group.",
        {
            "kind": {
                "type": "string",
                "enum": ["class", "property", "attribute", "group"],
            },
            "name": {"type": "string"},
        },
        ["kind", "name"],
    ),
    _tool_def(
        "neighbourhood",
        "Parents, children, and properties touching a class.",
        {"name": {"type": "string"}},
        ["name"],
    ),
    _tool_def(
        "find_similar_names",
        "Find class/property names similar to a token.",
        {"token": {"type": "string"}},
        ["token"],
    ),
    _tool_def(
        "submit_fix_plan",
        "Submit the final FixPlan. Does not mutate the ontology.",
        {
            "plan": {
                "type": "object",
                "description": "FixPlan with rule, summary, operations, alternatives",
            }
        },
        ["plan"],
    ),
]


def _dump(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False)


@trace_tool("design_fixer:list_classes")
def tool_list_classes(ctx: ToolContext, **_kwargs) -> str:
    rows = []
    for cls in ctx.ontology_classes or []:
        rows.append(
            {
                "name": cls.get("name"),
                "label": cls.get("label") or "",
                "parent": cls.get("parent") or cls.get("parent_uri") or "",
                "attributes": [
                    a.get("name")
                    for a in (cls.get("dataProperties") or [])
                    if a.get("name")
                ],
            }
        )
    return _dump({"classes": rows})


@trace_tool("design_fixer:list_properties")
def tool_list_properties(ctx: ToolContext, **_kwargs) -> str:
    rows = []
    for prop in ctx.ontology_properties or []:
        rows.append(
            {
                "name": prop.get("name"),
                "type": prop.get("type") or "",
                "domain": prop.get("domain") or "",
                "range": prop.get("range") or "",
                "label": prop.get("label") or "",
            }
        )
    return _dump({"properties": rows})


@trace_tool("design_fixer:get_item")
def tool_get_item(ctx: ToolContext, kind: str = "", name: str = "", **_kwargs) -> str:
    kind = (kind or "").strip().lower()
    name = (name or "").strip()
    if kind == "class":
        for cls in ctx.ontology_classes or []:
            if cls.get("name") == name:
                return _dump({"item": cls})
    elif kind in ("property", "attribute"):
        for prop in ctx.ontology_properties or []:
            if prop.get("name") == name:
                return _dump({"item": prop})
    elif kind == "group":
        for group in ctx.ontology_groups or []:
            if group.get("name") == name:
                return _dump({"item": group})
    return _dump({"error": f"{kind} '{name}' not found"})


@trace_tool("design_fixer:neighbourhood")
def tool_neighbourhood(ctx: ToolContext, name: str = "", **_kwargs) -> str:
    name = (name or "").strip()
    cls = next(
        (c for c in (ctx.ontology_classes or []) if c.get("name") == name),
        None,
    )
    if not cls:
        return _dump({"error": f"Class '{name}' not found"})
    parent = cls.get("parent") or cls.get("parent_uri") or ""
    children = [
        c.get("name")
        for c in (ctx.ontology_classes or [])
        if (c.get("parent") or c.get("parent_uri") or "") == name
    ]
    attrs = [
        a.get("name") for a in (cls.get("dataProperties") or []) if a.get("name")
    ]
    related = [
        {
            "name": p.get("name"),
            "type": p.get("type"),
            "domain": p.get("domain"),
            "range": p.get("range"),
        }
        for p in (ctx.ontology_properties or [])
        if p.get("domain") == name or p.get("range") == name
    ]
    return _dump(
        {
            "name": name,
            "parent": parent,
            "children": children,
            "attributes": attrs,
            "related_properties": related,
        }
    )


def _norm(token: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (token or "").lower())


@trace_tool("design_fixer:find_similar_names")
def tool_find_similar_names(ctx: ToolContext, token: str = "", **_kwargs) -> str:
    needle = _norm(token)
    if not needle:
        return _dump({"matches": []})
    names: List[str] = []
    for cls in ctx.ontology_classes or []:
        if cls.get("name"):
            names.append(str(cls["name"]))
    for prop in ctx.ontology_properties or []:
        if prop.get("name"):
            names.append(str(prop["name"]))
    matches = []
    for name in names:
        hay = _norm(name)
        if needle in hay or hay in needle or needle[:4] and needle[:4] in hay:
            matches.append(name)
    # de-dupe preserving order
    seen = set()
    out = []
    for m in matches:
        if m not in seen:
            seen.add(m)
            out.append(m)
    return _dump({"matches": out[:12]})


@trace_tool("design_fixer:submit_fix_plan")
def tool_submit_fix_plan(ctx: ToolContext, plan: Any = None, **_kwargs) -> str:
    if not isinstance(plan, dict):
        return _dump({"error": "plan must be an object"})
    for key in ("rule", "summary", "operations", "alternatives"):
        if key not in plan:
            return _dump({"error": f"plan missing '{key}'"})
    if not isinstance(plan.get("operations"), list) or not isinstance(
        plan.get("alternatives"), list
    ):
        return _dump({"error": "operations and alternatives must be arrays"})
    for op in list(plan.get("operations") or []) + list(plan.get("alternatives") or []):
        if not isinstance(op, dict) or op.get("op") not in ALLOWED_OPS:
            return _dump(
                {
                    "error": (
                        f"Invalid op {op.get('op')!r}; "
                        f"allowed={sorted(ALLOWED_OPS)}"
                    )
                }
            )
        if not str(op.get("rationale") or "").strip():
            return _dump({"error": "Each operation needs a non-empty rationale"})
    ctx.submitted_fix_plan = plan
    return _dump({"ok": True, "stored": True})


TOOL_HANDLERS: Dict[str, Callable] = {
    "list_classes": tool_list_classes,
    "list_properties": tool_list_properties,
    "get_item": tool_get_item,
    "neighbourhood": tool_neighbourhood,
    "find_similar_names": tool_find_similar_names,
    "submit_fix_plan": tool_submit_fix_plan,
}
