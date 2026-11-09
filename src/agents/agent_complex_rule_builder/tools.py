"""
Complex Rule Builder Agent – tool definitions and handlers.

Read tools let the LLM ground its questions and generated SPARQL in the
**live loaded ontology** (classes / properties passed into ``run_agent`` and
exposed in-process via :class:`~agents.tools.context.ToolContext` — no HTTP
loopback). The single write tool ``update_draft`` stashes the current
CONSTRUCT draft on the context so the engine can surface it to the editor's
right-hand code pane after each turn.

Tools:
    * ``get_ontology_overview`` – class names + key data properties + all
      object properties (domain/range), so the agent uses real names.
    * ``describe_class``        – one class's data + object properties.
    * ``update_draft``          – (re)write the proposed CONSTRUCT query.
"""

import json
from typing import Callable, Dict, List

from agents.tools.context import ToolContext
from back.core.logging import get_logger

logger = get_logger(__name__)


def _local_name(uri_or_name: str) -> str:
    if not uri_or_name:
        return ""
    i = max(uri_or_name.rfind("#"), uri_or_name.rfind("/"))
    return uri_or_name[i + 1:] if i >= 0 else uri_or_name


def _data_property_names(cls: dict) -> List[str]:
    out: List[str] = []
    for dp in cls.get("dataProperties", []) or []:
        name = dp.get("name") or dp.get("localName") or _local_name(dp.get("uri", ""))
        if name:
            out.append(name)
    return out


def _object_properties_for_class(ctx: ToolContext, class_name: str) -> List[dict]:
    """Object properties whose domain is *class_name* (localname, case-insensitive)."""
    target = (class_name or "").lower()
    out: List[dict] = []
    for p in ctx.ontology_properties:
        dom = (p.get("domain") or "")
        if dom.lower() == target or _local_name(dom).lower() == target:
            out.append(
                {
                    "name": p.get("name", ""),
                    "label": p.get("label", ""),
                    "range": p.get("range", ""),
                    "direction": p.get("direction", "forward"),
                    "type": p.get("type", ""),
                }
            )
    return out


# =====================================================
# Read tools
# =====================================================


def tool_get_ontology_overview(ctx: ToolContext, **_kwargs) -> str:
    """Return a compact overview of the loaded ontology."""
    classes = []
    for cls in ctx.ontology_classes:
        classes.append(
            {
                "name": cls.get("name", ""),
                "label": cls.get("label", ""),
                "parent": cls.get("parent", ""),
                "dataProperties": _data_property_names(cls),
            }
        )
    properties = []
    for p in ctx.ontology_properties:
        properties.append(
            {
                "name": p.get("name", ""),
                "domain": p.get("domain", ""),
                "range": p.get("range", ""),
                "type": p.get("type", ""),
            }
        )
    return json.dumps(
        {
            "classes": classes,
            "properties": properties,
            "base_uri": ctx.ontology_base_uri,
            "class_count": len(classes),
            "property_count": len(properties),
        }
    )


def tool_describe_class(ctx: ToolContext, *, name: str = "", **_kwargs) -> str:
    """Return one class's data + object properties."""
    target = (name or "").lower()
    cls = next(
        (
            c
            for c in ctx.ontology_classes
            if (c.get("name", "").lower() == target)
            or (_local_name(c.get("uri", "")).lower() == target)
        ),
        None,
    )
    if cls is None:
        return json.dumps({"error": f"Class '{name}' not found"})
    return json.dumps(
        {
            "name": cls.get("name", ""),
            "label": cls.get("label", ""),
            "description": cls.get("description", ""),
            "parent": cls.get("parent", ""),
            "dataProperties": _data_property_names(cls),
            "objectProperties": _object_properties_for_class(ctx, cls.get("name", "")),
        }
    )


# =====================================================
# Write tool
# =====================================================


def tool_update_draft(
    ctx: ToolContext,
    *,
    name: str = "",
    description: str = "",
    query: str = "",
    ready: bool = False,
    **_kwargs,
) -> str:
    """(Re)write the proposed CONSTRUCT draft shown in the editor's code pane."""
    ctx.rule_draft = {
        "name": name or "",
        "description": description or "",
        "query": query or "",
    }
    ctx.rule_draft_ready = bool(ready)
    logger.info(
        "agent_complex_rule_builder: draft updated (ready=%s, query_len=%d)",
        ctx.rule_draft_ready,
        len(query or ""),
    )
    return json.dumps({"ok": True, "ready": ctx.rule_draft_ready})


# =====================================================
# Tool schemas
# =====================================================

_GET_OVERVIEW_DEF = {
    "type": "function",
    "function": {
        "name": "get_ontology_overview",
        "description": (
            "Get a compact overview of the loaded ontology: class names with "
            "their data properties, and all object properties with domain/range. "
            "Call this first to learn the real names before asking questions or "
            "writing the query."
        ),
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
}

_DESCRIBE_CLASS_DEF = {
    "type": "function",
    "function": {
        "name": "describe_class",
        "description": (
            "Get one class's data properties and the object properties whose "
            "domain is that class, with ranges. Use to confirm exact property "
            "names before writing the CONSTRUCT query."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "The class name (e.g. 'Customer').",
                }
            },
            "required": ["name"],
        },
    },
}

_UPDATE_DRAFT_DEF = {
    "type": "function",
    "function": {
        "name": "update_draft",
        "description": (
            "Write or rewrite the proposed SPARQL CONSTRUCT rule shown to the "
            "user in the code pane. Call this whenever the query materially "
            "changes. Set ready=true only when the CONSTRUCT is complete and "
            "self-consistent."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Short rule name (e.g. 'InferHighValueCustomer').",
                },
                "description": {
                    "type": "string",
                    "description": "One-line description of what the rule infers.",
                },
                "query": {
                    "type": "string",
                    "description": "The full SPARQL CONSTRUCT { ... } WHERE { ... } query.",
                },
                "ready": {
                    "type": "boolean",
                    "description": "True when the query is complete and ready to save.",
                },
            },
            "required": ["query"],
        },
    },
}

TOOL_DEFINITIONS: List[dict] = [
    _GET_OVERVIEW_DEF,
    _DESCRIBE_CLASS_DEF,
    _UPDATE_DRAFT_DEF,
]

TOOL_HANDLERS: Dict[str, Callable] = {
    "get_ontology_overview": tool_get_ontology_overview,
    "describe_class": tool_describe_class,
    "update_draft": tool_update_draft,
}
