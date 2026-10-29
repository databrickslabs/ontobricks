"""Tools for the simulation facts agent.

``list_classes`` / ``list_properties_of`` reuse the cohort ontology
introspectors. ``search_entities`` hits the same live-graph preview as
the fact wizard. ``propose_patches`` validates :class:`SimulationPatch`.
"""

from __future__ import annotations

import json
from typing import Callable, Dict, List

import httpx

from agents.agent_cohort.tools import tool_list_classes, tool_list_properties_of
from agents.tools.context import ToolContext
from agents.tools.loopback_http import loopback_client
from back.core.errors import ValidationError
from back.core.logging import get_logger
from back.objects.simulation.SimulationPatch import SimulationPatch

logger = get_logger(__name__)

_HTTP_TIMEOUT = 60
_MAX_SEEDS = 15
_MAX_PATCHES = 50


def _client(ctx: ToolContext):
    return loopback_client(ctx, timeout=_HTTP_TIMEOUT)


def _error(msg: str) -> str:
    logger.warning("agent_simulation_facts: %s", msg)
    return json.dumps({"error": msg})


def _ok(payload: dict) -> str:
    return json.dumps(payload, default=str)


def tool_search_entities(
    ctx: ToolContext,
    *,
    query: str | None = None,
    class_uri: str | None = None,
    **_kwargs,
) -> str:
    """Preview-search live graph entities by label or id fragment."""
    q = str(query or "").strip()
    if not q and not class_uri:
        return _error("'query' or 'class_uri' is required.")
    body = {
        "phase": "preview",
        "entity_type": class_uri or "",
        "field": "any",
        "match_type": "contains",
        "value": q,
        "include_inferred": True,
    }
    sid = str(getattr(ctx, "dtwin_simulation_id", "") or "").strip()
    if sid:
        body["simulation"] = sid
    try:
        with _client(ctx) as client:
            resp = client.post("/dtwin/sync/filter", json=body)
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        return _error(
            f"entity search failed ({exc.response.status_code}): "
            f"{exc.response.text[:200]}"
        )
    except Exception as exc:  # pragma: no cover
        return _error(f"entity search error: {exc}")

    seeds = []
    for seed in (data.get("seeds") or [])[:_MAX_SEEDS]:
        uri = seed.get("uri") or seed.get("id") or ""
        if not uri:
            continue
        seeds.append({
            "uri": uri,
            "label": seed.get("label") or seed.get("name") or "",
            "type": seed.get("type") or "",
            "type_uri": seed.get("type_uri") or "",
        })
    return _ok({"query": q, "class_uri": class_uri or "", "seeds": seeds})


def tool_propose_patches(
    ctx: ToolContext,
    *,
    patches: list | None = None,
    scenario_description: str | None = None,
    **_kwargs,
) -> str:
    """Validate hypothetical facts and park them on the context."""
    if patches is None:
        patches = []
    if not isinstance(patches, list):
        return _error("'patches' must be a list.")
    if len(patches) > _MAX_PATCHES:
        return _error(f"At most {_MAX_PATCHES} patches per proposal.")
    canonical = []
    for i, raw in enumerate(patches):
        try:
            patch = SimulationPatch.from_dict(raw)
        except (ValidationError, TypeError, ValueError) as exc:
            return _error(f"patches[{i}]: {exc}")
        canonical.append(patch.to_dict())
    desc = str(scenario_description or "").strip()[:4000]
    if not canonical and not desc:
        return _error("Provide patches or a scenario_description.")
    ctx.metadata["proposed_patches"] = canonical
    if desc:
        ctx.metadata["proposed_description"] = desc
    return _ok({"valid": True, "count": len(canonical), "has_description": bool(desc)})


_SEARCH_DEF = {
    "type": "function",
    "function": {
        "name": "search_entities",
        "description": (
            "Search the live knowledge graph for entity instances matching "
            "a name or id fragment. Pass class_uri to restrict by type. "
            "Use the returned URIs as patch subject/object; do not invent URIs."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "class_uri": {
                    "type": "string",
                    "description": "Optional class URI from list_classes.",
                },
            },
            "required": ["query"],
        },
    },
}

_PROPOSE_DEF = {
    "type": "function",
    "function": {
        "name": "propose_patches",
        "description": (
            "Validate the hypothetical facts and register them for the UI. "
            "REQUIRED terminator: call this with the patch list and a "
            "complete scenario_description before your final reply. op is "
            "add, set, or remove. Use ontology property URIs and instance "
            "URIs from search_entities (or a minted URI only after search "
            "returned no hit). scenario_description merges CURRENT "
            "DESCRIPTION with the new what-if in 1–3 short paragraphs."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "patches": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "op": {"type": "string", "enum": ["add", "set", "remove"]},
                            "subject": {"type": "string"},
                            "predicate": {"type": "string"},
                            "object": {"type": "string"},
                            "datatype": {"type": "string"},
                        },
                        "required": ["op", "subject", "predicate", "object"],
                    },
                },
                "scenario_description": {
                    "type": "string",
                    "description": (
                        "Complete General-tab description of this scenario. "
                        "Keep prior intent from CURRENT DESCRIPTION."
                    ),
                },
            },
            "required": ["patches", "scenario_description"],
        },
    },
}

_LIST_CLASSES_DEF = {
    "type": "function",
    "function": {
        "name": "list_classes",
        "description": "List ontology classes (uri, label) in the active domain.",
        "parameters": {"type": "object", "properties": {}},
    },
}

_LIST_PROPERTIES_DEF = {
    "type": "function",
    "function": {
        "name": "list_properties_of",
        "description": (
            "List datatype (attribute) and object (relationship) properties "
            "for a class. Use those URIs as patch predicates."
        ),
        "parameters": {
            "type": "object",
            "properties": {"class_uri": {"type": "string"}},
            "required": ["class_uri"],
        },
    },
}

TOOL_DEFINITIONS: List[dict] = [
    _LIST_CLASSES_DEF,
    _LIST_PROPERTIES_DEF,
    _SEARCH_DEF,
    _PROPOSE_DEF,
]

TOOL_HANDLERS: Dict[str, Callable] = {
    "list_classes": tool_list_classes,
    "list_properties_of": tool_list_properties_of,
    "search_entities": tool_search_entities,
    "propose_patches": tool_propose_patches,
}
