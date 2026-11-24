"""Read-only tools for ``agent_ontology_evolution`` plus its two terminal submits.

Every tool reads the parent version's ontology index, the coverage report or
the curated Genie Pages held on the context; none of them writes anything or
reaches a warehouse. The submit tools validate before storing and tell the
model what to fix, which is the agent's only repair loop.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, List

from agents.tools.context import ToolContext
from agents.tracing import trace_tool
from back.core.errors import ValidationError
from back.objects.ontology.evolution.Intent import Intent
from back.objects.ontology.evolution.IntentValidator import FILTER_OPS, IntentValidator
from back.objects.ontology.evolution.OntologyIndex import normalise
from back.objects.ontology.evolution.ProposalBuilder import ProposalBuilder

MAX_SUBMIT_ATTEMPTS = 2  # the first submit plus one repair


def _state(ctx: ToolContext) -> Dict[str, Any]:
    return ctx.metadata["evolve"]


def _dump(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)


def _tool(name: str, description: str, properties: dict, required: list) -> dict:
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


_CITATIONS = {
    "type": "array",
    "description": "Page facts you relied on; quote copied verbatim from the page.",
    "items": {
        "type": "object",
        "properties": {"page": {"type": "string"}, "quote": {"type": "string"}},
        "required": ["page", "quote"],
    },
}

_READ_TOOLS: List[dict] = [
    _tool(
        "list_classes",
        "Classes with label, alternate labels, description and attributes.",
        {},
        [],
    ),
    _tool(
        "list_relationships",
        "Relationships (object properties) with domain, range and whether they are mapped.",
        {},
        [],
    ),
    _tool(
        "find_terms",
        "Classes and attributes whose name or labels match a word or phrase, "
        "plus page passages that mention it.",
        {"text": {"type": "string"}},
        ["text"],
    ),
    _tool(
        "search_pages",
        "Passages of the curated Genie Pages that match a query.",
        {"query": {"type": "string"}},
        ["query"],
    ),
    _tool(
        "read_page",
        "Read a curated Genie Page from an offset.",
        {"name": {"type": "string"}, "offset": {"type": "integer"}},
        ["name"],
    ),
]

INTENT_TOOLS: List[dict] = _READ_TOOLS + [
    _tool(
        "submit_intent",
        "Submit the typed intent once. Returns errors to fix if it is invalid.",
        {
            "intent": {
                "type": "object",
                "properties": {
                    "output_class": {"type": ["string", "null"]},
                    "mentions": {"type": "array", "items": {"type": "string"}},
                    "path_hint": {"type": "array", "items": {"type": "string"}},
                    "filters": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "class": {"type": "string"},
                                "attribute": {"type": "string"},
                                "op": {"type": "string", "enum": list(FILTER_OPS)},
                                "value": {"type": "string"},
                            },
                            "required": ["class", "attribute", "op", "value"],
                        },
                    },
                    "aggregate": {
                        "type": ["object", "null"],
                        "properties": {
                            "fn": {"type": "string"},
                            "attribute": {"type": "string"},
                        },
                    },
                },
                "required": ["output_class", "filters"],
            },
            "ambiguities": {"type": "array", "items": {"type": "string"}},
            "citations": _CITATIONS,
        },
        ["intent"],
    ),
]

PROPOSAL_TOOLS: List[dict] = _READ_TOOLS + [
    _tool(
        "list_join_candidates",
        "The joins coverage profiled for the gap, by index, with evidence and data checks.",
        {},
        [],
    ),
    _tool(
        "submit_proposal",
        "Submit up to three alternatives, best first. Returns errors to fix if none is usable.",
        {
            "summary": {"type": "string"},
            "alternatives": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "candidate": {"type": "integer"},
                        "name": {"type": "string"},
                        "label": {"type": "string"},
                        "description": {"type": "string"},
                        "rationale": {"type": "string"},
                        "citations": _CITATIONS,
                    },
                    "required": ["candidate", "rationale"],
                },
            },
        },
        ["alternatives"],
    ),
]


@trace_tool("ontology_evolution:list_classes")
def tool_list_classes(ctx: ToolContext, **_kwargs) -> str:
    index = _state(ctx)["index"]
    rows = []
    for name, cls in index.classes.items():
        rows.append(
            {
                "name": name,
                "label": cls.get("label") or "",
                "alternate_labels": list(cls.get("alternate_labels") or []),
                "description": str(cls.get("description") or "")[:300],
                "mapped": index.entity(name) is not None,
                "attributes": ["id"]
                + [
                    a.get("name") or a.get("localName")
                    for a in cls.get("dataProperties") or []
                    if a.get("name") or a.get("localName")
                ],
            }
        )
    return _dump({"classes": rows})


@trace_tool("ontology_evolution:list_relationships")
def tool_list_relationships(ctx: ToolContext, **_kwargs) -> str:
    index = _state(ctx)["index"]
    return _dump(
        {
            "relationships": [
                {
                    "name": name,
                    "label": rel.get("label") or "",
                    "domain": rel.get("domain") or "",
                    "range": rel.get("range") or "",
                    "mapped": index.relationship_mapping(name) is not None,
                    "description": str(rel.get("description") or "")[:300],
                }
                for name, rel in index.relationships.items()
            ]
        }
    )


@trace_tool("ontology_evolution:find_terms")
def tool_find_terms(ctx: ToolContext, text: str = "", **_kwargs) -> str:
    state = _state(ctx)
    index = state["index"]
    needle = normalise(text)
    attributes = []
    if needle:
        for cls_name, cls in index.classes.items():
            for attr in cls.get("dataProperties") or []:
                forms = {
                    normalise(attr.get("name") or ""),
                    normalise(attr.get("label") or ""),
                }
                if any(f and (needle in f or f in needle) for f in forms):
                    attributes.append(f"{cls_name}.{attr.get('name')}")
    return _dump(
        {
            "classes": index.resolve_mention(text),
            "attributes": attributes[:20],
            "page_passages": state["pages"].search(text, limit=3),
        }
    )


@trace_tool("ontology_evolution:search_pages")
def tool_search_pages(ctx: ToolContext, query: str = "", **_kwargs) -> str:
    pages = _state(ctx)["pages"]
    if not pages:
        return _dump(
            {"passages": [], "note": "No Genie Pages are selected for this version."}
        )
    return _dump({"passages": pages.search(query)})


@trace_tool("ontology_evolution:read_page")
def tool_read_page(ctx: ToolContext, name: str = "", offset: int = 0, **_kwargs) -> str:
    return _dump(_state(ctx)["pages"].read(name, offset))


def _candidate_rows(coverage: Dict[str, Any]) -> List[Dict[str, Any]]:
    join = coverage.get("join") or {}
    rows = []
    for i, cand in enumerate(join.get("candidates") or []):
        profile = cand.get("profile") or {}
        rows.append(
            {
                "candidate": i,
                "source_class": cand["source_class"],
                "target_class": cand["target_class"],
                "join": (
                    f"{cand['source_table'].split('.')[-1]}.{cand['source_column']} = "
                    f"{cand['target_table'].split('.')[-1]}.{cand['target_column']}"
                ),
                "evidence": cand.get("evidence") or [],
                "data_valid": bool(cand.get("data_valid")),
                "problems": cand.get("reasons") or [],
                "source_rows": profile.get("source_rows"),
                "matched_rows": profile.get("matched"),
                "chosen_by_rules": cand == join.get("chosen"),
            }
        )
    return rows


@trace_tool("ontology_evolution:list_join_candidates")
def tool_list_join_candidates(ctx: ToolContext, **_kwargs) -> str:
    coverage = _state(ctx)["coverage"]
    return _dump({"gap": coverage.get("gap"), "candidates": _candidate_rows(coverage)})


def _attempt(state: Dict[str, Any]) -> bool:
    state["submit_attempts"] = state.get("submit_attempts", 0) + 1
    return state["submit_attempts"] <= MAX_SUBMIT_ATTEMPTS


def _rejected(state: Dict[str, Any], errors: List[str]) -> str:
    state["last_errors"] = errors
    left = MAX_SUBMIT_ATTEMPTS - state["submit_attempts"]
    return _dump({"ok": False, "errors": errors, "attempts_left": max(left, 0)})


@trace_tool("ontology_evolution:submit_intent")
def tool_submit_intent(
    ctx: ToolContext,
    intent: Any = None,
    ambiguities: Any = None,
    citations: Any = None,
    **_kwargs,
) -> str:
    state = _state(ctx)
    if not _attempt(state):
        return _dump({"ok": False, "errors": ["no_attempts_left"]})
    try:
        parsed = Intent.from_dict(intent)
    except ValidationError as exc:
        return _rejected(state, [exc.message])
    check = IntentValidator(state["index"]).check(parsed)
    if check["errors"]:
        return _rejected(state, check["errors"])
    kept = state["pages"].verify(citations)
    intent_out = parsed.to_dict()
    notes = list(check["notes"])
    if intent_out["path_hint"] and not kept:
        # A path hint makes coverage keep that path and skip the shorter-link
        # check, so it must come from the question's organisation, not a guess.
        intent_out["path_hint"] = []
        notes.append("path_hint_dropped_without_citation")
    state["submission"] = {
        "intent": intent_out,
        "ambiguities": [str(a)[:300] for a in (ambiguities or []) if str(a).strip()][
            :5
        ],
        "citations": kept,
        "dropped_citations": (
            len(citations or []) - len(kept) if isinstance(citations, list) else 0
        ),
        "notes": notes,
    }
    return _dump({"ok": True, "notes": notes})


@trace_tool("ontology_evolution:submit_proposal")
def tool_submit_proposal(
    ctx: ToolContext, summary: str = "", alternatives: Any = None, **_kwargs
) -> str:
    state = _state(ctx)
    if not _attempt(state):
        return _dump({"ok": False, "errors": ["no_attempts_left"]})
    alts = []
    dropped = 0
    for alt in alternatives if isinstance(alternatives, list) else []:
        if isinstance(alt, dict):
            raw = alt.get("citations")
            kept = state["pages"].verify(raw)
            dropped += (len(raw) if isinstance(raw, list) else 0) - len(kept)
            alt = {**alt, "citations": kept}
        alts.append(alt)
    suggestion = {"alternatives": alts}
    built = ProposalBuilder(state["index"]).from_suggestion(
        state["coverage"], suggestion
    )
    if built is None:
        return _rejected(state, ["coverage_allows_no_proposal"])
    if not built["alternatives"]:
        return _rejected(
            state,
            [f"alternative {r['index']}: {r['reason']}" for r in built["rejected"]],
        )
    state["submission"] = {
        "summary": str(summary or "")[:400],
        "alternatives": alts,
        "rejected": built["rejected"],
        "dropped_citations": dropped,
    }
    return _dump(
        {
            "ok": True,
            "usable": len(built["alternatives"]),
            "rejected": built["rejected"],
        }
    )


_READ_HANDLERS: Dict[str, Callable] = {
    "list_classes": tool_list_classes,
    "list_relationships": tool_list_relationships,
    "find_terms": tool_find_terms,
    "search_pages": tool_search_pages,
    "read_page": tool_read_page,
}
INTENT_HANDLERS: Dict[str, Callable] = {
    **_READ_HANDLERS,
    "submit_intent": tool_submit_intent,
}
PROPOSAL_HANDLERS: Dict[str, Callable] = {
    **_READ_HANDLERS,
    "list_join_candidates": tool_list_join_candidates,
    "submit_proposal": tool_submit_proposal,
}
