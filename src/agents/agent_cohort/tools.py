"""
Cohort Discovery Agent -- tool definitions and handlers.

The cohort agent translates natural-language prompts like *"find people who
can be staffed together"* into a validated :class:`CohortRule` JSON. It
introspects the live ontology + graph through six small, deterministic
tools that all wrap **existing** Stage 1 endpoints (see ``cohort_design.md``
§12). The agent NEVER writes -- saving / materialisation remain user-driven
in the form.

Tools (all read-only except ``propose_rule``, which only validates):
    * ``list_classes``         -- compact ontology summary (classes only).
    * ``list_properties_of``   -- data + object properties for a class.
    * ``count_class_members``  -- live row count for a class via the
                                  Stage 1 ``/preview/class-stats`` route.
    * ``sample_values_of``     -- distinct property values via the
                                  Stage 1 ``/sample-values`` route.
    * ``propose_rule``         -- pydantic-style validation against the
                                  :class:`CohortRule` dataclass; on
                                  success, parks the rule in
                                  ``ToolContext.metadata['proposed_rule']``
                                  for the engine to return.
    * ``dry_run``              -- POST /dtwin/cohorts/dry-run; returns
                                  cluster-count + size summary only
                                  (truncated body to keep the prompt small).
"""

from __future__ import annotations

import json
from typing import Callable, Dict, List

import httpx

from agents.tools.context import ToolContext
from back.core.graph_analysis.models import CohortRule
from back.core.helpers import extract_local_name
from back.core.logging import get_logger

logger = get_logger(__name__)

_HTTP_TIMEOUT = 60
_MAX_CLASSES_RETURNED = 80
_MAX_PROPS_PER_CLASS = 60
_MAX_SAMPLE_LIMIT = 30
_DRY_RUN_PREVIEW_CLUSTERS = 5


# =====================================================
# Internal helpers
# =====================================================


def _client(ctx: ToolContext) -> httpx.Client:
    """Build a sync HTTP client bound to the loopback OntoBricks URL."""
    return httpx.Client(
        base_url=ctx.dtwin_base_url or "http://localhost:8000",
        cookies=ctx.dtwin_session_cookies or {},
        headers=ctx.dtwin_session_headers or {},
        timeout=_HTTP_TIMEOUT,
        follow_redirects=False,
    )


def _registry_params(ctx: ToolContext) -> dict:
    out = {}
    for k, v in (ctx.dtwin_registry_params or {}).items():
        if v:
            out[k] = v
    return out


def _error(msg: str) -> str:
    logger.warning("agent_cohort: %s", msg)
    return json.dumps({"error": msg})


def _ok(payload: dict) -> str:
    return json.dumps(payload, default=str)


def _pick_label(node: dict) -> str:
    return node.get("label") or node.get("name") or node.get("uri") or ""


def _ensure_uri(node: dict, base_uri: str) -> str:
    """Return the entry's URI, synthesising ``{base_uri}{name}`` when missing.

    Some flows (wizard, RDFS-only imports, manual ontology edits) store
    ``dataProperties`` / ``properties`` entries with only ``name`` /
    ``localName`` and no ``uri``. The cohort engine -- and downstream
    SPARQL -- need a real URI, so we mint one against the domain's
    ``base_uri`` when possible. When ``base_uri`` is empty we fall back
    to the local name; that still gives the LLM a usable identifier
    even if the eventual rule will need a fix-up.
    """
    uri = (node.get("uri") or node.get("iri") or node.get("id") or "").strip()
    if uri:
        return uri
    name = (node.get("localName") or node.get("name") or "").strip()
    if not name:
        return ""
    base = (base_uri or "").rstrip("/#")
    if not base:
        return name
    return f"{base}/{name}"


def _domain_matches(prop_domain: str, class_uri: str) -> bool:
    """Permissive domain match: full URI, local-name, or case-insensitive name.

    The OWL parser strips ``rdfs:domain`` to a local name (e.g.
    ``"Consultant"``), while wizard-created properties may store the
    bare class name verbatim or even lowercased. We accept any of:

    * exact full-URI match,
    * local-name match (``local_name(prop_domain) == local_name(class_uri)``),
    * case-insensitive equality on local names.
    """
    if not prop_domain:
        return False
    if prop_domain == class_uri:
        return True
    cls_local = extract_local_name(class_uri)
    dom_local = extract_local_name(prop_domain)
    if cls_local and dom_local and dom_local == cls_local:
        return True
    if cls_local and dom_local and dom_local.lower() == cls_local.lower():
        return True
    return False


# =====================================================
# Tool handlers
# =====================================================


def tool_list_classes(ctx: ToolContext, **_kwargs) -> str:
    """Return a compact list of ontology classes for the active domain.

    The ontology already lives in the user's session, so we hit the
    internal ``/ontology/get-loaded-ontology`` route -- the same one the
    cohort form uses for its class picker -- and shrink the payload to
    just what the LLM needs (URI + label + a hint that data properties
    exist).
    """
    try:
        with _client(ctx) as c:
            resp = c.get("/ontology/get-loaded-ontology")
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        return _error(
            f"ontology fetch failed ({exc.response.status_code}): "
            f"{exc.response.text[:200]}"
        )
    except Exception as exc:  # pragma: no cover -- network errors
        return _error(f"ontology fetch error: {exc}")

    ont = data.get("ontology") or {}
    classes = ont.get("classes") or []
    out = []
    for c in classes[:_MAX_CLASSES_RETURNED]:
        uri = c.get("uri") or c.get("iri") or ""
        if not uri:
            continue
        out.append({
            "uri": uri,
            "label": _pick_label(c),
            "data_property_count": len(c.get("dataProperties") or []),
        })
    return _ok({"count": len(out), "classes": out})


def tool_list_properties_of(
    ctx: ToolContext, *, class_uri: str | None = None, **_kwargs
) -> str:
    """Return data + object properties usable on instances of *class_uri*.

    Data properties are read off the class's ``dataProperties`` block
    AND cross-referenced with the global ``properties`` list filtered
    by ``DatatypeProperty`` + permissive domain match. Object properties
    are filtered from the global ``properties`` list using
    :func:`_domain_matches` (full URI, local-name, or case-insensitive).

    Robustness: entries lacking a ``uri`` field have one synthesised
    from ``ontology.base_uri + name`` (some import flows -- wizard,
    RDFS-only -- omit URIs). When no object property's domain matches
    the class we fall back to the full object-property list with
    ``domain_unknown=True``, since the cohort form itself shows all
    object properties without filtering.
    """
    if not class_uri:
        return _error("Missing 'class_uri'.")

    try:
        with _client(ctx) as c:
            resp = c.get("/ontology/get-loaded-ontology")
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:  # pragma: no cover -- network errors
        return _error(f"ontology fetch error: {exc}")

    ont = data.get("ontology") or {}
    base_uri = (ont.get("base_uri") or "").strip()

    cls = next(
        (
            c
            for c in (ont.get("classes") or [])
            if (c.get("uri") or c.get("iri")) == class_uri
        ),
        None,
    )
    if cls is None:
        return _error(f"Class '{class_uri}' not found in ontology.")

    seen_data: set = set()
    data_props: list = []

    def _push_data(p: dict, *, source: str) -> None:
        if len(data_props) >= _MAX_PROPS_PER_CLASS:
            return
        uri = _ensure_uri(p, base_uri)
        if not uri or uri in seen_data:
            return
        seen_data.add(uri)
        data_props.append({
            "uri": uri,
            "label": _pick_label(p),
            "name": p.get("name") or p.get("localName") or extract_local_name(uri),
            "datatype": p.get("range") or p.get("datatype") or "",
            "inherited": bool(p.get("inherited")),
            "uri_synthesised": not (p.get("uri") or p.get("iri") or p.get("id")),
            "source": source,
        })

    for p in cls.get("dataProperties") or []:
        _push_data(p, source="class")

    for p in ont.get("properties") or []:
        kind = (p.get("type") or p.get("kind") or "").lower()
        if "datatype" not in kind:
            continue
        if not _domain_matches(p.get("domain") or "", class_uri):
            continue
        _push_data(p, source="global")

    seen_obj: set = set()
    matched_obj_props: list = []
    all_obj_props: list = []

    for p in ont.get("properties") or []:
        kind = (p.get("type") or p.get("kind") or "").lower()
        if "object" not in kind:
            continue
        uri = _ensure_uri(p, base_uri)
        if not uri or uri in seen_obj:
            continue
        seen_obj.add(uri)
        entry = {
            "uri": uri,
            "label": _pick_label(p),
            "name": p.get("name") or p.get("localName") or extract_local_name(uri),
            "domain": p.get("domain") or "",
            "range": p.get("range") or "",
            "uri_synthesised": not (p.get("uri") or p.get("iri") or p.get("id")),
        }
        all_obj_props.append(entry)
        if _domain_matches(p.get("domain") or "", class_uri):
            matched_obj_props.append(entry)

    domain_unknown = False
    if matched_obj_props:
        object_props = matched_obj_props[:_MAX_PROPS_PER_CLASS]
    else:
        domain_unknown = bool(all_obj_props)
        object_props = all_obj_props[:_MAX_PROPS_PER_CLASS]
        for entry in object_props:
            entry["domain_unknown"] = True

    return _ok({
        "class_uri": class_uri,
        "class_label": _pick_label(cls),
        "data_properties": data_props,
        "object_properties": object_props,
        "object_properties_domain_unknown": domain_unknown,
        "base_uri": base_uri,
    })


def tool_count_class_members(
    ctx: ToolContext, *, class_uri: str | None = None, **_kwargs
) -> str:
    """Count graph instances of *class_uri* via the live preview route."""
    if not class_uri:
        return _error("Missing 'class_uri'.")

    try:
        with _client(ctx) as c:
            resp = c.get(
                "/dtwin/cohorts/preview/class-stats",
                params={**_registry_params(ctx), "class_uri": class_uri},
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        return _error(
            f"class-stats failed ({exc.response.status_code}): "
            f"{exc.response.text[:200]}"
        )
    except Exception as exc:  # pragma: no cover
        return _error(f"class-stats error: {exc}")

    if not data.get("success"):
        return _error(data.get("message") or "class-stats returned no data")

    return _ok({
        "class_uri": class_uri,
        "instance_count": data.get("count", 0),
    })


def tool_sample_values_of(
    ctx: ToolContext,
    *,
    class_uri: str | None = None,
    property_uri: str | None = None,
    limit: int = 20,
    **_kwargs,
) -> str:
    """Return up to *limit* distinct values for a (class, datatype-prop) pair."""
    if not class_uri or not property_uri:
        return _error("'class_uri' and 'property_uri' are required.")

    try:
        n = int(limit)
    except (TypeError, ValueError):
        n = 20
    n = max(1, min(n, _MAX_SAMPLE_LIMIT))

    body = {
        "class_uri": class_uri,
        "property": property_uri,
        "limit": n,
    }
    try:
        with _client(ctx) as c:
            resp = c.post(
                "/dtwin/cohorts/sample-values",
                params=_registry_params(ctx),
                json=body,
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        return _error(
            f"sample-values failed ({exc.response.status_code}): "
            f"{exc.response.text[:200]}"
        )
    except Exception as exc:  # pragma: no cover
        return _error(f"sample-values error: {exc}")

    if not data.get("success"):
        return _error(data.get("message") or "sample-values returned no data")

    return _ok({
        "class_uri": class_uri,
        "property_uri": property_uri,
        "values": data.get("values", []),
        "truncated": bool(data.get("truncated")),
    })


def tool_propose_rule(ctx: ToolContext, *, rule: dict | None = None, **_kwargs) -> str:
    """Validate a proposed CohortRule and park it on the context.

    The agent calls this tool when it has assembled a complete rule. We
    run client-side validation via :class:`CohortRule.from_dict` +
    :meth:`CohortRule.validate`; on success we stash the canonical dict
    in ``ctx.metadata['proposed_rule']`` so the engine can return it.

    The LLM is encouraged (but not required) to call ``dry_run`` afterwards
    to surface the cluster count to the user before terminating.
    """
    if not isinstance(rule, dict):
        return _error("Argument 'rule' must be an object.")
    try:
        obj = CohortRule.from_dict(rule)
        errors = obj.validate()
    except Exception as exc:
        return _error(f"Could not parse rule: {exc}")

    if errors:
        return _ok({"valid": False, "errors": errors})

    canonical = obj.to_dict()
    ctx.metadata["proposed_rule"] = canonical
    return _ok({
        "valid": True,
        "rule_id": canonical.get("id"),
        "class_uri": canonical.get("class_uri"),
        "links": len(canonical.get("links") or []),
        "compatibility": len(canonical.get("compatibility") or []),
    })


def tool_dry_run(ctx: ToolContext, *, rule: dict | None = None, **_kwargs) -> str:
    """Run :meth:`CohortBuilder.build` and return summary stats only.

    Wraps the existing ``POST /dtwin/cohorts/dry-run`` route. To keep the
    prompt small we strip the cluster member URIs and return the top-N
    cluster sizes plus aggregate stats.
    """
    if not isinstance(rule, dict):
        return _error("Argument 'rule' must be an object.")

    try:
        with _client(ctx) as c:
            resp = c.post(
                "/dtwin/cohorts/dry-run",
                params=_registry_params(ctx),
                json=rule,
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        body = exc.response.text[:200]
        return _error(f"dry-run failed ({exc.response.status_code}): {body}")
    except Exception as exc:  # pragma: no cover
        return _error(f"dry-run error: {exc}")

    if not data.get("success"):
        return _error(data.get("message") or "dry-run returned no data")

    clusters = data.get("clusters") or []
    head = []
    for c in clusters[:_DRY_RUN_PREVIEW_CLUSTERS]:
        head.append({
            "id": c.get("id"),
            "size": c.get("size") or len(c.get("members") or []),
            "idx": c.get("idx"),
        })
    return _ok({
        "stats": data.get("stats") or {},
        "cluster_count": len(clusters),
        "head": head,
    })


# =====================================================
# Tool schema definitions (OpenAI chat-completions format)
# =====================================================


_LIST_CLASSES_DEF = {
    "type": "function",
    "function": {
        "name": "list_classes",
        "description": (
            "Return the list of ontology classes available in the active "
            "domain (uri, label, data-property count). Always call this first "
            "to anchor on a real class URI before proposing a rule."
        ),
        "parameters": {"type": "object", "properties": {}},
    },
}

_LIST_PROPERTIES_OF_DEF = {
    "type": "function",
    "function": {
        "name": "list_properties_of",
        "description": (
            "List the datatype properties (for compatibility constraints) and "
            "object properties (for cohort 'links') applicable to a class. "
            "Use the property URIs returned here verbatim in the proposed rule."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "class_uri": {
                    "type": "string",
                    "description": "Full URI of the class, as returned by list_classes.",
                },
            },
            "required": ["class_uri"],
        },
    },
}

_COUNT_CLASS_MEMBERS_DEF = {
    "type": "function",
    "function": {
        "name": "count_class_members",
        "description": (
            "Return the number of graph instances of a class. Useful to "
            "sanity-check the user's intent (e.g. 'group consultants' should "
            "have non-zero members)."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "class_uri": {"type": "string"},
            },
            "required": ["class_uri"],
        },
    },
}

_SAMPLE_VALUES_OF_DEF = {
    "type": "function",
    "function": {
        "name": "sample_values_of",
        "description": (
            "Return up to N distinct values seen for a datatype property on "
            "instances of a class. Use this to spell value_equals / value_in "
            "constants the way the data actually stores them ('Exempt' vs. "
            "'EXEMPT', 'US' vs. 'United States')."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "class_uri": {"type": "string"},
                "property_uri": {"type": "string"},
                "limit": {"type": "integer", "description": "1-30, default 20."},
            },
            "required": ["class_uri", "property_uri"],
        },
    },
}

_PROPOSE_RULE_DEF = {
    "type": "function",
    "function": {
        "name": "propose_rule",
        "description": (
            "Validate and register a complete CohortRule JSON. Call this once "
            "you have an id, label, class_uri, and at least one link or "
            "compatibility row. Returns valid=true plus the rule id on success "
            "or a list of validation errors. Required terminating tool: you "
            "MUST call propose_rule successfully before ending the conversation."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "rule": {
                    "type": "object",
                    "description": (
                        "CohortRule JSON. Schema:\n"
                        "{ id: str, label: str, class_uri: str,\n"
                        "  links: [{shared_class: str, via?: str}],\n"
                        "  links_combine: 'any'|'all',\n"
                        "  compatibility: [\n"
                        "    {type: 'same_value', property: str}\n"
                        "    | {type: 'value_equals', property: str, value: any}\n"
                        "    | {type: 'value_in', property: str, values: [any]}\n"
                        "    | {type: 'value_range', property: str, min?: num, max?: num}\n"
                        "  ],\n"
                        "  group_type: 'connected'|'strict',\n"
                        "  min_size: int>=2,\n"
                        "  output: {graph: bool}\n"
                        "}"
                    ),
                },
            },
            "required": ["rule"],
        },
    },
}

_DRY_RUN_DEF = {
    "type": "function",
    "function": {
        "name": "dry_run",
        "description": (
            "Run the cohort engine on a candidate rule WITHOUT writing "
            "anything and return summary stats (cluster count + top 5 cluster "
            "sizes). Use this at most once after propose_rule to confirm the "
            "rule produces sensible cohorts before answering."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "rule": {"type": "object"},
            },
            "required": ["rule"],
        },
    },
}


TOOL_DEFINITIONS: List[dict] = [
    _LIST_CLASSES_DEF,
    _LIST_PROPERTIES_OF_DEF,
    _COUNT_CLASS_MEMBERS_DEF,
    _SAMPLE_VALUES_OF_DEF,
    _PROPOSE_RULE_DEF,
    _DRY_RUN_DEF,
]

TOOL_HANDLERS: Dict[str, Callable] = {
    "list_classes": tool_list_classes,
    "list_properties_of": tool_list_properties_of,
    "count_class_members": tool_count_class_members,
    "sample_values_of": tool_sample_values_of,
    "propose_rule": tool_propose_rule,
    "dry_run": tool_dry_run,
}
