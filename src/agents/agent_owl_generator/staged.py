"""Staged orchestrator for the three-stage ontology Generate workflow.

Public entry points (design:
``docs/superpowers/specs/2026-09-20-three-stage-ontology-generate-design.md``;
SPEC: ``.planning/agents/agent_owl_generator/SPEC.md`` §2/§3/§3a/§6a):

* :func:`detect_entities` — **Stage 1**. Reads the selected metadata and the
  READY parsed-document corpus (through the existing no-reparse tools), the
  guidelines/options, and the current ontology's locked anchors, and returns a
  bounded set of structured candidate entities (with evidence/provenance),
  deduplicated against the anchors. Never designs relations/attributes/axioms.
* :func:`infer_relations` / :func:`infer_attributes` / :func:`infer_axioms` —
  **Stage 3**, in that strict order. Each consumes ONLY the persisted,
  validated draft contract (locked anchors + included candidates, addressed by
  stable id — plus prior substage results), never a document tool, and emits
  structured output that references only those ids. An out-of-closure
  reference is **rejected** (reject-only), never fed back to the LLM for an
  in-request rewrite.

Checkpoint *persistence*, the async workflow, and the API routes are Task 4
(:mod:`back.objects.ontology.generate.GenerateWorkflow` +
:mod:`api.routers.internal.ontology`'s ``/wizard/generate/*`` routes) — these
functions validate ordering by inspecting the passed draft's checkpoints but
do not write them themselves. The deprecated one-shot bridge lives in
:mod:`agents.agent_owl_generator.engine`; it is intentionally NOT imported or
reachable from this module (or from any production caller as of Task 4), so
a staged path can never fall back to one-shot generation.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence

import requests

from back.core.logging import get_logger
from back.objects.ontology.generate.DraftValidationError import DraftValidationError
from back.objects.ontology.generate.GenerateDraft import (
    CHECKPOINT_DONE,
    GenerateDraft,
    GenerateEntity,
    SUBSTAGE_ATTRIBUTES,
    SUBSTAGE_AXIOMS,
    SUBSTAGE_RELATIONS,
    _SUBSTAGE_ORDER,
)
from agents.agent_owl_generator import prompts
from agents.agent_owl_generator import schemas
from agents.agent_owl_generator.tools import (
    TOOL_DEFINITIONS,
    TOOL_HANDLERS,
    ToolContext,
)
from agents.engine_base import (
    AgentStep,
    accumulate_usage,
    call_serving_endpoint,
    dispatch_tool,
    extract_message_content,
)
from agents.tracing import trace_agent, trace_tool

logger = get_logger(__name__)

# Output-token budget per call (Databricks endpoint output ceiling); shared
# with the deprecated bridge's value so truncation behaviour is identical.
_GEN_MAX_TOKENS = 8192
LLM_TIMEOUT = 180
_TEMPERATURE = 0.0  # deterministic for eval (SPEC §2)

# Stage-1 detection's bounded tool-GATHERING phase (see `detect_entities`'s
# two-phase docstring) may take a few turns before the model stops asking for
# more tools; keep it bounded. The schema-enforced FINALIZATION call that
# follows is always exactly one additional call — never part of this budget.
_MAX_GATHERING_ITERATIONS = 8
# Stage-3 completion is a paged structured exchange. A truncated PAGE gets at
# most one "smaller page" retry (a truncated page's partial JSON is unusable);
# this is never a validation rewrite loop.
_MAX_COMPLETION_ATTEMPTS = 2
# Paged completion (issue #173): each page is a bounded slice of the substage
# answer flagged with ``has_more``. The page budget bounds total completion
# output (~16 × the per-call output cap) so a model that never stops paging
# fails clearly instead of looping forever. Overridable via
# options["max_completion_pages"]; <= 0 falls back to the default.
_MAX_COMPLETION_PAGES = 16
# Candidate over-generation guard (mirrors the class cap). Overridable via
# options["max_classes"]; <= 0 disables.
_DEFAULT_MAX_CANDIDATES = 40


# =====================================================
# Result data classes
# =====================================================


@dataclass
class DetectionResult:
    """Outcome of Stage-1 :func:`detect_entities`."""

    success: bool
    candidate_entities: List[GenerateEntity] = field(default_factory=list)
    steps: List[AgentStep] = field(default_factory=list)
    usage: Dict[str, int] = field(default_factory=dict)
    error: str = ""
    rejected: bool = False


@dataclass
class CompletionResult:
    """Outcome of one Stage-3 substage (relations/attributes/axioms).

    ``rejected`` marks a reject-only outcome — a schema/closure/ordering
    violation reported for the user to retry, never silently rewritten. The
    caller (Task 4) is responsible for checkpoint persistence; this result
    never mutates or persists the draft.
    """

    success: bool
    substage: str = ""
    result: Dict[str, Any] = field(default_factory=dict)
    steps: List[AgentStep] = field(default_factory=list)
    usage: Dict[str, int] = field(default_factory=dict)
    error: str = ""
    rejected: bool = False
    rejection_reason: str = ""


# =====================================================
# Shared helpers
# =====================================================


def _notify(on_step: Optional[Callable[[str], None]], msg: str) -> None:
    if on_step:
        on_step(msg)


def _new_usage() -> Dict[str, int]:
    return {"prompt_tokens": 0, "completion_tokens": 0}


def _build_context(
    *,
    host: str,
    token: str,
    registry: Optional[dict],
    metadata: Optional[dict],
    domain_name: Optional[str],
    domain_folder: Optional[str],
    domain_version: Optional[str],
    warehouse_id: Optional[str],
    selected_tables: Optional[Sequence[str]],
) -> ToolContext:
    md = metadata or {}
    if selected_tables and md.get("tables"):
        filtered = [
            t
            for t in md["tables"]
            if (t.get("full_name") or t.get("name")) in set(selected_tables)
        ]
        if filtered:
            md = {**md, "tables": filtered}
    return ToolContext(
        host=host.rstrip("/"),
        token=token,
        registry=registry or {},
        domain_name=domain_name or "",
        domain_folder=domain_folder or "",
        domain_version=domain_version or "1",
        warehouse_id=warehouse_id or "",
        metadata=md,
    )


@trace_tool()
def _dispatch_detection_tool(
    ctx: ToolContext, tool_name: str, arguments: dict, *, trace_name: str
) -> str:
    """Dispatch one Stage-1 tool call with a per-tool MLflow TOOL span.

    ``dispatch_tool`` (``agents.engine_base``) is shared by ~10 other agent
    engines' ReAct loops; adding a span inside it would change every one of
    their traces. ``trace_tool`` is otherwise applied statically per-handler
    (e.g. ``agent_graph_interpreter.tools.tool_get_entity_details``), which
    does not fit a tool name chosen dynamically per LLM tool call. This local
    wrapper applies the same ``trace_tool`` decorator — scoped to just the
    staged detection tool-dispatch call — with no change to ``dispatch_tool``
    or the shared tool handlers themselves. The positional call signature
    ``(ctx, tool_name, arguments)`` matches ``trace_tool``'s argument-index
    contract exactly.
    """
    return dispatch_tool(TOOL_HANDLERS, ctx, tool_name, arguments, trace_name=trace_name)


# =====================================================
# Stage 1 — detection
# =====================================================


@trace_agent(name="owl_generator.detect", stage="detect")
def detect_entities(
    *,
    host: str,
    token: str,
    endpoint_name: str,
    metadata: Optional[dict] = None,
    guidelines: str = "",
    options: Optional[dict] = None,
    existing_anchors: Sequence[GenerateEntity] = (),
    selected_tables: Optional[List[str]] = None,
    selected_docs: Optional[List[str]] = None,
    registry: Optional[dict] = None,
    domain_name: Optional[str] = None,
    domain_folder: Optional[str] = None,
    domain_version: Optional[str] = None,
    warehouse_id: Optional[str] = None,
    draft_id: Optional[str] = None,
    draft_revision: Optional[int] = None,
    on_step: Optional[Callable[[str], None]] = None,
) -> DetectionResult:
    """Detect candidate entities from metadata + ready corpus + anchors.

    Returns a :class:`DetectionResult` whose ``candidate_entities`` default to
    ``included=true`` / ``origin=detected`` and are deduplicated against the
    locked ``existing_anchors``. Reads documents only through the no-reparse
    tools; never triggers ``ai_parse_document``.

    **Two-phase transport architecture (live-reliability fix)**: a
    prompt-only "JSON only" instruction cannot force a compliant model to
    skip a visible reasoning preamble — live reproduction showed a real
    endpoint still narrating prose ahead of the JSON on most calls despite a
    strengthened prompt. The endpoint contract this fixes rejects combining
    ``tools`` and ``response_format`` in one request, so detection is split
    into:

    1. A bounded tool-GATHERING phase (``tools=`` the metadata/document
       tools, never ``response_format``). Any turn with no tool calls ends
       gathering — its (unstructured, unenforced) content is discarded, NOT
       parsed as the answer. If the endpoint rejects ``tools`` outright
       (400/422), gathering is skipped entirely.
    2. Exactly ONE schema-enforced FINALIZATION call (``tools=None``,
       ``response_format=schemas.DETECTION_RESPONSE_FORMAT``) from the
       accumulated tool-call/result context. This is the only call whose
       content is ever parsed into ``candidate_entities``.

    This is **not** a schema-repair loop: there is exactly one finalization
    call, and the existing reject-only validation (:mod:`schemas`) still
    runs unconditionally on its answer afterwards — a malformed or
    length-truncated finalization answer fails clearly and is never
    re-prompted or retried within this call.
    """
    options = options or {}
    max_candidates = int(options.get("max_classes", _DEFAULT_MAX_CANDIDATES))
    max_candidates = max_candidates if max_candidates > 0 else None

    ctx = _build_context(
        host=host,
        token=token,
        registry=registry,
        metadata=metadata,
        domain_name=domain_name,
        domain_folder=domain_folder,
        domain_version=domain_version,
        warehouse_id=warehouse_id,
        selected_tables=selected_tables,
    )

    messages: List[dict] = [
        {
            "role": "system",
            "content": prompts.build_detection_system_prompt(
                existing_anchors=existing_anchors
            ),
        },
        {
            "role": "user",
            "content": prompts.build_detection_user_prompt(
                guidelines=guidelines,
                selected_tables=selected_tables or [],
                selected_docs=selected_docs or [],
            ),
        },
    ]

    usage = _new_usage()
    steps: List[AgentStep] = []

    _notify(on_step, "Detecting candidate entities…")

    # -----------------------------------------------------------------
    # Phase 1 — bounded tool-GATHERING. `tools=` is always set here, never
    # `response_format` (the endpoint rejects the combination — see
    # `shared.llm_target.build_llm_request`). A turn with no tool calls
    # ends gathering; its content is discarded, never parsed.
    # -----------------------------------------------------------------
    for _iteration in range(_MAX_GATHERING_ITERATIONS):
        try:
            resp = call_serving_endpoint(
                host,
                token,
                endpoint_name,
                messages,
                tools=TOOL_DEFINITIONS,
                max_tokens=_GEN_MAX_TOKENS,
                temperature=_TEMPERATURE,
                timeout=LLM_TIMEOUT,
                trace_name="owl_generator.detect",
            )
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status in (400, 422):
                # Endpoint rejects `tools` outright — skip straight to the
                # single schema-enforced finalization call below (no
                # unstructured single-shot fallback attempt in between).
                _notify(
                    on_step,
                    "Endpoint does not support tools — proceeding to "
                    "schema-enforced detection…",
                )
                break
            return DetectionResult(
                success=False, steps=steps, usage=usage, error=str(exc)
            )
        except requests.exceptions.RequestException as exc:
            return DetectionResult(
                success=False, steps=steps, usage=usage, error=str(exc)
            )

        accumulate_usage(usage, resp.get("usage", {}))
        choice = resp.get("choices", [{}])[0]
        message = choice.get("message", {})
        tool_calls = message.get("tool_calls", [])

        if not tool_calls:
            # Gathering is done. This turn's content (if any) is
            # unstructured and unenforced — it is NEVER parsed as the
            # answer, and is not even kept in the conversation sent to the
            # finalization call below.
            break

        messages.append(message)
        for tc in tool_calls:
            func = tc.get("function", {})
            tool_name = func.get("name", "")
            try:
                arguments = json.loads(func.get("arguments", "{}") or "{}")
            except json.JSONDecodeError:
                arguments = {}
            steps.append(
                AgentStep(
                    step_type="tool_call",
                    content=json.dumps(arguments),
                    tool_name=tool_name,
                )
            )
            t0 = time.time()
            tool_result = _dispatch_detection_tool(
                ctx,
                tool_name,
                arguments,
                trace_name="owl_generator.detect",
            )
            steps.append(
                AgentStep(
                    step_type="tool_result",
                    content=tool_result[:500],
                    tool_name=tool_name,
                    duration_ms=int((time.time() - t0) * 1000),
                )
            )
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tc.get("id", ""),
                    "content": tool_result,
                }
            )
        # Gathering continues for another iteration (budget permitting).

    # -----------------------------------------------------------------
    # Phase 2 — exactly ONE schema-enforced FINALIZATION call. `tools=None`
    # always; `response_format=` the strict detection schema so the
    # transport itself constrains the answer shape (not just the prompt).
    # Reject-only: a malformed or truncated answer fails here, with no
    # second finalization attempt.
    # -----------------------------------------------------------------
    _notify(on_step, "Finalizing detected entities…")
    try:
        resp = call_serving_endpoint(
            host,
            token,
            endpoint_name,
            messages,
            tools=None,
            response_format=schemas.DETECTION_RESPONSE_FORMAT,
            max_tokens=_GEN_MAX_TOKENS,
            temperature=_TEMPERATURE,
            timeout=LLM_TIMEOUT,
            trace_name="owl_generator.detect",
        )
    except requests.exceptions.HTTPError as exc:
        return DetectionResult(success=False, steps=steps, usage=usage, error=str(exc))
    except requests.exceptions.RequestException as exc:
        return DetectionResult(success=False, steps=steps, usage=usage, error=str(exc))

    accumulate_usage(usage, resp.get("usage", {}))
    choice = resp.get("choices", [{}])[0]
    finish_reason = choice.get("finish_reason", "?")
    content = extract_message_content(resp)
    steps.append(AgentStep(step_type="output", content=content[:200]))

    # Truncation guard: a length-capped finalization answer is incomplete
    # JSON. Fail clearly — never re-prompt for a concise re-emission (that
    # would be a rewrite loop on a schema-enforced call).
    if finish_reason == "length":
        return DetectionResult(
            success=False,
            steps=steps,
            usage=usage,
            error="Detection output was truncated and could not be completed.",
        )

    try:
        candidates = schemas.parse_detection_payload(
            content,
            existing_anchors=existing_anchors,
            max_candidates=max_candidates,
        )
    except schemas.SchemaValidationError as exc:
        # Reject-only: report the malformed answer; do not re-prompt.
        return DetectionResult(
            success=False,
            steps=steps,
            usage=usage,
            error=str(exc),
            rejected=True,
        )

    _notify(on_step, f"Detected {len(candidates)} candidate ent(y/ies).")
    return DetectionResult(
        success=True,
        candidate_entities=candidates,
        steps=steps,
        usage=usage,
    )


# =====================================================
# Stage 3 — completion (relations -> attributes -> axioms)
# =====================================================


def _completion_response_format(
    builder: Callable[[Any], Dict[str, Any]], draft: GenerateDraft
) -> Optional[Dict[str, Any]]:
    """Build a substage's enum-constrained ``response_format`` from the
    draft's own closed entity set (live-bug fix — see the module note in
    ``schemas.py``'s "Transport-level structured output for Stage 3"
    section). Returns ``None`` when the closed set is empty (an edge case
    ``ensure_ready_for_completion`` already prevents in the production
    workflow path) rather than building a schema with an impossible empty
    ``enum`` — completion then relies on the strengthened bare-id prompt
    (``prompts.py``) plus the unconditional ``validate_references`` check.
    """
    closed_ids = draft.closed_entity_ids()
    if not closed_ids:
        return None
    return builder(closed_ids)


def _ordering_error(draft: GenerateDraft, substage: str) -> str:
    """Return a non-empty reason string when *substage* may not start yet."""
    idx = _SUBSTAGE_ORDER.index(substage)
    for predecessor in _SUBSTAGE_ORDER[:idx]:
        status = (draft.completion_checkpoints.get(predecessor) or {}).get("status")
        if status != CHECKPOINT_DONE:
            return (
                f"Cannot start {substage!r} before {predecessor!r} is done "
                f"(current status: {status!r})."
            )
    return ""


def _accept_completion_page(
    content: str,
    *,
    substage: str,
    draft: GenerateDraft,
    items_key: str,
    key_fn: Callable[[Dict[str, Any]], Any],
    parse_fn: Callable[[str], Dict[str, Any]],
    refs_fn: Callable[[Dict[str, Any]], set],
    items: List[Dict[str, Any]],
    seen: set,
) -> tuple[bool, str]:
    """Parse, closure-check, and merge one completion page.

    Returns ``(has_more, error)`` — a non-empty ``error`` is a reject-only
    validation failure (malformed JSON or an out-of-closure entity id),
    never a rewrite prompt. Otherwise the page's NEW items (by ``key_fn``)
    are appended to ``items`` and recorded in ``seen``.
    """
    try:
        page_result = parse_fn(content)
    except schemas.SchemaValidationError as exc:
        return False, str(exc)

    page_items = page_result.get(items_key) or []

    # Entity-closure check on every page (reject-only). A reference outside
    # the locked anchors ∪ included candidates — including an excluded
    # candidate id — is rejected outright; the output is never checkpointed,
    # merged, or re-sent to the LLM for a rewrite.
    try:
        draft.validate_references(
            refs_fn({items_key: page_items}), context=substage
        )
    except DraftValidationError as exc:
        return False, str(exc)

    new_items = [it for it in page_items if key_fn(it) not in seen]
    for it in new_items:
        seen.add(key_fn(it))
    items.extend(new_items)
    return bool(page_result.get("has_more")), ""


def _call_completion_page(
    host: str,
    token: str,
    endpoint_name: str,
    messages: List[dict],
    response_format: Optional[Dict[str, Any]],
    trace_name: str,
) -> Dict[str, Any]:
    """One paged-completion LLM call — ``tools=None`` always (completion
    never reads documents/metadata)."""
    return call_serving_endpoint(
        host,
        token,
        endpoint_name,
        messages,
        tools=None,
        response_format=response_format,
        max_tokens=_GEN_MAX_TOKENS,
        temperature=_TEMPERATURE,
        timeout=LLM_TIMEOUT,
        trace_name=trace_name,
    )


def _run_completion(
    *,
    substage: str,
    host: str,
    token: str,
    endpoint_name: str,
    draft: GenerateDraft,
    system_prompt: str,
    user_prompt: str,
    items_key: str,
    key_fn: Callable[[Dict[str, Any]], Any],
    parse_fn: Callable[[str], Dict[str, Any]],
    refs_fn: Callable[[Dict[str, Any]], set],
    response_format: Optional[Dict[str, Any]],
    on_step: Optional[Callable[[str], None]],
    finalize_fn: Optional[Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]] = None,
    options: Optional[Dict[str, Any]] = None,
) -> CompletionResult:
    """Run one Stage-3 completion substage as a paged structured call.

    Paged output (issue #173): each response is ONE bounded page carrying
    ``has_more``; pages are merged/deduped by ``key_fn``, a ``has_more``
    page contributing zero NEW items ends the loop, and
    ``options["max_completion_pages"]`` bounds the total. Reject-only —
    ``validate_references`` still runs on every page.
    """
    # Strict ordering gate — fail fast, before any LLM call.
    ordering_reason = _ordering_error(draft, substage)
    if ordering_reason:
        return CompletionResult(
            success=False,
            substage=substage,
            error=ordering_reason,
            rejected=True,
            rejection_reason=ordering_reason,
        )

    options = options or {}
    max_pages = int(options.get("max_completion_pages") or 0)
    max_pages = max_pages if max_pages > 0 else _MAX_COMPLETION_PAGES

    messages: List[dict] = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    usage = _new_usage()
    steps: List[AgentStep] = []
    trace_name = f"owl_generator.{substage}"
    items: List[Dict[str, Any]] = []
    seen: set = set()

    _notify(on_step, f"Inferring {substage}…")

    page = page_trunc_retries = 0
    while True:
        try:
            resp = _call_completion_page(
                host, token, endpoint_name, messages, response_format, trace_name
            )
        except requests.exceptions.RequestException as exc:
            return _completion_failure(substage, steps, usage, str(exc))

        accumulate_usage(usage, resp.get("usage", {}))
        choice = resp.get("choices", [{}])[0]
        content = extract_message_content(resp)
        steps.append(AgentStep(step_type="output", content=content[:200]))

        if choice.get("finish_reason", "?") == "length":
            # A page that ignored the bounded-page contract gets ONE retry
            # asking for a smaller page — then the substage fails clearly
            # (the pre-#173 "re-emit the complete JSON" retry could never
            # succeed for an answer that cannot fit the output budget).
            if page_trunc_retries < _MAX_COMPLETION_ATTEMPTS - 1:
                page_trunc_retries += 1
                _queue_truncation_retry(messages, content, substage)
                continue
            return _completion_failure(
                substage, steps, usage,
                f"{substage} output page was truncated and could not be "
                "completed; reduce the entity set or retry.",
            )
        page_trunc_retries = 0
        count_before = len(items)
        has_more, error = _accept_completion_page(
            content,
            substage=substage,
            draft=draft,
            items_key=items_key,
            key_fn=key_fn,
            parse_fn=parse_fn,
            refs_fn=refs_fn,
            items=items,
            seen=seen,
        )
        if error:
            return _completion_failure(
                substage, steps, usage, error, rejected=True
            )

        if not has_more or len(items) == count_before:
            break
        page += 1
        if page >= max_pages:
            return _completion_failure(
                substage, steps, usage,
                f"{substage} did not finish within the {max_pages}-page "
                "completion budget; reduce the entity set or retry.",
            )
        _notify(on_step, f"Inferring {substage}… ({len(items)} items so far)")
        _queue_next_page(messages, content, substage, len(items))

    merged = finalize_fn(items) if finalize_fn else items
    return CompletionResult(
        success=True,
        substage=substage,
        result={items_key: merged},
        steps=steps,
        usage=usage,
    )


def _completion_failure(
    substage: str,
    steps: List[AgentStep],
    usage: Dict[str, int],
    error: str,
    *,
    rejected: bool = False,
) -> CompletionResult:
    return CompletionResult(
        success=False, substage=substage, steps=steps, usage=usage,
        error=error, rejected=rejected,
        rejection_reason=error if rejected else "",
    )


def _queue_truncation_retry(
    messages: List[dict], content: str, substage: str
) -> None:
    """Re-ask for the SAME page with fewer items after a length truncation."""
    messages += [
        {"role": "assistant", "content": content},
        {
            "role": "user",
            "content": prompts.build_truncated_page_retry_prompt(substage),
        },
    ]


def _queue_next_page(
    messages: List[dict], content: str, substage: str, emitted: int
) -> None:
    """Ask for the next page of a completion substage."""
    messages += [
        {"role": "assistant", "content": content},
        {
            "role": "user",
            "content": prompts.build_continuation_user_prompt(substage, emitted),
        },
    ]


@trace_agent(name="owl_generator.relations", stage="relations")
def infer_relations(
    *,
    host: str,
    token: str,
    endpoint_name: str,
    draft: GenerateDraft,
    options: Optional[dict] = None,
    draft_id: Optional[str] = None,
    draft_revision: Optional[int] = None,
    on_step: Optional[Callable[[str], None]] = None,
) -> CompletionResult:
    """Stage 3a: infer object-property relations over the closed entity set.

    ``options["existing_relations"]`` (``{label, domain, range}`` over anchor
    ids) are shown to the model and any restatement of them is dropped.
    """
    existing = list((options or {}).get("existing_relations") or [])
    return _run_completion(
        substage=SUBSTAGE_RELATIONS,
        host=host,
        token=token,
        endpoint_name=endpoint_name,
        draft=draft,
        system_prompt=prompts.build_relations_system_prompt(),
        user_prompt=prompts.build_relations_user_prompt(draft, existing),
        items_key="relations",
        key_fn=lambda rel: (
            str(rel.get("label") or "").casefold(),
            str(rel.get("domain") or ""),
            str(rel.get("range") or ""),
        ),
        parse_fn=lambda text: schemas.parse_relations_payload(text, existing),
        refs_fn=schemas.relations_referenced_ids,
        response_format=_completion_response_format(
            schemas.build_relations_response_format, draft
        ),
        finalize_fn=lambda items: schemas.drop_inverse_relations(items, existing),
        options=options,
        on_step=on_step,
    )


@trace_agent(name="owl_generator.attributes", stage="attributes")
def infer_attributes(
    *,
    host: str,
    token: str,
    endpoint_name: str,
    draft: GenerateDraft,
    options: Optional[dict] = None,
    draft_id: Optional[str] = None,
    draft_revision: Optional[int] = None,
    on_step: Optional[Callable[[str], None]] = None,
) -> CompletionResult:
    """Stage 3b: infer datatype attributes (requires relations done).

    Paged (issue #173): a wide table's attributes — or attributes across a
    40-entity draft — cannot fit one completion; the answer arrives in
    ``has_more``-flagged pages, merged and deduped by (label, domain).
    """
    return _run_completion(
        substage=SUBSTAGE_ATTRIBUTES,
        host=host,
        token=token,
        endpoint_name=endpoint_name,
        draft=draft,
        system_prompt=prompts.build_attributes_system_prompt(),
        user_prompt=prompts.build_attributes_user_prompt(draft),
        items_key="attributes",
        key_fn=lambda attr: (
            str(attr.get("label") or "").casefold(),
            str(attr.get("domain") or ""),
        ),
        parse_fn=schemas.parse_attributes_payload,
        refs_fn=schemas.attributes_referenced_ids,
        response_format=_completion_response_format(
            schemas.build_attributes_response_format, draft
        ),
        options=options,
        on_step=on_step,
    )


@trace_agent(name="owl_generator.axioms", stage="axioms")
def infer_axioms(
    *,
    host: str,
    token: str,
    endpoint_name: str,
    draft: GenerateDraft,
    options: Optional[dict] = None,
    draft_id: Optional[str] = None,
    draft_revision: Optional[int] = None,
    on_step: Optional[Callable[[str], None]] = None,
) -> CompletionResult:
    """Stage 3c: infer lightweight axioms (requires attributes done)."""
    return _run_completion(
        substage=SUBSTAGE_AXIOMS,
        host=host,
        token=token,
        endpoint_name=endpoint_name,
        draft=draft,
        system_prompt=prompts.build_axioms_system_prompt(),
        user_prompt=prompts.build_axioms_user_prompt(draft),
        items_key="axioms",
        key_fn=lambda ax: (
            str(ax.get("kind") or ""),
            str(ax.get("subject") or ""),
            str(ax.get("object") or ""),
        ),
        parse_fn=lambda text: schemas.parse_axioms_payload(
            text, existing_parents=draft.existing_parents()
        ),
        refs_fn=schemas.axioms_referenced_ids,
        response_format=_completion_response_format(
            schemas.build_axioms_response_format, draft
        ),
        finalize_fn=lambda items: schemas.drop_multi_parent_subclass_axioms(
            items, draft.existing_parents()
        ),
        options=options,
        on_step=on_step,
    )
