"""Sanity Checks Auto-fix agent: propose a typed FixPlan (read-only)."""

from __future__ import annotations

import json
import time
from typing import Any, Callable, Dict, List, Optional

from agents.agent_design_fixer.AgentResult import AgentResult
from agents.agent_design_fixer.tools import TOOL_DEFINITIONS, TOOL_HANDLERS
from agents.engine_base import (
    AgentStep,
    accumulate_usage,
    call_serving_endpoint,
    dispatch_tool,
)
from agents.tools.context import ToolContext
from agents.tracing import trace_agent
from back.core.logging import get_logger
from back.objects.ontology.autofix_ops import ALLOWED_OPS

logger = get_logger(__name__)

MAX_ITERATIONS = 6
LLM_TIMEOUT = 120
MAX_TOKENS = 2048
_TRACE_NAME = "design_fixer_agent"

SYSTEM_PROMPT = """\
You are an ontology design fixer for OntoBricks Sanity Checks.

Given ONE finding and read-only tools, propose a FixPlan. You NEVER mutate
the ontology. Finish by calling submit_fix_plan exactly once.

ALLOWED OPERATIONS (op field):
  clear_parent, set_parent, clear_domain, set_domain, clear_range, set_range,
  remove_group_member, attach_attribute, remove_property, set_label,
  set_description, rename, merge_into

PLAN SHAPE for submit_fix_plan:
{
  "rule": "<finding rule id>",
  "summary": "<one sentence>",
  "operations": [ { "op", "target", "params", "rationale", "selected": true } ],
  "alternatives": [ { "op", "target", "params", "rationale", "selected": false } ]
}

RULES
  * Every op needs a non-empty English rationale.
  * Prefer the best fix in operations (selected true); put 1–2 alternatives
    in alternatives.
  * For DC-W03 (missing domain / orphan attribute): ALWAYS include a
    remove_property alternative.
  * Do not invent operation types outside the allowed list.
  * Use tools to inspect the ontology before deciding when unsure.
"""


def _parse_arguments(raw_args) -> dict:
    if not isinstance(raw_args, str):
        return raw_args or {}
    try:
        return json.loads(raw_args)
    except json.JSONDecodeError:
        return {}


def _emit(result: AgentResult, step: AgentStep, on_event) -> None:
    result.steps.append(step)
    if on_event:
        on_event(step)


def _run_tool_calls(
    tool_calls: list, ctx: ToolContext, result: AgentResult, on_event
) -> List[dict]:
    tool_messages: List[dict] = []
    for tc in tool_calls:
        func = tc.get("function", {})
        tool_name = func.get("name", "")
        arguments = _parse_arguments(func.get("arguments", "{}"))
        _emit(
            result,
            AgentStep(
                step_type="tool_call",
                content=json.dumps(arguments, default=str),
                tool_name=tool_name,
            ),
            on_event,
        )
        t0 = time.perf_counter()
        content = dispatch_tool(
            TOOL_HANDLERS,
            ctx,
            tool_name,
            arguments,
            trace_name=_TRACE_NAME,
        )
        duration_ms = int((time.perf_counter() - t0) * 1000)
        _emit(
            result,
            AgentStep(
                step_type="tool_result",
                content=content,
                tool_name=tool_name,
                duration_ms=duration_ms,
            ),
            on_event,
        )
        tool_messages.append(
            {
                "role": "tool",
                "tool_call_id": tc.get("id", tool_name),
                "content": content,
            }
        )
    return tool_messages


def _user_message(finding: Dict[str, Any]) -> str:
    return (
        "FINDING (JSON)\n"
        + json.dumps(finding, ensure_ascii=False)
        + "\n\nPropose a FixPlan and call submit_fix_plan."
    )


def _normalize_plan(plan: Dict[str, Any], finding: Dict[str, Any]) -> Dict[str, Any]:
    """Fill missing target/finding_id defaults from the finding."""
    fid = (
        f"{finding.get('rule')}:{finding.get('kind')}:{finding.get('name')}"
    )
    target = {
        "kind": finding.get("kind") or "",
        "name": finding.get("name") or "",
    }
    if finding.get("domain"):
        target["domain"] = finding["domain"]

    def _fill(ops: list, selected_default: bool) -> list:
        out = []
        for i, op in enumerate(ops or []):
            if not isinstance(op, dict):
                continue
            row = dict(op)
            row.setdefault("target", dict(target))
            row.setdefault("params", {})
            row.setdefault("source", "ai")
            row.setdefault("finding_id", fid)
            row.setdefault("selected", selected_default)
            row.setdefault("id", f"op-ai-{i+1}")
            out.append(row)
        return out

    return {
        "rule": plan.get("rule") or finding.get("rule") or "",
        "summary": plan.get("summary") or "",
        "operations": _fill(plan.get("operations") or [], True),
        "alternatives": _fill(plan.get("alternatives") or [], False),
    }


@trace_agent(_TRACE_NAME)
def run_agent(
    finding: Dict[str, Any],
    classes: List[Dict[str, Any]],
    properties: List[Dict[str, Any]],
    groups: List[Dict[str, Any]],
    *,
    host: str,
    token: str,
    endpoint: str,
    endpoint_kind: str = "",
    on_event: Optional[Callable[[AgentStep], None]] = None,
    call_llm: Optional[Callable[..., dict]] = None,
) -> AgentResult:
    """Run the design-fixer loop and return a FixPlan on success."""
    result = AgentResult(success=False, llm_available=True)
    if not finding or not finding.get("rule"):
        result.error = "Missing finding."
        result.llm_available = False
        return result

    ctx = ToolContext(
        host=host or "",
        token=token or "",
        ontology_classes=list(classes or []),
        ontology_properties=list(properties or []),
        ontology_groups=list(groups or []),
    )
    llm = call_llm or call_serving_endpoint
    messages: List[dict] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": _user_message(finding)},
    ]

    try:
        for _ in range(MAX_ITERATIONS):
            response = llm(
                host,
                token,
                endpoint,
                messages,
                tools=TOOL_DEFINITIONS,
                max_tokens=MAX_TOKENS,
                temperature=0.0,
                timeout=LLM_TIMEOUT,
                trace_name=_TRACE_NAME,
                endpoint_kind=endpoint_kind,
            )
            accumulate_usage(result.usage, response)
            choice = (response.get("choices") or [{}])[0]
            message = choice.get("message") or {}
            tool_calls = message.get("tool_calls") or []
            if tool_calls:
                messages.append(message)
                messages.extend(_run_tool_calls(tool_calls, ctx, result, on_event))
                if ctx.submitted_fix_plan:
                    break
                continue
            # No tool calls — stop; plan must come from submit_fix_plan
            break

        if not ctx.submitted_fix_plan:
            result.error = "Agent did not submit a FixPlan."
            return result

        plan = _normalize_plan(ctx.submitted_fix_plan, finding)
        # Validate ops again after normalize
        for op in plan["operations"] + plan["alternatives"]:
            if op.get("op") not in ALLOWED_OPS:
                result.error = f"Invalid op in plan: {op.get('op')}"
                return result
        result.plan = plan
        result.success = True
        _emit(
            result,
            AgentStep(
                step_type="output",
                content=json.dumps(plan, ensure_ascii=False),
            ),
            on_event,
        )
        return result
    except Exception as exc:  # noqa: BLE001
        logger.exception("design_fixer agent failed")
        result.error = str(exc)
        return result
