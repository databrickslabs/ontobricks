"""
Shared infrastructure for OntoBricks agent engines.

Provides the common ``AgentStep`` dataclass and reusable helpers for LLM
serving-endpoint calls, tool dispatch, response content extraction, and
token usage accumulation.  Each concrete agent engine imports what it needs
and focuses exclusively on its own ``AgentResult``, system prompt, and
``run_agent`` loop.
"""

import json
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from back.core.logging import get_logger
from agents.llm_utils import call_llm_with_retry

logger = get_logger(__name__)


# =====================================================
# Shared data class
# =====================================================


@dataclass
class AgentStep:
    """One observable step of the agent's execution."""

    step_type: str  # tool_call | tool_result | output
    content: str
    tool_name: str = ""
    duration_ms: int = 0


# =====================================================
# LLM call helper
# =====================================================


def call_serving_endpoint(
    host: str,
    token: str,
    endpoint_name: str,
    messages: List[dict],
    *,
    tools: Optional[List[dict]] = None,
    max_tokens: int = 2048,
    temperature: float = 0.1,
    timeout: int = 180,
    trace_name: str = "agent:llm",
) -> dict:
    """Call a Databricks serving endpoint (OpenAI-compatible chat completions).

    Builds the URL, headers, and payload, then delegates to
    :func:`call_llm_with_retry` for retry/backoff logic.

    Args:
        trace_name: Used for MLflow span naming via ``@trace_llm``.
    """
    url = f"{host.rstrip('/')}/serving-endpoints/{endpoint_name}/invocations"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload: Dict[str, Any] = {
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    if tools:
        payload["tools"] = tools

    logger.info(
        "%s: POST %s — %d messages, %d tool defs, max_tokens=%d",
        trace_name,
        endpoint_name,
        len(messages),
        len(tools) if tools else 0,
        max_tokens,
    )

    resp = call_llm_with_retry(url, headers, payload, timeout=timeout)
    return resp.json()


# =====================================================
# Tool dispatch helper
# =====================================================


def dispatch_tool(
    handlers: Dict[str, Callable],
    ctx: Any,
    tool_name: str,
    arguments: dict,
    *,
    trace_name: str = "agent:tool",
) -> str:
    """Dispatch a tool call and return the JSON result string.

    Handles unknown tools and exceptions uniformly across agents.
    """
    handler = handlers.get(tool_name)
    if not handler:
        logger.warning(
            "%s: unknown tool '%s' — available: %s",
            trace_name,
            tool_name,
            list(handlers.keys()),
        )
        return json.dumps({"error": f"Unknown tool: {tool_name}"})
    try:
        t0 = time.time()
        result = handler(ctx, **arguments)
        elapsed = int((time.time() - t0) * 1000)
        logger.info(
            "%s: '%s' completed in %dms, returned %d chars",
            trace_name,
            tool_name,
            elapsed,
            len(result),
        )
        return result
    except Exception as exc:
        logger.exception("%s: '%s' raised exception: %s", trace_name, tool_name, exc)
        return json.dumps({"error": f"Tool execution failed: {exc}"})


# =====================================================
# Response content extraction
# =====================================================


def extract_message_content(llm_response: dict) -> str:
    """Extract text content from an OpenAI-style or predictions-style LLM response."""
    choices = llm_response.get("choices", [])
    if choices:
        return choices[0].get("message", {}).get("content", "") or ""
    preds = llm_response.get("predictions", [])
    if preds:
        return preds[0] if isinstance(preds[0], str) else str(preds[0])
    logger.warning(
        "extract_message_content: no choices or predictions, keys=%s",
        list(llm_response.keys()),
    )
    return ""


# =====================================================
# Token usage accumulation
# =====================================================


def accumulate_usage(total: Dict[str, int], usage_block: dict) -> None:
    """Add prompt/completion token counts from *usage_block* into *total* in-place."""
    total["prompt_tokens"] = total.get("prompt_tokens", 0) + usage_block.get(
        "prompt_tokens", 0
    )
    total["completion_tokens"] = total.get("completion_tokens", 0) + usage_block.get(
        "completion_tokens", 0
    )
