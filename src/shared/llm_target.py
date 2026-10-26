"""Shared routing contract for Databricks LLM targets."""

from __future__ import annotations

from typing import Any

AI_GATEWAY = "ai_gateway"
SERVING = "serving"
VALID_LLM_ENDPOINT_KINDS = frozenset({AI_GATEWAY, SERVING})


def normalize_llm_endpoint_kind(endpoint_name: str, endpoint_kind: str = "") -> str:
    """Return an explicit kind, inferring legacy Gateway FQNs when absent."""
    kind = str(endpoint_kind or "").strip().lower()
    if kind in VALID_LLM_ENDPOINT_KINDS:
        return kind
    name = str(endpoint_name or "").strip()
    return AI_GATEWAY if name.count(".") == 2 and "/" not in name else SERVING


def build_llm_request(
    host: str,
    endpoint_name: str,
    endpoint_kind: str,
    messages: list[dict],
    *,
    max_tokens: int,
    temperature: float | None,
    tools: list[dict] | None = None,
    response_format: dict | None = None,
) -> tuple[str, dict[str, Any]]:
    """Build the URL and OpenAI-compatible body for an LLM target.

    ``response_format`` is an opt-in OpenAI/Databricks-style structured-output
    directive (e.g. ``{"type": "json_schema", "json_schema": {...}}``) — it is
    only added to the payload when a caller explicitly passes it, so every
    other caller's payload is byte-identical to before. The endpoint contract
    this supports rejects combining ``tools`` and ``response_format`` in the
    same request ("Cannot specify both response_format and tools"), so that
    combination is rejected client-side, fail-fast, before ever reaching the
    network.
    """
    if tools and response_format:
        raise ValueError(
            "Cannot combine 'tools' and 'response_format' in the same LLM "
            "request — the endpoint contract rejects that combination. Use "
            "tools for bounded tool-gathering, then a separate tools=None "
            "call with response_format for schema-enforced finalization."
        )
    name = str(endpoint_name or "").strip()
    kind = normalize_llm_endpoint_kind(name, endpoint_kind)
    payload: dict[str, Any] = {"messages": messages, "max_tokens": max_tokens}
    if temperature is not None:
        payload["temperature"] = temperature
    if tools:
        payload["tools"] = tools
    if response_format:
        payload["response_format"] = response_format

    base = host.rstrip("/")
    if kind == AI_GATEWAY:
        payload["model"] = name
        return f"{base}/ai-gateway/mlflow/v1/chat/completions", payload
    return f"{base}/serving-endpoints/{name}/invocations", payload
