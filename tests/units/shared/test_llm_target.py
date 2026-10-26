"""Tests for shared Databricks LLM target routing."""

import pytest

from shared.llm_target import (
    AI_GATEWAY,
    SERVING,
    build_llm_request,
    normalize_llm_endpoint_kind,
)


def test_explicit_kind_wins_over_name_shape():
    assert normalize_llm_endpoint_kind("a.b.c", SERVING) == SERVING


def test_legacy_fqn_is_inferred_as_gateway():
    assert normalize_llm_endpoint_kind("main.ai.monclaudesonnetamoi") == AI_GATEWAY
    assert normalize_llm_endpoint_kind("databricks-claude-sonnet-4-5") == SERVING


def test_gateway_request_uses_mlflow_chat_completions():
    url, payload = build_llm_request(
        "https://workspace/",
        "main.ai.monclaudesonnetamoi",
        AI_GATEWAY,
        [{"role": "user", "content": "hello"}],
        max_tokens=256,
        temperature=0.1,
    )

    assert url == "https://workspace/ai-gateway/mlflow/v1/chat/completions"
    assert payload["model"] == "main.ai.monclaudesonnetamoi"


def test_serving_request_keeps_invocations_contract():
    url, payload = build_llm_request(
        "https://workspace",
        "databricks-claude-sonnet-4-5",
        SERVING,
        [],
        max_tokens=256,
        temperature=None,
    )

    assert url.endswith(
        "/serving-endpoints/databricks-claude-sonnet-4-5/invocations"
    )
    assert "model" not in payload
    assert "temperature" not in payload


def test_request_includes_tools_when_provided():
    tools = [{"type": "function", "function": {"name": "lookup"}}]
    _, payload = build_llm_request(
        "https://workspace",
        "main.ai.service",
        AI_GATEWAY,
        [],
        max_tokens=256,
        temperature=0.1,
        tools=tools,
    )

    assert payload["tools"] == tools


# ---------------------------------------------------------------------------
# Transport-level structured output (response_format) — opt-in pass-through.
#
# Root cause of the residual live Stage-1 unreliability this fixes: prompt-
# only "JSON only" instructions cannot force a compliant model to skip a
# visible reasoning preamble. The user's own probing of the active endpoint
# confirmed it accepts OpenAI/Databricks-style
# ``response_format={"type": "json_schema", "json_schema": ...}`` and
# rejects combining ``response_format`` with ``tools`` in the same request.
# This is a pure opt-in addition — every other caller that never passes
# ``response_format`` gets byte-identical payloads (see the tests above).
# ---------------------------------------------------------------------------


def test_response_format_is_included_when_provided():
    response_format = {"type": "json_schema", "json_schema": {"name": "x", "schema": {}}}
    _, payload = build_llm_request(
        "https://workspace",
        "databricks-claude-sonnet-5",
        SERVING,
        [],
        max_tokens=256,
        temperature=0.0,
        response_format=response_format,
    )

    assert payload["response_format"] == response_format


def test_response_format_absent_by_default():
    _, payload = build_llm_request(
        "https://workspace",
        "databricks-claude-sonnet-5",
        SERVING,
        [],
        max_tokens=256,
        temperature=0.0,
    )

    assert "response_format" not in payload


def test_raises_when_tools_and_response_format_both_provided():
    # The endpoint contract this fixes rejects combining `tools` and
    # `response_format` in one request ("Cannot specify both response_format
    # and tools"). Enforce that as a hard, fail-fast client-side guard —
    # never let a caller silently send an invalid combination over the wire.
    tools = [{"type": "function", "function": {"name": "lookup"}}]
    response_format = {"type": "json_schema", "json_schema": {"name": "x", "schema": {}}}
    with pytest.raises(ValueError, match="tools.*response_format|response_format.*tools"):
        build_llm_request(
            "https://workspace",
            "databricks-claude-sonnet-5",
            SERVING,
            [],
            max_tokens=256,
            temperature=0.0,
            tools=tools,
            response_format=response_format,
        )
