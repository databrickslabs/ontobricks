"""Tests for agents.engine_base – shared agent infrastructure."""

import copy
import json
import pytest
import requests
from unittest.mock import patch, MagicMock
from dataclasses import asdict

from agents.engine_base import (
    AgentStep,
    call_serving_endpoint,
    dispatch_tool,
    extract_message_content,
    accumulate_usage,
)


class TestAgentStep:
    def test_defaults(self):
        step = AgentStep(step_type="output", content="hello")
        assert step.step_type == "output"
        assert step.content == "hello"
        assert step.tool_name == ""
        assert step.duration_ms == 0

    def test_tool_call_step(self):
        step = AgentStep(
            step_type="tool_call", content="result", tool_name="get_ontology", duration_ms=42
        )
        assert step.tool_name == "get_ontology"
        assert step.duration_ms == 42

    def test_is_dataclass(self):
        step = AgentStep(step_type="output", content="x")
        d = asdict(step)
        assert d == {"step_type": "output", "content": "x", "tool_name": "", "duration_ms": 0}


class TestCallServingEndpoint:
    @patch("agents.engine_base.call_llm_with_retry")
    def test_builds_url_and_calls(self, mock_retry):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"choices": [{"message": {"content": "hi"}}]}
        mock_retry.return_value = mock_resp

        result = call_serving_endpoint(
            "https://host.databricks.com",
            "tok",
            "my-endpoint",
            [{"role": "user", "content": "hello"}],
        )

        mock_retry.assert_called_once()
        call_args = mock_retry.call_args
        assert "my-endpoint/invocations" in call_args[0][0]
        assert call_args[0][1]["Authorization"] == "Bearer tok"
        assert result == {"choices": [{"message": {"content": "hi"}}]}

    @patch("agents.engine_base.call_llm_with_retry")
    def test_includes_tools_when_provided(self, mock_retry):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {}
        mock_retry.return_value = mock_resp

        tools = [{"type": "function", "function": {"name": "get_data"}}]
        call_serving_endpoint(
            "https://host.databricks.com/",
            "tok",
            "ep",
            [],
            tools=tools,
        )

        payload = mock_retry.call_args[0][2]
        assert payload["tools"] == tools

    @patch("agents.engine_base.call_llm_with_retry")
    def test_no_tools_key_when_none(self, mock_retry):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {}
        mock_retry.return_value = mock_resp

        call_serving_endpoint("https://h", "t", "ep", [])
        payload = mock_retry.call_args[0][2]
        assert "tools" not in payload

    @patch("agents.engine_base.call_llm_with_retry")
    def test_strips_trailing_slash(self, mock_retry):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {}
        mock_retry.return_value = mock_resp

        call_serving_endpoint("https://host.com/", "t", "ep", [])
        url = mock_retry.call_args[0][0]
        assert "//serving" not in url

    @patch("agents.engine_base.call_llm_with_retry")
    def test_gateway_uses_chat_completions_and_model(self, mock_retry):
        mock_retry.return_value.json.return_value = {"choices": []}

        call_serving_endpoint(
            "https://host",
            "tok",
            "main.ai.mine",
            [],
            endpoint_kind="ai_gateway",
        )

        url, _, payload = mock_retry.call_args.args[:3]
        assert url == "https://host/ai-gateway/mlflow/v1/chat/completions"
        assert payload["model"] == "main.ai.mine"

    @patch("agents.engine_base.call_llm_with_retry")
    def test_explicit_serving_kind_allows_dotted_name(self, mock_retry):
        mock_retry.return_value.json.return_value = {}

        call_serving_endpoint(
            "https://host",
            "tok",
            "legacy.with.dots",
            [],
            endpoint_kind="serving",
        )

        assert "/serving-endpoints/legacy.with.dots/invocations" in (
            mock_retry.call_args.args[0]
        )

    # -----------------------------------------------------------------
    # Transport-level structured output (response_format) — opt-in, with
    # safe fallback/caching when the endpoint rejects it with a clear 400.
    # Root cause this supports: prompt-only "JSON only" instructions cannot
    # force a compliant model to skip a visible reasoning preamble; the
    # user's own endpoint accepts response_format={"type": "json_schema",
    # ...} and rejects combining it with `tools`. This is opt-in — every
    # other existing caller (which never passes response_format) is
    # unaffected, per the tests above.
    # -----------------------------------------------------------------

    @patch("agents.engine_base.call_llm_with_retry")
    def test_includes_response_format_when_provided(self, mock_retry):
        mock_retry.return_value.json.return_value = {}
        response_format = {"type": "json_schema", "json_schema": {"name": "x", "schema": {}}}

        call_serving_endpoint(
            "https://host", "tok", "ep", [], response_format=response_format
        )

        payload = mock_retry.call_args[0][2]
        assert payload["response_format"] == response_format

    @patch("agents.engine_base.call_llm_with_retry")
    def test_no_response_format_key_when_none(self, mock_retry):
        mock_retry.return_value.json.return_value = {}

        call_serving_endpoint("https://host", "tok", "ep", [])

        payload = mock_retry.call_args[0][2]
        assert "response_format" not in payload

    def test_raises_when_tools_and_response_format_both_given(self):
        # Enforced fail-fast, before any network call — never let a caller
        # silently send the endpoint an invalid tools+response_format
        # combination.
        tools = [{"type": "function", "function": {"name": "lookup"}}]
        response_format = {"type": "json_schema", "json_schema": {"name": "x", "schema": {}}}
        with patch("agents.engine_base.call_llm_with_retry") as mock_retry:
            with pytest.raises(ValueError):
                call_serving_endpoint(
                    "https://host",
                    "tok",
                    "ep",
                    [],
                    tools=tools,
                    response_format=response_format,
                )
            mock_retry.assert_not_called()

    @patch("agents.engine_base.call_llm_with_retry")
    def test_unsupported_response_format_is_stripped_and_retried(self, mock_retry):
        # First attempt: the endpoint rejects response_format with a 400
        # naming it explicitly (mirrors the real
        # "does not support the temperature parameter" 400 body shape this
        # retry pattern already handles for `temperature`). Second attempt
        # (payload with response_format stripped) succeeds. `payload` is
        # mutated in place between attempts, so snapshot a deep copy on each
        # call rather than reading it back from `call_args_list` afterwards.
        rejection = MagicMock()
        rejection.status_code = 400
        rejection.text = (
            '{"error_code":"BAD_REQUEST","message":"BAD_REQUEST: Model x '
            'does not support the response_format parameter."}'
        )
        http_error = requests.exceptions.HTTPError(response=rejection)
        success_resp = MagicMock()
        success_resp.json.return_value = {"choices": [{"message": {"content": "ok"}}]}
        seen_payloads = []

        def _record(_url, _headers, payload, timeout=None):
            seen_payloads.append(copy.deepcopy(payload))
            if len(seen_payloads) == 1:
                raise http_error
            return success_resp

        mock_retry.side_effect = _record

        response_format = {"type": "json_schema", "json_schema": {"name": "x", "schema": {}}}
        result = call_serving_endpoint(
            "https://host", "tok", "ep", [], response_format=response_format
        )

        assert result == {"choices": [{"message": {"content": "ok"}}]}
        assert mock_retry.call_count == 2
        assert seen_payloads[0]["response_format"] == response_format
        assert "response_format" not in seen_payloads[1]

    @patch("agents.engine_base.call_llm_with_retry")
    def test_response_format_ban_is_cached_for_the_endpoint(self, mock_retry):
        # After one endpoint discovers response_format is unsupported, a
        # later call to the SAME endpoint proactively omits it instead of
        # re-discovering the 400 every time (mirrors the existing
        # per-endpoint `temperature` ban cache).
        rejection = MagicMock()
        rejection.status_code = 400
        rejection.text = "does not support the response_format parameter"
        http_error = requests.exceptions.HTTPError(response=rejection)
        success_resp = MagicMock()
        success_resp.json.return_value = {}
        mock_retry.side_effect = [http_error, success_resp, success_resp]

        response_format = {"type": "json_schema", "json_schema": {"name": "x", "schema": {}}}
        call_serving_endpoint(
            "https://host", "tok", "cached-ep", [], response_format=response_format
        )
        mock_retry.reset_mock()
        mock_retry.side_effect = [success_resp]

        call_serving_endpoint(
            "https://host", "tok", "cached-ep", [], response_format=response_format
        )

        assert mock_retry.call_count == 1
        payload = mock_retry.call_args[0][2]
        assert "response_format" not in payload


class TestDispatchTool:
    def test_known_tool(self):
        ctx = MagicMock()
        handlers = {"my_tool": lambda c, **kw: json.dumps({"ok": True})}
        result = dispatch_tool(handlers, ctx, "my_tool", {})
        assert json.loads(result) == {"ok": True}

    def test_unknown_tool(self):
        result = dispatch_tool({}, MagicMock(), "missing_tool", {})
        parsed = json.loads(result)
        assert "error" in parsed
        assert "Unknown tool" in parsed["error"]

    def test_exception_in_handler(self):
        def bad_handler(ctx, **kw):
            raise RuntimeError("boom")

        result = dispatch_tool({"bad": bad_handler}, MagicMock(), "bad", {})
        parsed = json.loads(result)
        assert "error" in parsed
        assert "boom" in parsed["error"]

    def test_passes_kwargs(self):
        def echo_handler(ctx, **kwargs):
            return json.dumps(kwargs)

        result = dispatch_tool(
            {"echo": echo_handler}, MagicMock(), "echo", {"a": 1, "b": "two"}
        )
        assert json.loads(result) == {"a": 1, "b": "two"}


class TestExtractMessageContent:
    def test_openai_format(self):
        resp = {"choices": [{"message": {"content": "Hello world"}}]}
        assert extract_message_content(resp) == "Hello world"

    def test_predictions_format_string(self):
        resp = {"predictions": ["predicted text"]}
        assert extract_message_content(resp) == "predicted text"

    def test_predictions_format_non_string(self):
        resp = {"predictions": [42]}
        assert extract_message_content(resp) == "42"

    def test_empty_choices(self):
        assert extract_message_content({"choices": []}) == ""

    def test_no_content_key(self):
        resp = {"choices": [{"message": {}}]}
        assert extract_message_content(resp) == ""

    def test_unknown_format(self):
        assert extract_message_content({"unknown": 1}) == ""

    def test_none_content(self):
        resp = {"choices": [{"message": {"content": None}}]}
        assert extract_message_content(resp) == ""


class TestAccumulateUsage:
    def test_from_empty(self):
        total = {}
        accumulate_usage(total, {"prompt_tokens": 10, "completion_tokens": 5})
        assert total == {"prompt_tokens": 10, "completion_tokens": 5}

    def test_accumulates(self):
        total = {"prompt_tokens": 10, "completion_tokens": 5}
        accumulate_usage(total, {"prompt_tokens": 20, "completion_tokens": 15})
        assert total == {"prompt_tokens": 30, "completion_tokens": 20}

    def test_missing_keys(self):
        total = {"prompt_tokens": 10}
        accumulate_usage(total, {})
        assert total["prompt_tokens"] == 10
        assert total["completion_tokens"] == 0

    def test_empty_usage_block(self):
        total = {}
        accumulate_usage(total, {})
        assert total == {"prompt_tokens": 0, "completion_tokens": 0}
