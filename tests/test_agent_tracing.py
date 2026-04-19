"""Tests for agents.tracing – MLflow tracing helpers."""

import pytest
from unittest.mock import patch, MagicMock

import agents.tracing as tracing_mod
from agents.tracing import (
    _resolve_experiment_name,
    _safe_inputs,
    _safe_result,
    _truncate,
    trace_agent,
    trace_llm,
    trace_tool,
    is_tracing_ready,
    setup_tracing,
)


class TestResolveExperimentName:
    def test_absolute_path_unchanged(self, monkeypatch):
        monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
        assert _resolve_experiment_name("/Users/me/exp") == "/Users/me/exp"

    def test_local_tracking_unchanged(self, monkeypatch):
        monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
        assert _resolve_experiment_name("my-experiment") == "my-experiment"

    def test_databricks_tracking_adds_shared(self, monkeypatch):
        monkeypatch.setenv("MLFLOW_TRACKING_URI", "databricks")
        assert _resolve_experiment_name("my-exp") == "/Shared/my-exp"

    def test_databricks_tracking_case_insensitive(self, monkeypatch):
        monkeypatch.setenv("MLFLOW_TRACKING_URI", "Databricks")
        assert _resolve_experiment_name("my-exp") == "/Shared/my-exp"

    def test_non_databricks_tracking(self, monkeypatch):
        monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
        assert _resolve_experiment_name("my-exp") == "my-exp"


class TestTruncate:
    def test_short_string(self):
        assert _truncate("hello", 10) == "hello"

    def test_exact_limit(self):
        assert _truncate("hello", 5) == "hello"

    def test_over_limit(self):
        result = _truncate("hello world", 5)
        assert result == "hello…"
        assert len(result) == 6  # 5 chars + ellipsis


class TestSafeInputs:
    def test_excludes_secrets(self):
        result = _safe_inputs({"token": "secret", "host": "https://h", "client": "obj", "name": "ok"})
        assert "token" not in result
        assert "host" not in result
        assert "client" not in result
        assert result["name"] == "ok"

    def test_truncates_strings(self):
        result = _safe_inputs({"data": "x" * 500})
        assert result["data"].endswith("…")

    def test_lists_summarised(self):
        result = _safe_inputs({"items": [1, 2, 3]})
        assert "list" in result["items"]
        assert "3" in result["items"]

    def test_dicts_summarised(self):
        result = _safe_inputs({"cfg": {"a": 1, "b": 2}})
        assert "dict" in result["cfg"]

    def test_other_types(self):
        result = _safe_inputs({"obj": object()})
        assert result["obj"] == "object"

    def test_primitives(self):
        result = _safe_inputs({"n": 42, "f": 3.14, "b": True, "none": None})
        assert result["n"] == "42"
        assert result["b"] == "True"


class TestSafeResult:
    def test_none_result(self):
        assert _safe_result(None) == {}

    def test_agent_result_like(self):
        class FakeResult:
            success = True
            iterations = 3
            error = None
            usage = {"prompt_tokens": 10}

        result = _safe_result(FakeResult())
        assert result["success"] is True
        assert result["iterations"] == 3
        assert "error" not in result

    def test_plain_object(self):
        assert _safe_result(object()) == {}


class TestSetupTracing:
    def test_success(self, monkeypatch):
        tracing_mod._TRACING_READY = False
        mock_mlflow = MagicMock()

        with patch.dict("sys.modules", {"mlflow": mock_mlflow}):
            result = setup_tracing("test-exp")

        assert result is True
        assert tracing_mod._TRACING_READY is True

    def test_failure_graceful(self, monkeypatch):
        tracing_mod._TRACING_READY = True

        with patch.dict("sys.modules", {"mlflow": None}):
            with patch("builtins.__import__", side_effect=ImportError("no mlflow")):
                result = setup_tracing()

        assert result is False
        assert tracing_mod._TRACING_READY is False

    def test_is_tracing_ready_reflects_state(self):
        tracing_mod._TRACING_READY = False
        assert is_tracing_ready() is False
        tracing_mod._TRACING_READY = True
        assert is_tracing_ready() is True
        tracing_mod._TRACING_READY = False


class TestTraceDecorators:
    """When tracing is NOT ready, decorators should be no-ops."""

    def setup_method(self):
        tracing_mod._TRACING_READY = False

    def test_trace_agent_passthrough(self):
        @trace_agent("test")
        def my_agent(**kwargs):
            return "result"

        assert my_agent() == "result"

    def test_trace_llm_passthrough(self):
        @trace_llm("test")
        def my_llm(*args, **kwargs):
            return {"choices": [{"message": {"content": "hi"}}]}

        assert my_llm()["choices"][0]["message"]["content"] == "hi"

    def test_trace_tool_passthrough(self):
        @trace_tool("test")
        def my_dispatch(*args, **kwargs):
            return "tool result"

        assert my_dispatch() == "tool result"

    def test_preserves_function_name(self):
        @trace_agent()
        def original_name():
            pass

        assert original_name.__name__ == "original_name"
