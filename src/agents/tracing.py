"""
MLflow tracing configuration for OntoBricks agents.

Provides helpers to initialise MLflow experiment tracking and tracing,
plus a safe decorator that degrades to a no-op when MLflow is unavailable
or not configured.
"""

import os
import functools
from typing import Optional

from back.core.logging import get_logger

logger = get_logger(__name__)

_TRACING_READY = False

EXPERIMENT_NAME = os.getenv("ONTOBRICKS_MLFLOW_EXPERIMENT", "ontobricks-agents")


def _resolve_experiment_name(name: str) -> str:
    """Ensure the experiment name is an absolute workspace path on Databricks.

    Databricks MLflow requires paths like ``/Users/<user>/...`` or ``/Shared/...``.
    When running locally (no tracking URI or file-based), the name is returned as-is.
    """
    if name.startswith("/"):
        return name

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "")
    if tracking_uri.lower() != "databricks":
        return name

    return f"/Shared/{name}"


def setup_tracing(experiment_name: Optional[str] = None) -> bool:
    """Initialise MLflow tracing for agent runs.

    Call once during application startup.  Returns *True* if tracing was
    successfully configured, *False* otherwise (the application can still
    run without tracing).
    """
    global _TRACING_READY
    try:
        import mlflow

        name = _resolve_experiment_name(experiment_name or EXPERIMENT_NAME)
        mlflow.set_experiment(name)
        mlflow.tracing.enable()
        _TRACING_READY = True
        logger.info("MLflow tracing enabled — experiment='%s'", name)
        return True
    except Exception as exc:
        logger.warning(
            "MLflow tracing setup failed (agents will run without tracing): %s", exc
        )
        _TRACING_READY = False
        return False


def is_tracing_ready() -> bool:
    return _TRACING_READY


def trace_agent(name: Optional[str] = None):
    """Decorator: wrap an agent ``run_agent`` function with an AGENT span."""

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            if not _TRACING_READY:
                return fn(*args, **kwargs)
            import mlflow
            from mlflow.entities import SpanType

            with mlflow.start_span(
                name=name or fn.__name__, span_type=SpanType.AGENT
            ) as span:
                span.set_inputs(_safe_inputs(kwargs))
                result = fn(*args, **kwargs)
                span.set_outputs(_safe_result(result))
                return result

        return wrapper

    return decorator


def trace_llm(name: Optional[str] = None):
    """Decorator: wrap an LLM call function with an LLM span."""

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            if not _TRACING_READY:
                return fn(*args, **kwargs)
            import mlflow
            from mlflow.entities import SpanType

            with mlflow.start_span(
                name=name or fn.__name__, span_type=SpanType.LLM
            ) as span:
                span.set_inputs(
                    {
                        "endpoint": kwargs.get("endpoint_name")
                        or (args[2] if len(args) > 2 else "?"),
                        "message_count": len(
                            kwargs.get("messages") or (args[3] if len(args) > 3 else [])
                        ),
                    }
                )
                result = fn(*args, **kwargs)
                if isinstance(result, dict):
                    usage = result.get("usage", {})
                    span.set_outputs(
                        {
                            "finish_reason": result.get("choices", [{}])[0].get(
                                "finish_reason"
                            ),
                            "prompt_tokens": usage.get("prompt_tokens"),
                            "completion_tokens": usage.get("completion_tokens"),
                        }
                    )
                return result

        return wrapper

    return decorator


def trace_tool(name: Optional[str] = None):
    """Decorator: wrap a tool-dispatch function with a TOOL span."""

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            if not _TRACING_READY:
                return fn(*args, **kwargs)
            import mlflow
            from mlflow.entities import SpanType

            tool_name = kwargs.get("tool_name") or (args[1] if len(args) > 1 else "?")
            tool_args = kwargs.get("arguments") or (args[2] if len(args) > 2 else {})
            with mlflow.start_span(
                name=f"tool:{tool_name}", span_type=SpanType.TOOL
            ) as span:
                span.set_inputs(
                    {
                        "tool_name": tool_name,
                        "arguments": _truncate(str(tool_args), 500),
                    }
                )
                result = fn(*args, **kwargs)
                span.set_outputs(
                    {"result_length": len(result) if isinstance(result, str) else None}
                )
                return result

        return wrapper

    return decorator


def _safe_inputs(kwargs: dict) -> dict:
    """Extract trace-safe inputs from kwargs, excluding secrets."""
    exclude = {"token", "host", "client"}
    out = {}
    for k, v in kwargs.items():
        if k in exclude:
            continue
        if isinstance(v, (str, int, float, bool, type(None))):
            out[k] = _truncate(str(v), 300)
        elif isinstance(v, list):
            out[k] = f"[list, len={len(v)}]"
        elif isinstance(v, dict):
            out[k] = f"[dict, keys={list(v.keys())[:10]}]"
        else:
            out[k] = type(v).__name__
    return out


def _safe_result(result) -> dict:
    """Serialise an AgentResult-like object for the span output."""
    if result is None:
        return {}
    out = {}
    for attr in ("success", "iterations", "error", "usage"):
        val = getattr(result, attr, None)
        if val is not None:
            out[attr] = val
    return out


def _truncate(s: str, limit: int) -> str:
    return s if len(s) <= limit else s[:limit] + "…"
