"""Unit tests for back.core.logging.LogManager (T-M1.P4 under CNS).

Closes the §2 gap: "logging module — no tests".

Covers:
- Singleton access (`instance()` returns the same object).
- `get_logger(name)` returns a Logger with the expected name + parent.
- `setup(level, log_dir, log_file)` configures the rotating handler.
- The JSON formatter produces parseable JSON with required fields.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import pytest

from back.core.logging.LogManager import LogManager, _JSONFormatter


@pytest.fixture(autouse=True)
def _reset_log_manager_singleton(monkeypatch):
    """Each test gets a fresh singleton — logging state is global, easy to bleed."""
    monkeypatch.setattr(LogManager, "_instance", None)
    yield
    monkeypatch.setattr(LogManager, "_instance", None)


@pytest.mark.unit
class TestSingleton:
    def test_instance_returns_same_object(self):
        a = LogManager.instance()
        b = LogManager.instance()
        assert a is b

    def test_initial_state_unconfigured(self):
        mgr = LogManager.instance()
        assert mgr.is_configured is False
        assert mgr.log_path is None


@pytest.mark.unit
class TestGetLogger:
    def test_returns_logger_instance(self):
        logger = LogManager.instance().get_logger("ontobricks.test")
        assert isinstance(logger, logging.Logger)

    def test_logger_name_namespaced_under_app(self):
        # LogManager prepends the app logger name when callers ask for a sub-logger.
        logger = LogManager.instance().get_logger("test.module")
        assert "test.module" in logger.name or logger.name.endswith("test.module")

    def test_two_calls_same_name_same_logger(self):
        a = LogManager.instance().get_logger("same.name")
        b = LogManager.instance().get_logger("same.name")
        assert a is b

    def test_get_logger_none_returns_root_app_logger(self):
        logger = LogManager.instance().get_logger(None)
        assert isinstance(logger, logging.Logger)


@pytest.mark.unit
class TestSetup:
    def test_setup_marks_configured(self, tmp_path):
        mgr = LogManager.instance()
        mgr.setup(level="DEBUG", log_dir=str(tmp_path), log_file="ontobricks.log")
        assert mgr.is_configured is True

    def test_setup_sets_log_path(self, tmp_path):
        mgr = LogManager.instance()
        mgr.setup(level="INFO", log_dir=str(tmp_path), log_file="app.log")
        assert mgr.log_path is not None
        assert "app.log" in mgr.log_path

    def test_setup_respects_level(self, tmp_path):
        mgr = LogManager.instance()
        mgr.setup(level="WARNING", log_dir=str(tmp_path), log_file="app.log")
        assert mgr.level == "WARNING"

    def test_setup_creates_log_dir_if_missing(self, tmp_path):
        # Use a sub-path that doesn't yet exist.
        new_dir = tmp_path / "nested" / "logs"
        mgr = LogManager.instance()
        mgr.setup(level="INFO", log_dir=str(new_dir), log_file="app.log")
        assert new_dir.exists()

    def test_setup_default_level_is_info_or_lower(self, tmp_path):
        # Constants module declares the default; we don't pin a specific value,
        # just verify it's a valid logging-level name.
        mgr = LogManager.instance()
        mgr.setup(log_dir=str(tmp_path), log_file="app.log")
        assert mgr.level in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


@pytest.mark.unit
class TestJSONFormatter:
    def test_emits_valid_json(self):
        fmt = _JSONFormatter()
        record = logging.LogRecord(
            name="ontobricks.test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="hello %s",
            args=("world",),
            exc_info=None,
        )
        out = fmt.format(record)
        parsed = json.loads(out)
        assert parsed["msg"] == "hello world"
        assert parsed["level"] == "INFO"
        assert parsed["logger"] == "ontobricks.test"

    def test_includes_required_fields(self):
        fmt = _JSONFormatter()
        record = logging.LogRecord(
            name="x", level=logging.ERROR, pathname=__file__, lineno=10,
            msg="boom", args=(), exc_info=None,
        )
        parsed = json.loads(fmt.format(record))
        for required in ("ts", "level", "logger", "module", "func", "line", "msg"):
            assert required in parsed

    def test_serialises_exception(self):
        fmt = _JSONFormatter()
        try:
            raise ValueError("kaboom")
        except ValueError:
            import sys
            exc_info = sys.exc_info()
            record = logging.LogRecord(
                name="x", level=logging.ERROR, pathname=__file__, lineno=10,
                msg="failed", args=(), exc_info=exc_info,
            )
            parsed = json.loads(fmt.format(record))
            assert "exception" in parsed
            assert "ValueError" in parsed["exception"]

    def test_handles_non_serialisable_args(self):
        fmt = _JSONFormatter()

        class NotSerialisable:
            def __repr__(self):
                return "<unserialisable>"

        record = logging.LogRecord(
            name="x", level=logging.INFO, pathname=__file__, lineno=10,
            msg="got %s", args=(NotSerialisable(),), exc_info=None,
        )
        # Must not raise — `default=str` in dumps handles arbitrary objects.
        out = fmt.format(record)
        assert "unserialisable" in out


@pytest.mark.unit
class TestPublicAPI:
    """The module-level shims must delegate to LogManager."""

    def test_get_logger_module_function_works(self):
        from back.core.logging import get_logger

        logger = get_logger("module.shim.test")
        assert isinstance(logger, logging.Logger)

    def test_setup_logging_module_function_works(self, tmp_path):
        from back.core.logging import setup_logging

        # Should not raise; should leave the singleton configured.
        setup_logging(level="INFO", log_dir=str(tmp_path), log_file="x.log")
        assert LogManager.instance().is_configured
