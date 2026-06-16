"""`redacted_caplog` fixture — rejects log records leaking secrets.

Required in all `db`-marked tests (Lakebase JWT auth) and any test that goes
through the production logging path. Wraps pytest's `caplog` with a regex check
that fails the test if any log record matches a secret pattern.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Iterator
import pytest


# Patterns intentionally conservative: false positives are far cheaper than
# a real JWT leaking into a log we don't quarantine.
_SECRET_PATTERNS = [
    re.compile(r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"),  # JWT
    re.compile(r"dapi[0-9a-f]{32,}"),  # Databricks PAT
    re.compile(r"sk-[A-Za-z0-9]{32,}"),  # OpenAI-style
    re.compile(r"(?i)password\s*[:=]\s*[^\s]{4,}"),
    re.compile(r"(?i)secret\s*[:=]\s*[^\s]{4,}"),
    re.compile(r"(?i)bearer\s+[A-Za-z0-9_\-\.=]{20,}"),
]


@dataclass
class _RedactingHandler(logging.Handler):
    records: list[logging.LogRecord] = field(default_factory=list)
    violations: list[tuple[str, str]] = field(default_factory=list)

    def __post_init__(self) -> None:
        super().__init__()

    def emit(self, record: logging.LogRecord) -> None:
        msg = record.getMessage()
        for pattern in _SECRET_PATTERNS:
            m = pattern.search(msg)
            if m:
                self.violations.append((pattern.pattern, m.group(0)))
        self.records.append(record)


@pytest.fixture
def redacted_caplog() -> Iterator[_RedactingHandler]:
    """Yield a handler that records logs and tracks secret-pattern violations.

    At test teardown, the fixture asserts zero violations. Tests can opt out of
    the assertion by calling `redacted_caplog.violations.clear()` *before* leaving
    the test body (e.g., when intentionally exercising a redaction failure).
    """
    handler = _RedactingHandler()
    root = logging.getLogger()
    original_level = root.level
    root.setLevel(logging.DEBUG)
    root.addHandler(handler)
    try:
        yield handler
    finally:
        root.removeHandler(handler)
        root.setLevel(original_level)
        if handler.violations:
            samples = "; ".join(
                f"{pat} matched {hit[:40]}…" for pat, hit in handler.violations[:3]
            )
            raise AssertionError(
                f"Secret leaked into logs: {len(handler.violations)} violation(s). {samples}"
            )
