"""No function or method is silently redefined in the same scope (ruff F811)."""

from __future__ import annotations

import pytest

from tests.units.architecture.source_index import SOURCE_INDEX

pytestmark = pytest.mark.unit


def test_no_silent_redefinition():
    duplicates = [
        f"{m.rel_path}:{lineno} {qualname}"
        for m in SOURCE_INDEX.modules
        for qualname, lineno in m.duplicate_definitions()
    ]
    assert not duplicates, f"Redefined functions (the earlier body is dead code): {duplicates}"
