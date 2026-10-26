"""Size budget: classes <= 800 LOC, functions/methods <= 100 LOC.

Known offenders are frozen in ``baseline.json`` and may only shrink.
"""

from __future__ import annotations

import pytest

from tests.units.architecture.ratchet import (
    MAX_CLASS_LOC,
    MAX_FUNCTION_LOC,
    ArchitectureRatchet,
)

pytestmark = pytest.mark.unit

RATCHET = ArchitectureRatchet()
BASELINE = RATCHET.load_baseline()


def _grown(current: dict, baseline: dict) -> dict:
    return {
        key: (baseline.get(key), size)
        for key, size in current.items()
        if size > baseline.get(key, 0)
    }


def _stale(current: dict, baseline: dict) -> list:
    return sorted(key for key in baseline if key not in current)


def test_no_class_grows_beyond_budget():
    grown = _grown(RATCHET.oversized_classes(), BASELINE["oversized_classes"])
    assert not grown, (
        f"Classes over {MAX_CLASS_LOC} LOC that are new or grew (baseline, actual) — "
        f"extract a class instead: {grown}"
    )


def test_no_function_grows_beyond_budget():
    grown = _grown(RATCHET.oversized_functions(), BASELINE["oversized_functions"])
    assert not grown, (
        f"Functions over {MAX_FUNCTION_LOC} LOC that are new or grew (baseline, actual) — "
        f"extract a method instead: {grown}"
    )


def test_size_baseline_has_no_stale_entries():
    stale = _stale(RATCHET.oversized_classes(), BASELINE["oversized_classes"]) + _stale(
        RATCHET.oversized_functions(), BASELINE["oversized_functions"]
    )
    assert not stale, (
        "Entries now within budget (or moved) — run "
        f"`python -m tests.units.architecture.ratchet`: {stale}"
    )
