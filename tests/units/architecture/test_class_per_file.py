"""One public class per file, named after the class (``.coding_rules §2-3``).

Aggregate record files (a root record plus value objects owned by it) are
the only permanent exception — see ``.planning/class-per-file-refactor`` D1.
"""

from __future__ import annotations

import pytest

from tests.units.architecture.ratchet import ArchitectureRatchet

pytestmark = pytest.mark.unit

RATCHET = ArchitectureRatchet()
BASELINE = RATCHET.load_baseline()


def _allowed_counts() -> dict:
    return {**BASELINE["multi_class_files"], **BASELINE["aggregate_record_files"]}


def test_no_new_multi_class_file():
    allowed = _allowed_counts()
    offenders = {
        path: count
        for path, count in RATCHET.multi_class_counts().items()
        if count > allowed.get(path, 1)
    }
    assert not offenders, (
        "Files with more public classes than allowed (split one class per file): "
        f"{offenders}"
    )


def test_multi_class_baseline_is_tight():
    current = RATCHET.multi_class_counts()
    stale = {
        path: (allowed, current.get(path, 1))
        for path, allowed in _allowed_counts().items()
        if current.get(path, 1) < allowed
    }
    assert not stale, (
        "Baseline entries are looser than the code (allowed, actual) — run "
        f"`python -m tests.units.architecture.ratchet`: {stale}"
    )


def test_single_class_files_are_named_after_their_class():
    known = set(BASELINE["filename_mismatches"])
    new = [p for p in RATCHET.filename_mismatches() if p not in known]
    assert not new, f"Rename these files to match their public class: {new}"


def test_filename_baseline_is_tight():
    stale = set(BASELINE["filename_mismatches"]) - set(RATCHET.filename_mismatches())
    assert not stale, (
        "Fixed files still listed in baseline.json — run "
        f"`python -m tests.units.architecture.ratchet`: {sorted(stale)}"
    )
