"""Shrink-only baselines for the architecture guardrails.

``baseline.json`` lists today's known offenders. Tests fail when a new
offender appears, when a known offender gets worse, and when an entry is
stale (fixed but still listed) so the baseline keeps shrinking.

Tighten the baseline after a refactor with::

    uv run --frozen python -m tests.units.architecture.ratchet

The command only removes entries or lowers numbers (and follows an entry
that moved to another file without growing); it never adds an offender.
New offenders must be fixed, not baselined. Registering a new aggregate
record file (``aggregate_record_files``) is a reviewed manual edit.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from tests.units.architecture.source_index import SOURCE_INDEX, SourceIndex

BASELINE_PATH = Path(__file__).with_name("baseline.json")

MAX_CLASS_LOC = 800
MAX_FUNCTION_LOC = 100

FILENAME_SCOPE = ("back/", "shared/", "agents/")


class ArchitectureRatchet:
    """Measures ``src/`` against the class-per-file and size rules."""

    def __init__(self, index: SourceIndex = SOURCE_INDEX) -> None:
        self._index = index

    def multi_class_counts(self) -> Dict[str, int]:
        return {
            m.rel_path: len(m.public_classes)
            for m in self._index.modules
            if len(m.public_classes) > 1
        }

    def filename_mismatches(self) -> List[str]:
        return sorted(
            m.rel_path
            for m in self._index.under(*FILENAME_SCOPE)
            if m.stem != "__init__"
            and len(m.public_classes) == 1
            and m.public_classes[0].name != m.stem
        )

    def oversized_classes(self) -> Dict[str, int]:
        return {
            f"{m.rel_path}::{c.name}": c.size
            for m in self._index.modules
            for c in m.classes
            if c.size > MAX_CLASS_LOC
        }

    def oversized_functions(self) -> Dict[str, int]:
        return {
            f"{m.rel_path}::{f.qualname}": f.size
            for m in self._index.modules
            for f in m.iter_functions()
            if f.size > MAX_FUNCTION_LOC
        }

    @staticmethod
    def load_baseline() -> dict:
        return json.loads(BASELINE_PATH.read_text(encoding="utf-8"))

    def tightened_baseline(self) -> dict:
        """Return the baseline restricted to current offenders, never loosened."""
        old = self.load_baseline()
        multi = self.multi_class_counts()
        classes = self.oversized_classes()
        functions = self.oversized_functions()
        mismatches = set(self.filename_mismatches())
        return {
            "aggregate_record_files": {
                k: min(v, multi[k])
                for k, v in old["aggregate_record_files"].items()
                if k in multi
            },
            "multi_class_files": {
                k: min(v, multi[k]) for k, v in old["multi_class_files"].items() if k in multi
            },
            "filename_mismatches": sorted(set(old["filename_mismatches"]) & mismatches),
            "oversized_classes": _carry_over(old["oversized_classes"], classes),
            "oversized_functions": _carry_over(old["oversized_functions"], functions),
        }


def _carry_over(old: Dict[str, int], current: Dict[str, int]) -> Dict[str, int]:
    """Keep baseline entries still oversized, following moves to another file.

    A moved entry keeps its budget only when its qualified name is unique on
    both sides and it did not grow, so moving code never hides new offenders.
    """
    kept = {k: min(v, current[k]) for k, v in old.items() if k in current}

    def by_qualname(keys) -> Dict[str, List[str]]:
        groups: Dict[str, List[str]] = {}
        for key in keys:
            groups.setdefault(key.split("::", 1)[1], []).append(key)
        return groups

    gone = by_qualname(k for k in old if k not in current)
    new = by_qualname(k for k in current if k not in old)
    for qualname, old_keys in gone.items():
        new_keys = new.get(qualname, [])
        if len(old_keys) == 1 and len(new_keys) == 1:
            size = current[new_keys[0]]
            if size <= old[old_keys[0]]:
                kept[new_keys[0]] = size
    return kept


def write_baseline(baseline: dict) -> None:
    BASELINE_PATH.write_text(
        json.dumps(baseline, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    write_baseline(ArchitectureRatchet().tightened_baseline())
