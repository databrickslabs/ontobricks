"""Rule-based judges for ``agent_simulation_facts``.

Dry-run scores the dataset contract: every ``expected.patches`` item must
parse as :class:`SimulationPatch`. Live scoring compares an agent result
the same way.
"""

from __future__ import annotations

from typing import Any, Iterable

from back.objects.simulation.SimulationPatch import SimulationPatch
from back.core.errors import ValidationError


def patches_are_valid(patches: Iterable[Any] | None) -> bool:
    """Return True when *patches* is missing, empty, or all valid."""
    if patches is None:
        return True
    if not isinstance(patches, list):
        return False
    for raw in patches:
        try:
            SimulationPatch.from_dict(raw)
        except (ValidationError, TypeError, ValueError):
            return False
    return True


def score_dataset_row(row: dict) -> float:
    """1.0 if the row's expected patches are schema-valid, else 0.0."""
    expected = row.get("expected") or {}
    return 1.0 if patches_are_valid(expected.get("patches")) else 0.0


def score_agent_patches(patches: Iterable[Any] | None) -> float:
    """1.0 if the agent output parses, else 0.0 (None is a miss → 0)."""
    if patches is None:
        return 0.0
    return 1.0 if patches_are_valid(patches) else 0.0
