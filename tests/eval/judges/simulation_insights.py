"""Rule-based dataset-contract judge for ``agent_simulation_insights``.

Dry-run scores each baseline row without calling the LLM: the input must be
well-formed, the scenario patches must parse as :class:`Patch`, and the
expected section headings and graph list must be valid. Live scoring of the
agent output is not wired yet (see ``.planning/simulation-insights/SPEC.md``).
"""

from __future__ import annotations

from typing import Any

from back.core.errors import ValidationError
from back.objects.intelligence import Patch

REQUIRED_SECTIONS = ("What changed", "Why", "Business impact", "Risks")
VALID_GRAPHS = frozenset({"live", "sandbox"})


def _non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _patches_parse(patches: Any) -> bool:
    if not isinstance(patches, list):
        return False
    try:
        for raw in patches:
            Patch.from_dict(raw)
    except (ValidationError, TypeError, ValueError):
        return False
    return True


def _input_is_valid(inp: dict) -> bool:
    if not _non_empty_str(inp.get("message")):
        return False
    if not isinstance(inp.get("live_answer"), str) or not isinstance(
        inp.get("sandbox_answer"), str
    ):
        return False
    scenario = inp.get("scenario")
    if not isinstance(scenario, dict) or not _non_empty_str(scenario.get("label")):
        return False
    return _patches_parse(scenario.get("patches") or [])


def _expected_is_valid(expected: dict) -> bool:
    sections = expected.get("sections")
    if not isinstance(sections, list) or not sections:
        return False
    for name in REQUIRED_SECTIONS:
        if not any(name.lower() in str(s).lower() for s in sections):
            return False
    for key in ("must_mention", "must_not_claim"):
        values = expected.get(key) or []
        if not isinstance(values, list) or not all(_non_empty_str(v) for v in values):
            return False
    graphs = expected.get("allowed_graphs") or sorted(VALID_GRAPHS)
    return isinstance(graphs, list) and set(graphs) <= VALID_GRAPHS


def score_dataset_row(row: dict) -> float:
    """1.0 when the row's input and expected contract are well-formed, else 0.0."""
    inp = row.get("input") or {}
    expected = row.get("expected") or {}
    if not _input_is_valid(inp) or not _expected_is_valid(expected):
        return 0.0
    return 1.0
