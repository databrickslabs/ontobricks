"""Unit tests for e2e/scenario collection gating (Playwright isolation)."""

from __future__ import annotations

from types import SimpleNamespace

from tests.e2e.conftest import _e2e_explicitly_selected


def _config(*, markexpr: str = "", args: tuple[str, ...] = ()):
    return SimpleNamespace(
        option=SimpleNamespace(markexpr=markexpr),
        invocation_params=SimpleNamespace(args=args),
    )


class TestE2eExplicitlySelected:
    def test_bare_pytest_is_not_explicit(self):
        assert _e2e_explicitly_selected(_config()) is False

    def test_not_scenario_marker_is_not_explicit(self):
        assert _e2e_explicitly_selected(_config(markexpr="not scenario")) is False

    def test_scenario_marker_is_explicit(self):
        assert _e2e_explicitly_selected(_config(markexpr="scenario")) is True

    def test_e2e_marker_is_explicit(self):
        assert _e2e_explicitly_selected(_config(markexpr="e2e")) is True

    def test_e2e_path_is_explicit(self):
        assert _e2e_explicitly_selected(
            _config(args=("tests/e2e/scenarios/test_full_lifecycle.py",))
        ) is True
