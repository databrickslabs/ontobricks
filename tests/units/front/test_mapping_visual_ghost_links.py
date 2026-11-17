"""Mapping Visual must not flash ghost/unmapped relationship SVGs on load.

Root cause: MappingStudioTabs.show() called _onShown both directly and via
Bootstrap ``shown.bs.tab``, scheduling two concurrent initMappingDesigner()
runs. Each appended an SVG; cleanup only removed the first ``:scope > svg``,
so a stale layer of (often unmapped) relationship paths lingered until the
later schema-drift re-init happened to clear one of them.
"""

from pathlib import Path
import re

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
DESIGN_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-design.js"


def _js() -> str:
    return DESIGN_JS.read_text(encoding="utf-8")


def test_show_does_not_double_invoke_on_shown_when_switching_tabs():
    """Tab.show() already fires shown.bs.tab → _onShown; show() must not also call it."""
    js = _js()
    show = re.search(
        r"show:\s*function\s*\(tab\)\s*\{(.*?)\n\s*\},",
        js,
        re.DOTALL,
    )
    assert show, "MappingStudioTabs.show is missing"
    body = show.group(1)
    # When the target tab is not already active, rely on Bootstrap's shown event.
    assert "classList.contains('active')" in body or 'classList.contains("active")' in body
    # Must not unconditionally call _onShown after Tab.show().
    assert not re.search(
        r"Tab\.getOrCreateInstance\([^)]*\)\.show\(\)[^;]*;\s*"
        r"(?:MappingStudioTabs\._syncUrl\([^)]*\);\s*)?"
        r"MappingStudioTabs\._onShown\(",
        body,
        re.DOTALL,
    )


def test_init_clears_all_canvas_svgs_not_just_the_first():
    js = _js()
    assert "querySelectorAll(':scope > svg')" in js or 'querySelectorAll(":scope > svg")' in js
    assert "querySelector(':scope > svg')" not in js


def test_init_is_superseded_by_a_newer_call():
    """Concurrent inits must not both paint — a sequence token aborts stale runs."""
    js = _js()
    assert "mappingDesignerInitSeq" in js
    assert re.search(
        r"if\s*\(\s*seq\s*!==\s*mappingDesignerInitSeq\s*\)\s*return",
        js,
    )
