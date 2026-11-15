"""Discuss button wiring for Intelligence → Communities, Patterns, Scenarios, Compare.

The button opens the shared domain discussion (window.openOntologyDiscussion,
exported by comments-panel.js on every page). It is bound from JS by id, so
the intelligence partials stay free of inline onclick handlers.
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

FRONT = Path(__file__).resolve().parents[3] / "src/front"
PARTIALS = FRONT / "templates/partials/intelligence"
JS = FRONT / "static/intelligence/js"

CASES = [
    # (partial, discuss button id, bound in js file)
    ("_intel_communities.html", "commDiscussBtn", "intelligence-communities.js"),
    ("_intel_patterns_saved.html", "patSavedDiscussBtn", "intelligence-patterns.js"),
    ("_intel_patterns.html", "patDiscussBtn", "intelligence-patterns.js"),
    ("_sim_scenarios.html", "simDiscussBtn", "simulation.js"),
    ("_sim_compare.html", "simCompareDiscussBtn", "simulation-compare.js"),
]


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


@pytest.mark.parametrize("partial,btn_id,js_name", CASES)
def test_header_has_discuss_button(partial, btn_id, js_name):
    html = _read(PARTIALS / partial)
    assert f'id="{btn_id}"' in html
    tag = html.split(f'id="{btn_id}"', 1)[0].rsplit("<button", 1)[1]
    assert "onto-discuss-btn" in tag
    assert "bi-chat-dots" in html.split(f'id="{btn_id}"', 1)[1][:200]


@pytest.mark.parametrize("partial,btn_id,js_name", CASES)
def test_button_is_bound_from_js_not_inline(partial, btn_id, js_name):
    html = _read(PARTIALS / partial)
    assert "onclick=" not in html
    js = _read(JS / js_name)
    assert btn_id in js
    assert "openOntologyDiscussion()" in js


def test_communities_discuss_sits_before_primary_cta():
    html = _read(PARTIALS / "_intel_communities.html")
    assert html.index('id="commDiscussBtn"') < html.index('id="commDetectBtn"')


def test_scenarios_discuss_sits_before_primary_cta():
    html = _read(PARTIALS / "_sim_scenarios.html")
    assert html.index('id="simDiscussBtn"') < html.index('id="simNewBtn"')
