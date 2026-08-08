"""Contract tests for the "External configuration" badge on the Ontology
Designer (D3.js force-directed graph, `ontology-map.js`).

The badge is a purely visual indicator shown on entity nodes whose backing
class has a Dashboard, Dataset, Actions, or Bridges configured under the
entity panel's "References" tab. It carries no tooltip and no click handler.
"""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
MAP_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-map.js"
MAP_CSS = REPO_ROOT / "src/front/static/ontology/css/ontology-map.css"


def test_node_data_computes_has_external_flag():
    js = MAP_JS.read_text(encoding="utf-8")
    assert (
        "hasExternal: !!(cls.dashboard || cls.dataset || "
        "(cls.actions || []).length || (cls.bridges || []).length)" in js
    )


def test_badge_rendered_only_for_matching_nodes():
    js = MAP_JS.read_text(encoding="utf-8")
    assert "nodeElements.filter(d => d.hasExternal)" in js
    assert "map-node-external-badge-bg" in js
    assert "map-node-external-badge-icon" in js


def test_badge_has_no_tooltip_or_click_handler():
    js = MAP_JS.read_text(encoding="utf-8")
    # The badge markup block must not attach a <title> (native SVG tooltip)
    # or an .on('click', ...) handler.
    start = js.index("externalBadgeNodes")
    end = js.index("Tooltip on hover")
    block = js[start:end]
    assert "<title>" not in block
    assert ".on(" not in block


def test_badge_marked_decorative_for_accessibility():
    js = MAP_JS.read_text(encoding="utf-8")
    assert "aria-label" in js
    assert "'aria-hidden', 'true'" in js


def test_badge_css_present_and_dims_with_neighborhood_highlight():
    css = MAP_CSS.read_text(encoding="utf-8")
    assert ".map-node-external-badge-bg" in css
    assert ".map-node-external-badge-icon" in css
    assert "pointer-events: none" in css
    assert ".map-node.dimmed .map-node-external-badge-bg" in css
