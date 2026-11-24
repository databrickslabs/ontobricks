"""Entity and relationship side panels show the URI under the pane title."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
PANELS_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-shared-panels.js"
PANELS_CSS = REPO_ROOT / "src/front/static/ontology/css/ontology-shared-panels.css"
MAP_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-map.js"


def test_panel_header_includes_uri_slot():
    js = PANELS_JS.read_text(encoding="utf-8")
    assert 'id="sharedPanelUri"' in js
    assert "class=\"panel-header-uri\"" in js
    assert "function setSharedPanelHeader(panel, titleHtml, uri)" in js


def test_entity_and_relationship_openers_pass_uri_to_header():
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "cls.uri || ''" in js
    assert "prop.uri || ''" in js
    assert js.count("setSharedPanelHeader(") >= 6


def test_form_fields_do_not_render_uri_under_name_inputs():
    js = PANELS_JS.read_text(encoding="utf-8")
    entity_name = js.index('id="sharedEntityName"')
    entity_label = js.index('id="sharedEntityLabel"', entity_name)
    assert "URI:" not in js[entity_name:entity_label]

    rel_name = js.index('id="sharedRelName"')
    rel_label = js.index('id="sharedRelLabel"', rel_name)
    assert "URI:" not in js[rel_name:rel_label]


def test_panel_header_uri_style_is_smaller_and_muted():
    css = PANELS_CSS.read_text(encoding="utf-8")
    assert ".panel-header-uri" in css
    assert "var(--db-text-muted)" in css
    assert "font-size: 0.65rem" in css
    assert "font-weight: 400" in css


def test_canvas_map_does_not_render_uri():
    js = MAP_JS.read_text(encoding="utf-8")
    assert "map-node-uri" not in js
    assert "map-link-uri" not in js
