"""Entity and relationship side panels show the element URI under the name."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
PANELS_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-shared-panels.js"
MAP_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-map.js"


def test_entity_panel_renders_uri_under_name():
    js = PANELS_JS.read_text(encoding="utf-8")
    mount = js.index('id="sharedEntityEmojiPickerMount"')
    block = js[mount:mount + 200]
    assert "cls?.uri" in block
    assert "URI: ${escapeHtml(cls.uri)}" in block


def test_relationship_panel_renders_uri_under_name():
    js = PANELS_JS.read_text(encoding="utf-8")
    name_input = js.index('id="sharedRelName"')
    block = js[name_input:name_input + 260]
    assert "prop?.uri" in block
    assert "URI: ${escapeHtml(prop.uri)}" in block


def test_canvas_map_does_not_render_uri():
    js = MAP_JS.read_text(encoding="utf-8")
    assert "map-node-uri" not in js
    assert "map-link-uri" not in js
