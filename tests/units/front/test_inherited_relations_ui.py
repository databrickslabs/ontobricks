"""Source contracts for inherited outgoing relations (Studio, Mapping, panels)."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
HELPER = REPO_ROOT / "src/front/static/global/js/ontology-inheritance.js"
ONTOLOGY_HTML = REPO_ROOT / "src/front/templates/ontology.html"
MAPPING_HTML = REPO_ROOT / "src/front/templates/mapping.html"
MAP_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-map.js"
MAP_CSS = REPO_ROOT / "src/front/static/ontology/css/ontology-map.css"
PANEL_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-shared-panels.js"
DESIGN_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-design.js"
DESIGN_CSS = REPO_ROOT / "src/front/static/mapping/css/mapping-design.css"


def test_helper_file_exposes_outgoing_relations_for_class():
    js = HELPER.read_text(encoding="utf-8")
    assert "function outgoingRelationsForClass(classes, properties, className)" in js
    assert "inheritedFrom" in js
    assert "DatatypeProperty" in js


def test_ontology_and_mapping_pages_load_the_helper_before_maps():
    onto = ONTOLOGY_HTML.read_text(encoding="utf-8")
    mapping = MAPPING_HTML.read_text(encoding="utf-8")
    needle = "global/js/ontology-inheritance.js"
    assert needle in onto
    assert needle in mapping
    assert onto.index(needle) < onto.index("ontology/js/ontology-map.js")
    assert mapping.index(needle) < mapping.index("mapping/js/mapping-design.js")


def test_studio_map_pushes_inherited_outgoing_links():
    js = MAP_JS.read_text(encoding="utf-8")
    assert "outgoingRelationsForClass(" in js
    assert "inherited: true" in js
    css = MAP_CSS.read_text(encoding="utf-8")
    assert ".map-link.inherited" in css
    assert "Inherited relation" in js


def test_entity_panel_lists_inherited_outgoing_relations():
    js = PANEL_JS.read_text(encoding="utf-8")
    assert 'id="sharedEntityInheritedRelations"' in js
    assert "outgoingRelationsForClass(" in js
    assert "Inherited relationships" in js
    assert "from ${escapeHtml(r.inheritedFrom" in js


def test_mapping_map_draws_inherited_outgoing_links():
    js = DESIGN_JS.read_text(encoding="utf-8")
    assert "outgoingRelationsForClass(" in js
    css = DESIGN_CSS.read_text(encoding="utf-8")
    assert ".mapping-map-link.inherited" in css
    assert "Inherited relation" in js
    assert "epInheritedRelations" in js
    assert "via " in js
