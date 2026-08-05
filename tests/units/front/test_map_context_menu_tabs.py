"""Contract tests for Designer (D3 map) context-menu tab shortcuts.

Right-clicking an entity or relationship on the Ontology "Designer" graph
should offer shortcuts straight to each tab of the shared entity/relationship
panel (Details/Attributes/References/Constraints for entities; Details/
Constraints for relationships, which have no Attributes/References tab).
"""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
MAP_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-map.js"
PANELS_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-shared-panels.js"


def test_entity_context_menu_has_four_tab_shortcuts():
    js = MAP_JS.read_text(encoding="utf-8")
    for tab, label, icon in [
        ("details", "Details", "bi-info-circle"),
        ("attributes", "Attributes", "bi-tags"),
        ("actions", "References", "bi-lightning"),
        ("constraints", "Constraints", "bi-sliders"),
    ]:
        assert f'data-action="open-tab-{tab}"' in js
        assert icon in js
        assert label in js


def test_entity_tab_shortcuts_call_edit_class_by_name_with_tab():
    js = MAP_JS.read_text(encoding="utf-8")
    assert "editClassByName(entityData.name, tab)" in js
    assert "['details', 'attributes', 'actions', 'constraints']" in js


def test_relationship_context_menu_exists_with_two_tab_shortcuts():
    js = MAP_JS.read_text(encoding="utf-8")
    assert "function showMapRelationshipContextMenu(" in js
    assert "['details', 'constraints']" in js
    assert "editPropertyByName(linkData.name, tab)" in js


def test_link_hitarea_has_contextmenu_handler():
    js = MAP_JS.read_text(encoding="utf-8")
    start = js.index("const linkHitareas")
    end = js.index("Draw relationship labels")
    block = js[start:end]
    assert ".on('contextmenu', function(event, d)" in block
    assert "showMapRelationshipContextMenu(event, d, container)" in block


def test_panel_open_functions_accept_active_tab_option():
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "if (options.activeTab) _entityPanelActiveTab = options.activeTab;" in js
    assert "if (options.activeTab) _relPanelActiveTab = options.activeTab;" in js


def test_compatibility_functions_thread_active_tab():
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "function editClassByName(className, activeTab)" in js
    assert "function editClass(idx, activeTab)" in js
    assert "function editPropertyByName(propertyName, activeTab)" in js
    assert "function editProperty(idx, activeTab)" in js
    assert "if (activeTab) opts.activeTab = activeTab;" in js
