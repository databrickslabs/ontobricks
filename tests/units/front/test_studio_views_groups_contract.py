"""Contracts for Studio Business View dropdown + ontology group collapse."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
MAP_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-map.js"
MAP_CSS = REPO_ROOT / "src/front/static/ontology/css/ontology-map.css"
MAP_HTML = REPO_ROOT / "src/front/templates/partials/ontology/_ontology_map.html"
ONTOLOGY_HTML = REPO_ROOT / "src/front/templates/ontology.html"
GROUPS_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-groups.js"
GROUPS_HTML = REPO_ROOT / "src/front/templates/partials/ontology/_ontology_groups.html"
PANELS_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-shared-panels.js"
MENU = REPO_ROOT / "src/front/config/menu_config.json"
PERSISTENCE_JS = REPO_ROOT / "src/front/static/global/js/ontology-persistence.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_ontoviz_and_design_section_are_gone():
    html = _read(ONTOLOGY_HTML)
    assert "ontoviz.js" not in html
    assert "ontology-design.js" not in html
    assert 'id="design-section"' not in html
    assert "ontology-persistence.js" in html
    assert not (REPO_ROOT / "src/front/static/global/ontoviz/ontoviz.js").exists()
    assert not (REPO_ROOT / "src/front/static/global/js/ontology-design.js").exists()
    assert not (
        REPO_ROOT / "src/front/templates/partials/ontology/_ontology_design.html"
    ).exists()


def test_menu_has_no_business_views_or_groups_sidebar_entry():
    import json

    menus = {m["id"]: m for m in json.loads(_read(MENU))["menus"]}
    ontology_ids = [
        item["id"]
        for group in menus["ontology"]["groups"]
        for item in group["items"]
    ]
    assert "design" not in ontology_ids
    assert "groups" not in ontology_ids
    assert "map" in ontology_ids
    labels = {
        item.get("label")
        for group in menus["ontology"]["groups"]
        for item in group["items"]
    }
    assert "Business Views" not in labels
    assert "Groups" not in labels


def test_studio_toolbar_has_view_dropdown_and_groups_menu():
    html = _read(MAP_HTML)
    assert 'id="studioViewBtn"' in html
    assert 'id="studioViewMenu"' in html
    assert 'data-view="__all__"' in html
    assert 'id="studioDeleteViewBtn"' in html
    assert 'id="studioGroupsBtn"' in html
    assert 'id="studioNewGroupBtn"' in html


def test_groups_modal_has_delete_and_no_sidebar_section():
    groups_html = _read(
        REPO_ROOT / "src/front/templates/partials/ontology/_ontology_groups.html"
    )
    ontology_html = _read(ONTOLOGY_HTML)
    groups_css = _read(
        REPO_ROOT / "src/front/static/ontology/css/ontology-groups.css"
    )
    groups_js = _read(GROUPS_JS)
    assert 'id="groupModal"' in groups_html
    assert 'id="groupModalDeleteBtn"' in groups_html
    assert 'id="groups-section"' not in ontology_html
    assert 'id="groupsList"' not in groups_html
    assert 'studio-group-menu-item' in groups_js
    assert 'flex-nowrap' in groups_js
    assert 'white-space: nowrap' in groups_css
    assert 'sharedEntityGroup' in _read(
        REPO_ROOT / "src/front/static/ontology/js/ontology-shared-panels.js"
    )


def test_map_exposes_view_and_group_apis():
    js = _read(MAP_JS)
    for name in (
        "refreshStudioViewSelect",
        "applyStudioView",
        "wireStudioViewControls",
        "flushStudioLayoutBeforeSave",
        "loadStudioGroups",
        "collapseStudioGroup",
        "expandStudioGroup",
        "refreshStudioGroups",
        "_snapshotStudioEntityPositions",
        "_applyStudioEntityPosition",
        "_studioEntityPositions",
    ):
        assert name in js
    assert "SidebarNav.switchTo('design')" not in js
    assert "loadOntologyIntoDesigner" not in js
    assert "collapsedGroups" in js
    # Collapse/expand must await persist + rebuild so layout cannot race.
    assert "await persistStudioCollapsedGroups()" in js
    assert "await initOntologyMap()" in js


def test_group_toggle_css_exists():
    css = _read(MAP_CSS)
    assert ".studio-group-toggle" in css
    assert ".map-group-circle" in css
    assert ".studio-view-toggle" in css
    assert ".studio-view-rename" in css
    # + badge: white glyph centered on a group-colored circle (fill set in JS).
    assert "dominant-baseline: central" in css
    assert "text-anchor: middle" in css
    js = _read(MAP_JS)
    assert ".attr('fill', groupColor)" in js
    assert "dominant-baseline', 'central'" in js
    # Member badge shows a minus (collapse); the group circle keeps the plus (expand).
    assert "studio-group-toggle-collapse" in js
    assert ".text('\\u2212')" in js


def test_business_view_rename_is_wired_from_dropdown():
    """Rename uses the rich View dropdown's pencil and the existing rename endpoint."""
    js = _read(MAP_JS)
    assert "function renameStudioView(" in js
    assert "/domain/design-views/rename" in js
    assert "studio-view-rename" in js
    assert "studioViewMenu" in js
    # Client-side duplicate check gives an inline message before the POST.
    assert "already exists" in js

    perms = _read(REPO_ROOT / "src/front/static/global/css/permissions.css")
    assert ".studio-view-rename" in perms


def test_groups_crud_refreshes_studio():
    js = _read(GROUPS_JS)
    assert "refreshStudioGroups" in js


def test_exclusive_group_membership_is_surfaced_in_ui():
    """An entity can belong to only one group — modal, panel, and Studio menus say so."""
    groups_js = _read(GROUPS_JS)
    assert "_wireExclusiveMemberChecks" in groups_js
    assert "data-other-group" in groups_js
    assert "An entity can belong to only one group" in groups_js

    panels = _read(PANELS_JS)
    assert "sharedEntityGroupExclusiveHint" in panels
    assert "An entity can belong to only one group." in panels

    groups_html = _read(GROUPS_HTML)
    assert "An entity can belong to only one group" in groups_html
    assert "groupMembersExclusiveHint" in groups_html

    css = _read(MAP_CSS)
    assert ".map-context-submenu" in css
    assert ".map-context-has-submenu" in css

    map_js = _read(MAP_JS)
    assert 'data-action="remove-from-group"' in map_js
    assert "Remove from group" in map_js


def test_persistence_module_marks_registry_dirty():
    js = _read(PERSISTENCE_JS)
    assert "setDomainDirty('ontology', true)" in js
    assert "setDomainDirty('ontology', false)" in js
    assert "function saveRegistryOnUnload" in js
