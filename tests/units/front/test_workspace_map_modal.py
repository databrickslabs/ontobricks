"""Workspace map modal contracts for the L1 Domain badge overlay."""

import json
import re
from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
TEMPLATES = REPO_ROOT / "src/front/templates"
BASE_HTML = TEMPLATES / "base.html"
PARTIAL = TEMPLATES / "partials/layout/_workspace_map_modal.html"
MENU_CONFIG = REPO_ROOT / "src/front/config/menu_config.json"
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"
COMPONENTS_CSS = REPO_ROOT / "src/front/static/global/css/components.css"

_COLUMN_ICONS = {
    "domain": "bi-box",
    "ontology": "bi-bezier2",
    "digitaltwin": "bi-radar",
    "intelligence": "bi-sliders2-vertical",
}


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _menus() -> dict[str, dict]:
    config = json.loads(_read(MENU_CONFIG))
    return {menu["id"]: menu for menu in config["menus"]}


def _render_modal() -> str:
    environment = Environment(loader=FileSystemLoader(TEMPLATES), autoescape=True)
    template = environment.get_template("partials/layout/_workspace_map_modal.html")
    return template.render(menu_config=json.loads(_read(MENU_CONFIG)))


def test_base_includes_workspace_map_modal():
    assert 'partials/layout/_workspace_map_modal.html' in _read(BASE_HTML)


def test_modal_root_and_four_columns():
    html = _render_modal()
    assert 'id="workspaceMapModal"' in html
    assert "modal-dialog-centered modal-xl" in html
    assert re.findall(r'data-workspace-map-col="([^"]+)"', html) == [
        "domain",
        "ontology",
        "digitaltwin",
        "intelligence",
    ]
    assert "assignment" not in re.findall(r'data-workspace-map-col="([^"]+)"', html)


def test_column_icons_match_menu_config():
    html = _render_modal()
    menus = _menus()
    for column_id, icon in _COLUMN_ICONS.items():
        assert menus[column_id]["icon"] == icon
        column = html.split(f'data-workspace-map-col="{column_id}"', 1)[1]
        assert f"bi {icon}" in column


def test_simulation_workbench_is_not_on_the_map():
    html = _render_modal()
    sim_column = html.split('data-workspace-map-col="intelligence"', 1)[1]
    assert "/intelligence/?section=workbench" not in sim_column
    assert "/intelligence/?section=scenarios" in sim_column


def test_representative_workspace_routes_are_rendered():
    html = _render_modal()
    assert "/domain/?section=information" in html
    assert "/ontology/?section=map" in html
    assert "/dtwin/?section=sigmagraph" in html
    assert "/dtwin/?section=design" in html
    assert "/dtwin/?section=manual" not in html


def test_domain_sidebar_only_w3c_items_are_omitted():
    html = _render_modal()
    domain_column = html.split('data-workspace-map-col="domain"', 1)[1].split(
        'data-workspace-map-col="ontology"', 1
    )[0]
    assert "/domain/?section=owl-content" not in domain_column
    assert "/domain/?section=r2rml" not in domain_column


def test_graph_and_intelligence_items_carry_graph_gate():
    html = _render_modal()
    kg_column = html.split('data-workspace-map-col="digitaltwin"', 1)[1].split(
        'data-workspace-map-col="intelligence"', 1
    )[0]
    sim_column = html.split('data-workspace-map-col="intelligence"', 1)[1]
    assert "nav-requires-graph" in kg_column
    assert "nav-requires-graph" in sim_column
    assert "/dtwin/?section=design" in kg_column


def test_workspace_map_css_uses_shell_tokens():
    css = _read(COMPONENTS_CSS)
    assert ".ob-workspace-map-grid" in css
    assert ".ob-workspace-map-item.is-current" in css
    assert "repeat(4, minmax(0, 1fr))" in css
    block = css[css.index("Workspace map modal") : css.index("/* Local color")]
    assert "--db-hover-indigo" in block
    assert "--db-primary-light" in block
    assert not re.search(r"#[0-9A-Fa-f]{3,8}", block)


def test_navbar_opens_workspace_map_from_domain_badge():
    js = _read(NAVBAR_JS)
    assert "function openWorkspaceMap(" in js
    assert "function bindWorkspaceMapTrigger(" in js
    assert "bindWorkspaceMapTrigger();" in js
    open_block = js[js.index("function openWorkspaceMap(") :][:800]
    bind_block = js[js.index("function bindWorkspaceMapTrigger(") :][:900]
    assert "event.preventDefault()" in open_block
    assert "Modal.getOrCreateInstance" in open_block
    assert "domainContextTrigger" in bind_block
    assert "settingsDomainReturnLink" in bind_block


def test_settings_return_link_reuses_workspace_map_modal_handler():
    js = _read(NAVBAR_JS)
    assert "const triggerIds = ['domainContextTrigger', 'settingsDomainReturnLink'];" in js
    assert "link.addEventListener('click', openWorkspaceMap);" in js
    assert "opens context navigation via the workspace map" in js


def test_navbar_highlights_current_workspace_map_item():
    js = _read(NAVBAR_JS)
    assert "function highlightWorkspaceMapCurrent(" in js
    assert "data-workspace-map-item" in js
    assert "data-workspace-map-default" in js
    assert "is-current" in js
    assert "aria-current" in js


def test_navbar_syncs_workspace_map_title_and_status():
    js = _read(NAVBAR_JS)
    assert "function syncWorkspaceMapTitle(" in js
    assert "workspaceMapDomainLabel" in js
    apply_block = js[js.index("function applyDomainInfo(") :][:1800]
    assert "applyDomainStatusBadge(mapLabel" in apply_block


def test_workspace_map_embeds_switch_version_controls():
    """The context popup owns the former Switch Version modal controls."""
    html = _render_modal()
    assert 'id="switchVersionSelect"' in html
    assert 'id="switchSaveFirst"' in html
    assert 'id="btnConfirmSwitch"' in html
    assert 'id="btnCreateVersionFromSwitch"' in html
    assert "ob-workspace-map-versions" in html
    assert html.index("ob-workspace-map-versions") < html.index("ob-workspace-map-grid")


def test_open_workspace_map_initialises_version_controls():
    js = _read(NAVBAR_JS)
    open_block = js[js.index("function openWorkspaceMap(") :][:1200]
    assert "initWorkspaceMapVersions(" in open_block or "populateSwitchVersions(" in open_block
    assert "configureSwitchSaveOption(" in open_block or "initWorkspaceMapVersions(" in open_block
    assert "function bindWorkspaceMapVersionActions(" in js
    assert "domainSwitchModal" not in js


def test_domain_switch_opens_the_combined_workspace_map():
    js = _read(NAVBAR_JS)
    switch_fn = js[js.index("async function domainSwitch(") :][:900]
    assert "openWorkspaceMap(" in switch_fn
    assert "showSwitchDomainDialog(" not in switch_fn
