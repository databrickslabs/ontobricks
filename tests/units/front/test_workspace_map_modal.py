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
    "assignment": "bi-shuffle",
    "digitaltwin": "bi-radar",
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
        "assignment",
        "digitaltwin",
    ]


def test_column_icons_match_menu_config():
    html = _render_modal()
    menus = _menus()
    for column_id, icon in _COLUMN_ICONS.items():
        assert menus[column_id]["icon"] == icon
        column = html.split(f'data-workspace-map-col="{column_id}"', 1)[1]
        assert f"bi {icon}" in column


def test_representative_workspace_routes_are_rendered():
    html = _render_modal()
    assert "/domain/?section=information" in html
    assert "/ontology/?section=map" in html
    assert "/dtwin/?section=sigmagraph" in html


def test_domain_sidebar_only_w3c_items_are_omitted():
    html = _render_modal()
    domain_column = html.split('data-workspace-map-col="domain"', 1)[1].split(
        'data-workspace-map-col="ontology"', 1
    )[0]
    assert "/domain/?section=owl-content" not in domain_column
    assert "/domain/?section=r2rml" not in domain_column


def test_mapping_and_kg_items_carry_graph_gate():
    html = _render_modal()
    mapping_column = html.split('data-workspace-map-col="assignment"', 1)[1].split(
        'data-workspace-map-col="digitaltwin"', 1
    )[0]
    kg_column = html.split('data-workspace-map-col="digitaltwin"', 1)[1]
    assert "nav-requires-graph" in mapping_column
    assert "nav-requires-graph" in kg_column
