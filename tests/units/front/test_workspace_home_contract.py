"""Workspace Home landings for Context, Ontology, Graph, Intelligence."""

import json
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
FRONT = REPO_ROOT / "src/front"
MENU_CONFIG = FRONT / "config/menu_config.json"
BASE_HTML = FRONT / "templates/base.html"
HOME_PARTIAL = FRONT / "templates/partials/layout/_workspace_home.html"
MAIN_CSS = FRONT / "static/global/css/main.css"
SIDEBAR_JS = FRONT / "static/global/js/sidebar-nav.js"
HELP_JS = FRONT / "static/global/js/help-modal.js"

WORKSPACES = (
    ("domain", "domain.html", "domain"),
    ("ontology", "ontology.html", "ontology"),
    ("digitaltwin", "dtwin.html", "digitaltwin"),
    ("intelligence", "intelligence.html", "intelligence"),
    ("settings", "settings.html", "settings"),
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _menus() -> dict:
    return {m["id"]: m for m in json.loads(_read(MENU_CONFIG))["menus"]}


@pytest.mark.parametrize("menu_id,page,get_id", WORKSPACES)
def test_home_is_default_section_and_page_hosts_it(menu_id, page, get_id):
    menu = _menus()[menu_id]
    assert "home_tiles" not in menu
    items = [i for g in menu["groups"] for i in g["items"]]
    defaults = [i for i in items if i.get("default")]
    assert len(defaults) == 1
    assert defaults[0]["id"] == "home"
    assert items[0]["id"] == "home"
    html = _read(FRONT / "templates" / page)
    assert 'id="home-section" class="sidebar-section active"' in html
    assert html.count('class="sidebar-section active"') == 1
    assert "partials/layout/_workspace_home.html" in html
    assert f"get_menu('{get_id}')" in html

    feature_groups = [
        g for g in menu["groups"] if any(i["id"] != "home" for i in g["items"])
    ]
    assert len(feature_groups) >= 2
    for group in feature_groups:
        for item in group["items"]:
            if item["id"] == "home":
                continue
            assert item.get("description"), f"{menu_id}/{item['id']} missing description"
            assert item.get("help_jump"), f"{menu_id}/{item['id']} missing help_jump"


def test_workspace_home_partial_groups_by_menu_sections():
    html = _read(HOME_PARTIAL)
    assert "section-header" in html
    assert 'class="card"' in html
    assert html.index("section-header") < html.index('class="card"')
    assert "ob-workspace-home-group" in html
    assert "ob-workspace-home-group-title" in html
    assert "workspace_home_menu.groups" in html
    assert "rejectattr('id', 'equalto', 'home')" in html
    assert "ob-workspace-home-tile" in html
    assert "data-help-jump" in html
    assert "ob-workspace-home-resources" in html


def test_app_switcher_always_opens_workspace_home():
    html = _read(BASE_HTML)
    menu = html[html.index("ob-app-switcher-list") : html.index("ob-subnav-close-btn")]
    assert menu.count("?section=home") == 5
    for ns in ("domain_m", "ontology_m", "kg_m", "intel_m", "settings_m"):
        assert f"{ns}.v.route" in menu
    assert 'data-requires-app="admin"' in menu


def test_settings_is_admin_only_in_menu_and_switcher():
    settings = _menus()["settings"]
    assert settings.get("admin_only") is True
    html = _read(BASE_HTML)
    menu = html[html.index("ob-app-switcher-list") : html.index("ob-subnav-close-btn")]
    assert "settings_m.v.route" in menu
    assert menu.count('data-requires-app="admin"') == 1
    css = _read(REPO_ROOT / "src/front/static/global/css/permissions.css")
    assert ".ob-app-switcher-item[data-requires-app=\"admin\"]" in css
    assert ".ob-workspace-home-tile[data-requires-app=\"admin\"]" in css


def test_workspace_home_styles_and_navigation_hooks():
    css = _read(MAIN_CSS)
    assert re.search(r"\.ob-workspace-home-group\s*\+", css) or re.search(
        r"\.ob-workspace-home-group-title\s*\{", css
    )
    assert re.search(r"\.ob-workspace-home-tiles\s*\{", css)
    assert re.search(r"\.ob-workspace-home-tile\s*\{", css)
    resources = re.search(r"\.ob-workspace-home-resources\s*\{([^}]*)\}", css)
    assert resources and "margin-top: auto" in resources.group(1)
    body = re.search(r"\.ob-workspace-home\s*>\s*\.card\s*>\s*\.card-body\s*\{([^}]*)\}", css)
    assert body and "flex-direction: column" in body.group(1)
    js = _read(SIDEBAR_JS)
    assert "ob-workspace-home-tile" in js
    help_js = _read(HELP_JS)
    assert "_jumpToHelp" in help_js
    assert "jump.closest('.help-modal-body')" in help_js
