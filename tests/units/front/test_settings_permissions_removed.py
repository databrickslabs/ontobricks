"""Contract: Settings Admin no longer exposes a Permissions page."""

from pathlib import Path
import json


REPO_ROOT = Path(__file__).resolve().parents[3]
MENU = REPO_ROOT / "src/front/config/menu_config.json"
SETTINGS_HTML = REPO_ROOT / "src/front/templates/settings.html"
PERMISSIONS_JS = REPO_ROOT / "src/front/static/config/js/permissions.js"


def test_settings_menu_has_no_permissions_item():
    menu = json.loads(MENU.read_text(encoding="utf-8"))
    settings = next(m for m in menu["menus"] if m["id"] == "settings")
    admin = next(g for g in settings["groups"] if g["id"] == "settings-admin")
    ids = [item["id"] for item in admin["items"]]
    assert "permissions" not in ids
    assert "teams" in ids


def test_settings_template_has_no_permissions_section():
    html = SETTINGS_HTML.read_text(encoding="utf-8")
    assert 'id="permissions-section"' not in html
    assert "config/js/permissions.js" not in html
    assert 'id="teams-section"' in html


def test_permissions_page_js_removed():
    assert not PERMISSIONS_JS.exists()
