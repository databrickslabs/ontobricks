"""Create Domain is Admin (CAN_MANAGE) only — UI contracts."""

from pathlib import Path


REPO = Path(__file__).resolve().parents[3]
FRONT = REPO / "src" / "front"


def test_home_new_domain_button_requires_admin():
    html = (
        FRONT / "templates" / "partials" / "registry" / "_registry_domains.html"
    ).read_text(encoding="utf-8")
    button_start = html.index('id="btnHomeNewDomain"')
    button = html[button_start : html.index("</button>", button_start)]
    assert 'data-requires-app="admin"' in button


def test_home_js_wires_create_button_to_domain_new():
    js = (FRONT / "static" / "home" / "js" / "home.js").read_text(
        encoding="utf-8"
    )
    assert "btnHomeNewDomain" in js
    assert "window.domainNew()" in js
    assert "fetch('/reset-session'" not in js


def test_admin_dropdown_items_restore_flex_not_block():
    """Settings gear rows are flex icon+label; ``display: block`` un-aligns labels."""
    css = (
        FRONT / "static" / "global" / "css" / "permissions.css"
    ).read_text(encoding="utf-8")
    selector = (
        'body[data-app-role="admin"] .dropdown-item[data-requires-app="admin"]'
    )
    rule_start = css.index(selector)
    rule = css[rule_start : css.index("}", rule_start)]
    assert "display: flex !important;" in rule
    assert "display: block !important;" not in rule
