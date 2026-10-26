"""Create Domain is Admin (CAN_MANAGE) only — UI contracts."""

from pathlib import Path


REPO = Path(__file__).resolve().parents[3]
FRONT = REPO / "src" / "front"


def test_home_new_domain_button_requires_admin():
    html = (FRONT / "templates" / "home.html").read_text(encoding="utf-8")
    assert (
        'data-requires-app="admin" onclick="domainNew()"' in html
    )


def test_registry_modal_new_domain_button_requires_admin():
    html = (
        FRONT / "templates" / "partials" / "registry" / "_registry_domains.html"
    ).read_text(encoding="utf-8")
    button_start = html.index('id="btnRegistryModalNewDomain"')
    button = html[button_start : html.index("</button>", button_start)]
    assert 'data-requires-app="admin"' in button


def test_home_empty_cta_requires_admin_and_calls_domain_new():
    js = (FRONT / "static" / "home" / "js" / "home.js").read_text(
        encoding="utf-8"
    )
    assert (
        'class="ob-domain-empty-cta" data-requires-app="admin" '
        'data-action="newDomain"' in js
    )
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


def test_empty_cta_display_restored_for_admin():
    css = (
        FRONT / "static" / "global" / "css" / "permissions.css"
    ).read_text(encoding="utf-8")
    selector = (
        'body[data-app-role="admin"] '
        '.ob-domain-empty-cta[data-requires-app="admin"]'
    )
    rule_start = css.index(selector)
    rule = css[rule_start : css.index("}", rule_start)]
    assert "display: inline-flex !important;" in rule
