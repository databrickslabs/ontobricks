from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
HTML = ROOT / "src/front/templates/partials/domain/_domain_versions.html"
PAGE = ROOT / "src/front/templates/domain.html"
CSS = ROOT / "src/front/static/domain/css/domain-versions.css"
JS = ROOT / "src/front/static/domain/js/domain-versions.js"


def test_versions_use_semantic_card_list_not_table():
    html = HTML.read_text(encoding="utf-8")
    assert 'id="versionsCardList"' in html
    assert 'role="list"' in html
    assert "<table" not in html
    assert "onclick=" not in html
    assert "style=" not in html


def test_versions_stylesheet_is_wired():
    assert "domain/css/domain-versions.css" in PAGE.read_text(encoding="utf-8")
    assert CSS.exists()


def test_card_css_owns_full_height_scroll_and_mobile_reset():
    css = CSS.read_text(encoding="utf-8")
    assert "#versionsCardList" in css
    assert "overflow-y: auto" in css
    mobile = css[css.index("@media (max-width: 768px)") :]
    assert "#versions-section .dm-versions-workspace" in mobile
    assert "#versionsCardList" in mobile
    assert "overflow: visible" in mobile
    assert "height: auto" in mobile


def test_js_renders_server_capabilities_and_new_endpoints():
    js = JS.read_text(encoding="utf-8")
    assert "version.transitions" in js
    assert "version.can_delete" in js
    assert "version.delete_control_visible" in js
    assert "/domain/set-version-status" in js
    assert "/domain/versions/" in js
    assert "STATUS_MAP" in js
    assert "is_latest" not in js
    assert "status === 'DRAFT'" not in js
