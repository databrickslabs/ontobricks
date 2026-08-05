"""Contract tests for Mapping "Unmap all" UI labels and wiring."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
INFO_HTML = REPO_ROOT / "src/front/templates/partials/mapping/_mapping_information.html"
DESIGN_HTML = REPO_ROOT / "src/front/templates/partials/mapping/_mapping_design.html"
INFO_JS = REPO_ROOT / "src/front/static/global/js/mapping-information.js"


def test_information_page_has_unmap_all_button():
    html = INFO_HTML.read_text(encoding="utf-8")
    assert 'id="resetMappingsBtn"' in html
    assert "Unmap all" in html


def test_designer_page_has_unmap_all_button():
    html = DESIGN_HTML.read_text(encoding="utf-8")
    assert 'id="resetMappingsDesignBtn"' in html
    assert "Unmap all" in html


def test_confirm_dialog_uses_unmap_all_copy():
    js = INFO_JS.read_text(encoding="utf-8")
    assert "title: 'Unmap All'" in js
    assert "confirmText: 'Unmap all'" in js
    assert "delete <strong>all entity and relationship mappings</strong>" in js


def test_both_buttons_wire_to_confirm_reset():
    js = INFO_JS.read_text(encoding="utf-8")
    assert "'resetMappingsBtn'" in js
    assert "'resetMappingsDesignBtn'" in js
    assert "confirmResetMappings" in js


def test_reset_restores_button_html_in_finally():
    """Failure path must restore the original button label/spinner state."""
    js = INFO_JS.read_text(encoding="utf-8")
    assert "async function resetAllMappings()" in js
    assert "originalHTML" in js
    assert "finally {" in js
    assert "btn.innerHTML = originalHTML" in js
    assert "btn.disabled = false" in js
