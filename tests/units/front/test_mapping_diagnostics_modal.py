"""Mapping Diagnostics is a modal popup opened from Graph/Build when mappings are not ready."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
MENU = REPO_ROOT / "src/front/config/menu_config.json"
DTWIN_HTML = REPO_ROOT / "src/front/templates/dtwin.html"
MODAL = REPO_ROOT / "src/front/templates/partials/mapping/_mapping_diagnostics_modal.html"
PAGE = REPO_ROOT / "src/front/templates/partials/mapping/_mapping_diagnostics.html"
DIAG_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-diagnostics.js"
DBX_JS = REPO_ROOT / "src/front/static/query/js/query-databricks-build.js"


def test_mapping_menu_has_no_diagnostics_page():
    menu = MENU.read_text(encoding="utf-8")
    assert '"id": "assignment"' not in menu
    mapping = menu[menu.index('"id": "digitaltwin-management"') : menu.index('"id": "digitaltwin-data"')]
    assert '"id": "diagnostics"' not in mapping
    assert "assignment-advanced" not in menu


def test_graph_page_does_not_include_diagnostics_section():
    html = DTWIN_HTML.read_text(encoding="utf-8")
    assert 'id="diagnostics-section"' not in html
    assert "_mapping_diagnostics.html" not in html
    assert PAGE.exists() is False
    assert MODAL.exists()
    assert "_mapping_diagnostics_modal.html" in html


def test_dtwin_page_includes_diagnostics_modal_and_assets():
    html = DTWIN_HTML.read_text(encoding="utf-8")
    assert "_mapping_diagnostics_modal.html" in html
    assert "mapping-diagnostics.js" in html
    assert "mapping-diagnostics.css" in html


def test_diagnostics_modal_structure():
    html = MODAL.read_text(encoding="utf-8")
    assert 'id="mappingDiagnosticsModal"' in html
    assert 'id="runDiagnosticsBtn"' in html
    assert 'id="diagAccordion"' in html
    assert 'id="diagKpiTiles"' in html
    assert "modal-xl" in html


def test_build_opens_diagnostics_modal_when_not_ready():
    dbx = DBX_JS.read_text(encoding="utf-8")
    assert "MappingDiagnostics.open" in dbx
    assert 'href="/mapping/?section=diagnostics"' not in dbx


def test_diagnostics_js_loads_via_api():
    js = DIAG_JS.read_text(encoding="utf-8")
    assert "/mapping/diagnostics" in js
