"""R2RML import lives in the Graph → Management → Mapping toolbar (Mapping General page removed)."""

import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
MENU = REPO_ROOT / "src/front/config/menu_config.json"
DTWIN_HTML = REPO_ROOT / "src/front/templates/dtwin.html"
DESIGN_HTML = REPO_ROOT / "src/front/templates/partials/mapping/_mapping_design.html"
GENERAL_HTML = REPO_ROOT / "src/front/templates/partials/mapping/_mapping_information.html"
R2RML_TAB_HTML = REPO_ROOT / "src/front/templates/partials/mapping/_mapping_r2rml.html"
IMPORT_PARTIAL = REPO_ROOT / "src/front/templates/partials/mapping/_mapping_import.html"
IMPORT_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-import.js"
HELP = REPO_ROOT / "src/front/templates/partials/layout/help_modal.html"


def test_graph_menu_has_no_import_or_general_mapping_page():
    menus = {m["id"]: m for m in json.loads(MENU.read_text(encoding="utf-8"))["menus"]}
    digitaltwin = menus["digitaltwin"]
    ids = {item["id"] for group in digitaltwin["groups"] for item in group["items"]}
    assert "import" not in ids
    assert "information" not in ids
    assert "design" in ids


def test_graph_page_does_not_include_import_section():
    html = DTWIN_HTML.read_text(encoding="utf-8")
    assert 'id="import-section"' not in html
    assert "_mapping_import.html" not in html
    assert IMPORT_PARTIAL.exists() is False


def test_mapping_general_page_and_r2rml_tab_are_removed():
    html = DTWIN_HTML.read_text(encoding="utf-8")
    assert 'id="information-section"' not in html
    assert "_mapping_information.html" not in html
    assert GENERAL_HTML.exists() is False
    assert R2RML_TAB_HTML.exists() is False


def test_studio_toolbar_hosts_r2rml_import_button():
    html = DESIGN_HTML.read_text(encoding="utf-8")
    assert 'id="mappingImportR2RMLBtn"' in html
    assert 'id="mappingStudioImportR2rmlFileInput"' in html
    assert 'accept=".ttl,.rdf,.xml"' in html
    assert 'id="mappingExportR2RMLBtn"' in html
    assert "replace" in html.lower()


def test_import_js_posts_to_parse_endpoint():
    js = IMPORT_JS.read_text(encoding="utf-8")
    assert "/mapping/parse-r2rml" in js
    assert "mappingImportR2RMLBtn" in js
    assert "mappingStudioImportR2rmlFileInput" in js


def test_help_mentions_r2rml_under_studio():
    help_html = HELP.read_text(encoding="utf-8")
    assert "R2RML" in help_html
    assert "Import page" not in help_html
