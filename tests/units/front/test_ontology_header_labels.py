"""Contract: Entities / Relationships "+ Add" and Import tabs share one button style and name."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
ENTITIES_HTML = (
    ROOT / "src/front/templates/partials/ontology/_ontology_entities.html"
).read_text(encoding="utf-8")
RELATIONSHIPS_HTML = (
    ROOT / "src/front/templates/partials/ontology/_ontology_relationships.html"
).read_text(encoding="utf-8")
IMPORT_HTML = (
    ROOT / "src/front/templates/partials/ontology/_ontology_import.html"
).read_text(encoding="utf-8")
IMPORT_JS = (
    ROOT / "src/front/static/ontology/js/ontology-import.js"
).read_text(encoding="utf-8")

IMPORT_BUTTON_IDS = [
    "importOwlLocalBtn",
    "importRdfsLocalBtn",
    "importFiboBtn",
    "importCdiscBtn",
    "importIofBtn",
    "importFhirBtn",
]

ADD_BUTTON = re.compile(
    r'<button[^>]*class="btn btn-sm btn-primary ontology-edit-btn"[^>]*>\s*'
    r'<i class="bi bi-plus-circle me-1"></i> Add\s*</button>'
)


def _button(html: str, button_id: str) -> str:
    match = re.search(rf'<button[^>]*id="{button_id}"[^>]*>.*?</button>', html, re.S)
    assert match, f"button #{button_id} not found"
    return match.group(0)


def test_entities_and_relationships_add_buttons_are_primary_and_named_add():
    for html in (ENTITIES_HTML, RELATIONSHIPS_HTML):
        assert ADD_BUTTON.search(html), "Add button must be btn-primary and labelled Add"
        assert "Add Entity" not in html
        assert "Add Relationship" not in html


def test_all_import_tab_buttons_share_style_and_name():
    for button_id in IMPORT_BUTTON_IDS:
        button = _button(IMPORT_HTML, button_id)
        assert 'class="btn btn-sm btn-primary"' in button, button_id
        assert '<i class="bi bi-download"></i> Import\n' in button, button_id
        assert "From Local File" not in button, button_id
        assert "Import Selected" not in button, button_id


def test_import_js_resets_restore_the_same_label():
    assert "Import Selected" not in IMPORT_JS
    assert IMPORT_JS.count("""<i class="bi bi-download"></i> Import';""") == 4
