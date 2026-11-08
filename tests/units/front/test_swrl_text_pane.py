"""Source contracts for the Business Rules SHACL-AF text modal.

The modal is read-only (view / export of the generated SHACL-AF rules); SWRL
text import was removed when the raw layer became read-only SHACL.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HTML = (
    ROOT / "src/front/templates/partials/ontology/_ontology_business_rules.html"
).read_text(encoding="utf-8")
JS = (ROOT / "src/front/static/ontology/js/ontology-swrl.js").read_text(encoding="utf-8")


def test_swrl_button_and_modal_markup():
    assert "openSwrlModal" in HTML
    assert 'id="brSwrlModal"' in HTML
    assert 'id="brSwrlEditor"' in HTML
    assert 'id="brSwrlPanel"' not in HTML
    # SWRL text import was removed — the modal is read-only.
    assert 'id="brSwrlImportModal"' not in HTML
    assert 'id="brSwrlImportFile"' not in HTML
    assert 'id="brSwrlImportText"' not in HTML


def test_swrl_module_exposes_modal_helpers():
    for name in (
        "openSwrlModal",
        "refreshSwrlText",
        "exportSwrl",
    ):
        assert f"{name}(" in JS, f"missing SwrlModule.{name}"
    # Import helpers were removed with the read-only pivot.
    assert "showSwrlImportModal" not in JS
    assert "doSwrlImport" not in JS
    assert "toggleSwrlPanel" not in JS
    # Rules text/export now target the SHACL-AF endpoints.
    assert "/ontology/shacl-rules/text" in JS
    assert "/ontology/shacl-rules/export" in JS
