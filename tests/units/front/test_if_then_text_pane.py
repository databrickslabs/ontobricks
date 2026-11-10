"""Source contracts for the Business Rules SHACL-AF text modal.

The modal is read-only (view / export of the generated SHACL-AF rules); IF-THEN
text import was removed when the raw layer became read-only SHACL.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HTML = (
    ROOT / "src/front/templates/partials/ontology/_ontology_business_rules.html"
).read_text(encoding="utf-8")
JS = (ROOT / "src/front/static/ontology/js/ontology-if-then.js").read_text(encoding="utf-8")


def test_if_then_button_and_modal_markup():
    assert "openIfThenModal" in HTML
    assert 'id="brIfThenModal"' in HTML
    assert 'id="brIfThenEditor"' in HTML
    assert 'id="brIfThenPanel"' not in HTML
    # IF-THEN text import was removed — the modal is read-only.
    assert 'id="brIfThenImportModal"' not in HTML
    assert 'id="brIfThenImportFile"' not in HTML
    assert 'id="brIfThenImportText"' not in HTML


def test_generated_shacl_is_collapsed_behind_a_toggle():
    # The generated SHACL code is hidden by default and revealed by a button.
    assert 'data-if-then-action="toggleShaclPreview"' in HTML
    assert 'id="ifThenToggleShacl"' in HTML
    assert 'id="ifThenPreviewWrap"' in HTML
    # The code pane starts hidden.
    wrap = HTML.split('id="ifThenPreviewWrap"')[1][:40]
    assert "d-none" in wrap
    assert "toggleShaclPreview(" in JS


def test_then_pane_has_set_attribute_section():
    # THEN can set data-property values, mirroring the IF attribute conditions.
    assert 'id="ifThenAssignments"' in HTML
    assert 'data-if-then-action="addAssignmentRow"' in HTML
    assert "Set attributes" in HTML


def test_if_then_module_exposes_set_attribute_helpers():
    for name in (
        "_assignmentAtoms",
        "addAssignmentRow",
        "_removeAssignment",
        "_onAssignmentChange",
        "_renderAssignments",
    ):
        assert f"{name}(" in JS, f"missing IfThenModule.{name}"


def test_if_then_module_exposes_modal_helpers():
    for name in (
        "openIfThenModal",
        "refreshIfThenText",
        "exportIfThen",
    ):
        assert f"{name}(" in JS, f"missing IfThenModule.{name}"
    # Import helpers were removed with the read-only pivot.
    assert "showIfThenImportModal" not in JS
    assert "doIfThenImport" not in JS
    assert "toggleIfThenPanel" not in JS
    # Rules text/export now target the SHACL-AF endpoints.
    assert "/ontology/shacl-rules/text" in JS
    assert "/ontology/shacl-rules/export" in JS
