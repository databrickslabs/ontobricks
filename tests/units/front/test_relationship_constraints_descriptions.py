"""Contract tests for the relationship panel's "Constraints" tab descriptions.

Users asked for each constraint option (Cardinality Min/Max, and the four
Property Characteristics checkboxes) to be explained with a visible caption
below it -- not a hover tooltip -- in the Ontology Designer's relationship
edit panel.
"""

from pathlib import Path
import re


REPO_ROOT = Path(__file__).resolve().parents[3]
PANELS_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-shared-panels.js"


def _relationship_constraints_block() -> str:
    js = PANELS_JS.read_text(encoding="utf-8")
    start = js.index("data-form-tab-content=\"constraints\"", js.index("sharedRelDomain"))
    end = js.index("</form>", start)
    return js[start:end]


def test_cardinality_fields_have_visible_captions():
    block = _relationship_constraints_block()
    assert "Minimum number of values required per subject" in block
    assert "Maximum number of values allowed; leave empty for unlimited" in block


def test_property_characteristics_each_have_visible_captions():
    block = _relationship_constraints_block()
    expected = {
        "sharedRelFunctional": "Each subject can have at most one value for this relationship",
        "sharedRelInverseFunctional": "Each target value can be linked back to at most one subject",
        "sharedRelSymmetric": "If A is related to B, then B is automatically related to A too",
        "sharedRelTransitive": "If A is related to B, and B is related to C, then A is automatically related to C",
    }
    for checkbox_id, caption in expected.items():
        assert checkbox_id in block
        assert caption in block


def test_no_title_tooltips_on_constraint_options():
    block = _relationship_constraints_block()
    # None of the constraint inputs/checkboxes should rely on a hover
    # tooltip anymore -- descriptions must be plain visible text instead.
    for checkbox_id in (
        "sharedRelFunctional",
        "sharedRelInverseFunctional",
        "sharedRelSymmetric",
        "sharedRelTransitive",
    ):
        # Grab the <input ...> tag for this checkbox and confirm it (and its
        # <label>) carry no `title=` attribute.
        input_match = re.search(rf'<input[^>]*id="{checkbox_id}"[^>]*>', block)
        assert input_match, f"could not find checkbox input for {checkbox_id}"
        assert "title=" not in input_match.group(0)

        label_match = re.search(rf'<label[^>]*for="{checkbox_id}"[^>]*>', block)
        assert label_match, f"could not find label for {checkbox_id}"
        assert "title=" not in label_match.group(0)

    assert 'title="Minimum cardinality"' not in block
    assert 'title="Maximum cardinality"' not in block


def test_each_characteristic_uses_form_text_caption_style():
    block = _relationship_constraints_block()
    # Consistent with the rest of the panel's muted caption styling.
    assert block.count('class="form-text small mt-0"') == 4
