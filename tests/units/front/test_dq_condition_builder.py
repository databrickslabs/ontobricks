"""Contract: the Data Quality IF block reuses the shared condition rows.

Conditions guard conformance and consistency rules only, they use the
decision-table operator vocabulary plus exists/notExists, and a row whose
operator needs no value hides its value input. The rows are rendered by the
shared ``ConditionRowsModule`` rather than a third bespoke builder.
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
CONDITIONS_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-conditions.js"
DATAQUALITY_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-dataquality.js"
DATAQUALITY_HTML = (
    REPO_ROOT / "src/front/templates/partials/ontology/_ontology_dataquality.html"
)
ONTOLOGY_HTML = REPO_ROOT / "src/front/templates/ontology.html"


def _method_body(path: Path, name: str) -> str:
    """Return the body of the ``[async ]name(...) { ... }`` object method."""
    source = path.read_text(encoding="utf-8")
    match = re.search(rf"\n    (?:async\s+)?{re.escape(name)}\(", source)
    assert match, f"method {name} not found in {path.name}"
    i = source.index("{", match.start()) + 1
    depth = 1
    body_start = i
    while i < len(source) and depth > 0:
        if source[i] == "{":
            depth += 1
        elif source[i] == "}":
            depth -= 1
        i += 1
    assert depth == 0, f"unbalanced braces in {name}"
    return source[body_start : i - 1]


def test_condition_module_exposes_render_and_collect():
    source = CONDITIONS_JS.read_text(encoding="utf-8")
    assert "window.ConditionRowsModule" in source
    for method in ("render(", "collect(", "addRow(", "summarize("):
        assert f"\n    {method}" in source, f"ConditionRowsModule must expose {method})"


def test_operators_match_the_decision_table_vocabulary():
    source = CONDITIONS_JS.read_text(encoding="utf-8")
    operators = set(re.findall(r"op: '(\w+)'", source))
    assert operators == {
        "eq",
        "neq",
        "gt",
        "gte",
        "lt",
        "lte",
        "startsWith",
        "endsWith",
        "contains",
        "exists",
        "notExists",
    }


def test_existence_operators_hide_the_value_input():
    body = _method_body(CONDITIONS_JS, "_rowHtml")
    assert "isExistence ? 'd-none' : ''" in body, (
        "a row whose operator takes no value must hide its value input"
    )


def test_relationship_only_offers_existence_operators():
    body = _method_body(CONDITIONS_JS, "_operatorOptions")
    assert "isRelationship" in body
    assert "this.EXISTENCE_OPERATORS" in body


def test_collected_rows_keep_dom_alignment():
    body = _method_body(CONDITIONS_JS, "collect")
    assert ".filter(" not in body, (
        "collect must keep incomplete rows so remove indexes stay aligned"
    )


def test_conditions_are_limited_to_conformance_and_consistency():
    source = DATAQUALITY_JS.read_text(encoding="utf-8")
    assert "CONDITION_CATEGORIES: ['conformance', 'consistency']" in source
    body = _method_body(DATAQUALITY_JS, "_renderConditions")
    assert "_conditionsSupported" in body, (
        "the IF block must be hidden for unsupported dimensions"
    )


def test_saved_conditions_drop_incomplete_rows():
    body = _method_body(DATAQUALITY_JS, "_collectConditions")
    assert "c.property_uri && c.op" in body


def test_shape_payload_carries_conditions():
    body = _method_body(DATAQUALITY_JS, "saveShape")
    assert "shape.conditions = this._collectConditions(category)" in body
    assert "shape.condition_logic = this._conditionLogic()" in body


def test_modal_has_the_if_block_and_logic_toggle():
    html = DATAQUALITY_HTML.read_text(encoding="utf-8")
    assert 'id="dqConditionBlock"' in html
    assert 'id="dqConditionRows"' in html
    assert 'id="dqCondLogicAnd"' in html
    assert 'id="dqCondLogicOr"' in html


def test_condition_module_is_loaded_before_the_data_quality_module():
    html = ONTOLOGY_HTML.read_text(encoding="utf-8")
    assert html.index("ontology-conditions.js") < html.index("ontology-dataquality.js")
