"""Business Rules list UI: Simple-tab toolbar + Data Quality card style."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HTML = (
    ROOT / "src/front/templates/partials/ontology/_ontology_business_rules.html"
).read_text(encoding="utf-8")
IF_THEN_JS = (
    ROOT / "src/front/static/ontology/js/ontology-if-then.js"
).read_text(encoding="utf-8")
BR_JS = (
    ROOT / "src/front/static/ontology/js/ontology-business-rules.js"
).read_text(encoding="utf-8")
ONTOLOGY_CSS = (
    ROOT / "src/front/static/global/css/ontology.css"
).read_text(encoding="utf-8")
DQ_JS = (
    ROOT / "src/front/static/ontology/js/ontology-dataquality.js"
).read_text(encoding="utf-8")


def _simple_pane() -> str:
    start = HTML.index('id="br-pane-if-then"')
    end = HTML.index('id="br-pane-sparql"')
    return HTML[start:end]


def _section_header() -> str:
    start = HTML.index('class="section-header')
    end = HTML.index('id="brTabs"')
    return HTML[start:end]


def test_auto_generate_and_shacl_live_in_simple_tab():
    pane = _simple_pane()
    header = _section_header()
    assert 'id="brAutoGenBtn"' in pane
    assert 'data-br-action="auto-generate"' in pane
    assert "openIfThenModal()" in pane
    assert "Auto-generate" in pane
    assert "SHACL" in pane
    assert 'id="brAutoGenBtn"' not in header
    assert "openIfThenModal()" not in header


def test_section_header_keeps_add_rule():
    header = _section_header()
    assert 'id="brAddBtn"' in header
    assert 'data-br-action="add-rule"' in header


def test_simple_and_complex_rows_use_shared_ob_rule_card():
    assert "ob-rule-card" in IF_THEN_JS
    assert "if-then-rule-card" in IF_THEN_JS
    assert "ob-rule-card" in BR_JS
    assert "br-rule-card" in BR_JS
    assert ".ob-rule-card" in ONTOLOGY_CSS
    assert "ob-rule-card dq-shape-card" in DQ_JS
    # Match Data Quality: badge + fw-medium title, not Bootstrap .card wrappers.
    assert "badge bg-primary bg-opacity-75" in IF_THEN_JS
    assert "small fw-medium" in IF_THEN_JS
    assert 'class="card mb-2 if-then-rule-card"' not in IF_THEN_JS
    assert "small fw-medium" in BR_JS
