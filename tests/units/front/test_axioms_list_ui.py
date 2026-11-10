"""Expressions & Axioms lists share the Business Rules Complex card style."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
AXIOMS_JS = (
    ROOT / "src/front/static/ontology/js/ontology-axioms.js"
).read_text(encoding="utf-8")
BR_JS = (
    ROOT / "src/front/static/ontology/js/ontology-business-rules.js"
).read_text(encoding="utf-8")
ONTOLOGY_CSS = (
    ROOT / "src/front/static/global/css/ontology.css"
).read_text(encoding="utf-8")
ONTOLOGY_HTML = (
    ROOT / "src/front/templates/ontology.html"
).read_text(encoding="utf-8")
MAIN_CSS = (
    ROOT / "src/front/static/global/css/main.css"
).read_text(encoding="utf-8")


def test_expressions_and_axioms_reuse_complex_rule_card():
    assert "BusinessRulesModule._renderRuleCard" in AXIOMS_JS
    assert "br-rule-card" in AXIOMS_JS
    assert "axiom-card" not in AXIOMS_JS
    assert 'class="card mb-2 axiom-card"' not in AXIOMS_JS
    assert "_itemMeta(" in AXIOMS_JS
    assert ".ob-rule-card" in MAIN_CSS


def test_axiom_card_css_overrides_are_gone():
    assert ".axiom-card" not in ONTOLOGY_CSS


def test_rule_card_toggle_is_optional_for_axioms():
    """Axioms have no enable/disable; Complex/Simple still pass onToggle."""
    assert "const toggleHtml = onToggle" in BR_JS
    assert "metaHtml = meta" in BR_JS
    # Axioms call site does not pass onToggle.
    start = AXIOMS_JS.index("BusinessRulesModule._renderRuleCard(container, {")
    end = AXIOMS_JS.index("});", start)
    call = AXIOMS_JS[start:end]
    assert "onEdit:" in call and "onDelete:" in call
    assert "onToggle" not in call


def test_business_rules_module_loads_after_axioms():
    assert ONTOLOGY_HTML.index("ontology/js/ontology-axioms.js") < ONTOLOGY_HTML.index(
        "ontology/js/ontology-business-rules.js"
    )
