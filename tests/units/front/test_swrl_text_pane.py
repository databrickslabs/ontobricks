"""Source contracts for the Business Rules SWRL text modal."""

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
    assert 'id="brSwrlImportModal"' in HTML
    assert 'id="brSwrlImportFile"' in HTML
    assert 'id="brSwrlImportText"' in HTML
    assert 'id="brSwrlPanel"' not in HTML


def test_swrl_module_exposes_modal_helpers():
    for name in (
        "openSwrlModal",
        "refreshSwrlText",
        "exportSwrl",
        "showSwrlImportModal",
        "doSwrlImport",
    ):
        assert f"{name}(" in JS, f"missing SwrlModule.{name}"
    assert "toggleSwrlPanel" not in JS
