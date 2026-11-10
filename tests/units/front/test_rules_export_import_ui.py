"""Contract for the rules export / import controls on Ontology → Business rules."""

import re
from pathlib import Path

FRONT = Path(__file__).resolve().parents[3] / "src" / "front"


def _read(rel: str) -> str:
    return (FRONT / rel).read_text(encoding="utf-8")


def test_header_has_export_link_and_import_button_before_add_rule():
    html = _read("templates/partials/ontology/_ontology_business_rules.html")
    assert 'href="/ontology/rules/export"' in html
    assert 'data-br-action="import-rules"' in html
    assert 'id="brImportRulesFile"' in html
    assert html.index("brImportRulesFile") < html.index('id="brAddBtn"')


def test_import_reads_json_confirms_posts_and_shows_summary():
    js = _read("static/ontology/js/ontology-business-rules.js")
    assert "importRulesFile(" in js
    assert "/ontology/rules/import" in js
    assert "showConfirmDialog(" in js
    assert "_showImportSummary(" in js
    assert "JSON.parse" in js


def test_import_button_opens_the_file_picker():
    js = _read("static/ontology/js/ontology-business-rules.js")
    assert "act === 'import-rules'" in js
    assert "brImportRulesFile" in js


def test_export_and_import_ids_do_not_clash_with_graph_or_simulation_pages():
    partial = _read("templates/partials/ontology/_ontology_business_rules.html")
    new_ids = {"brExportRulesBtn", "brImportRulesBtn", "brImportRulesFile", "brImportSummaryModal"}
    for rel in ("templates/dtwin.html", "templates/simulation.html"):
        page_ids = set(re.findall(r'\bid="([^"]+)"', _read(rel)))
        assert not (new_ids & page_ids), rel
    for ident in new_ids - {"brImportSummaryModal"}:
        assert f'id="{ident}"' in partial


def test_import_summary_lists_added_replaced_and_rejected():
    js = _read("static/ontology/js/ontology-business-rules.js")
    assert "Added" in js
    assert "Replaced" in js
    assert "Rejected" in js
    assert "brImportSummaryModal" in js
    assert "result.rejected" in js


def test_import_handles_invalid_json_and_non_object_files():
    js = _read("static/ontology/js/ontology-business-rules.js")
    assert "The file is not valid JSON" in js
    assert "This is not a rules file" in js
    assert "Array.isArray(pkg)" in js
    assert "input.value = ''" in js


def test_import_confirm_names_simple_and_complex_counts():
    js = _read("static/ontology/js/ontology-business-rules.js")
    assert "Import ${simple} Simple and ${complex} Complex rule(s)" in js
    assert "Rules with the same name are replaced" in js


def test_import_refreshes_both_rule_lists_after_success():
    js = _read("static/ontology/js/ontology-business-rules.js")
    assert "this._loadRules('sparql_rules')" in js
    assert "IfThenModule.loadRules()" in js
    assert "this._refreshAllBadges()" in js
