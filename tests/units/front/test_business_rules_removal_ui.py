"""Complex rule editor: the Removal rule type is read from the query (preview, save gate)."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[3]
TEMPLATE = REPO / "src/front/templates/partials/ontology/_ontology_business_rules.html"
JS = REPO / "src/front/static/ontology/js/ontology-business-rules.js"
CSS = REPO / "src/front/static/ontology/css/ontology-business-rules.css"


@pytest.fixture(scope="module")
def html():
    return TEMPLATE.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def js():
    return JS.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def css():
    return CSS.read_text(encoding="utf-8")


def test_editor_shows_the_rule_type_label_and_removal_controls(html):
    for element_id in ("sparqlKindLabel", "sparqlRemovalPreview", "sparqlRemovalConfirm", "sparqlRemovalBox"):
        assert f'id="{element_id}"' in html


def test_there_is_no_rule_type_dropdown(html, js):
    # The query decides the kind: a dropdown could disagree with it.
    assert 'id="sparqlKind"' not in html
    assert "getElementById('sparqlKind')" not in js


def test_the_rule_type_follows_the_query(js):
    assert "_sparqlKindOf(query) {" in js
    assert "_sparqlKind() {" in js
    assert "return this._sparqlKindOf(document.getElementById('sparqlQuery')?.value);" in js


def test_the_query_input_updates_the_rule_type_and_preview(js):
    assert "addEventListener('input', () => this._sparqlQueryChanged());" in js
    body = js[js.index("_sparqlQueryChanged() {"):]
    assert "sparqlKindLabel" in body[:600]
    assert "sparqlRemovalBox" in body[:600]
    assert "this._sparqlRemovalReset();" in body[:600]


def test_editor_calls_the_removal_preview_endpoint(js):
    assert "/ontology/rules/sparql_rules/removal-preview" in js


def test_preview_button_is_wired_through_the_action_delegate(html, js):
    assert 'data-br-action="sparql-preview"' in html
    assert "act === 'sparql-preview'" in js


def test_no_kind_is_sent_to_the_backend(js):
    # Save and validate send the query alone; the backend reads the kind from it.
    assert "kind: this._sparqlKind()" not in js
    assert "rule: { name, query }" in js


def test_save_is_gated_on_a_preview_of_the_same_query(js):
    assert "_sparqlRemovalReady(query)" in js
    assert "pv.query !== query" in js
    assert "pv.requiresConfirmation" in js


def test_rule_list_shows_a_removal_badge_from_the_query(js, css):
    assert "this._sparqlKindOf(r.query) === 'removal'" in js
    assert "br-kind-badge-removal" in js
    assert ".br-kind-badge-removal" in css


def test_the_assistant_stays_available_for_removal_rules(js, css):
    assert "cra-layout--manual" not in js
    assert "cra-layout--manual" not in css


def test_an_assistant_reply_updates_the_rule_type_through_the_query(js):
    # The assistant sets the query programmatically (no input event), so it
    # calls the same handler a typed edit runs.
    assert "_craApplyDraftKind" not in js
    assert "this._sparqlQueryChanged();" in js
    assert "data.draft.kind" not in js
