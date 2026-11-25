"""Complex rule editor: the Removal rule type (kind selector, preview, save gate)."""

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


def test_editor_has_kind_selector_and_removal_controls(html):
    for element_id in ("sparqlKind", "sparqlRemovalPreview", "sparqlRemovalConfirm", "sparqlRemovalBox"):
        assert f'id="{element_id}"' in html


def test_kind_selector_offers_construct_and_removal(html):
    assert 'value="construct"' in html
    assert 'value="removal"' in html


def test_editor_calls_the_removal_preview_endpoint(js):
    assert "/ontology/rules/sparql_rules/removal-preview" in js


def test_preview_button_is_wired_through_the_action_delegate(html, js):
    assert 'data-br-action="sparql-preview"' in html
    assert "act === 'sparql-preview'" in js


def test_save_payload_carries_the_kind(js):
    assert "kind: this._sparqlKind()" in js
    assert "'removal'" in js


def test_validation_request_carries_the_kind(js):
    # Without the kind a removal query is validated as CONSTRUCT and rejected.
    assert "rule: { name, query, kind: this._sparqlKind() }" in js


def test_save_is_gated_on_a_preview_of_the_same_query(js):
    assert "_sparqlRemovalReady(query)" in js
    assert "pv.query !== query" in js
    assert "pv.requiresConfirmation" in js


def test_editing_the_query_clears_the_preview(js):
    assert "this._sparqlRemovalReset()" in js


def test_rule_list_shows_a_removal_badge(js, css):
    assert "br-kind-badge-removal" in js
    assert ".br-kind-badge-removal" in css


def test_removal_rules_hide_the_assistant_column(js, css):
    assert "cra-layout--manual" in js
    assert ".cra-layout--manual .cra-col-chat" in css
