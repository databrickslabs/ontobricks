"""Simple (IF-THEN) editor: the Assert / Remove action for the THEN part."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[3]
TEMPLATE = REPO / "src/front/templates/partials/ontology/_ontology_business_rules.html"
JS = REPO / "src/front/static/ontology/js/ontology-if-then.js"


@pytest.fixture(scope="module")
def html():
    return TEMPLATE.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def js():
    return JS.read_text(encoding="utf-8")


def test_editor_has_the_action_select_with_both_options(html):
    assert 'id="ifThenAction"' in html
    assert 'value="assert"' in html
    assert 'value="retract"' in html


def test_editor_has_the_retraction_hint(html):
    assert 'id="ifThenRetractNote"' in html


def test_action_change_is_wired_to_the_module(js):
    assert "getElementById('ifThenAction')" in js
    assert "_onActionChanged()" in js


def test_save_payload_carries_the_action(js):
    assert "action: this._actionValue()," in js


def test_preview_request_carries_the_action(js):
    assert "action: this._actionValue()," in js.split("/ontology/shacl-rules/preview")[1][:400]


def test_editing_restores_the_saved_action(js):
    assert "this._setActionUi(this._actionOf(rule));" in js


def test_list_shows_the_removal_badge_for_retractions(js):
    assert "br-kind-badge-removal small" in js
    assert "this._actionOf(rule) === 'retract' ? retractBadge : badge" in js
