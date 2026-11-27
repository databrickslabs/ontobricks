"""HTTP contract of the Simple-rule retraction (``action: "retract"``)."""

import json

import pytest
from fastapi.testclient import TestClient

from shared.fastapi.main import app
from tests.units.api.test_ui_rendering import ensure_loaded_domain

pytestmark = pytest.mark.unit

#: Its THEN part restates the IF part on purpose: the tautology gate must not fire.
RETRACT = {
    "name": "RouteDropMeterLink",
    "description": "d",
    "antecedent": "Meter(?m) \u2227 Customer(?c) \u2227 meterOf(?m, ?c)",
    "consequent": "meterOf(?m, ?c)",
    "action": "retract",
}
ASSERT = {
    "name": "RouteAssertRule",
    "antecedent": "Customer(?c)",
    "consequent": "Payment(?c)",
}


@pytest.fixture
def client():
    client = TestClient(app)
    ensure_loaded_domain(client)
    return client


def _save(client, rule, index=-1):
    return client.post("/ontology/if-then/save", json={"rule": rule, "index": index})


def _rules(client):
    return client.get("/ontology/if-then/list").json()["rules"]


class TestSaveAndList:
    def test_retraction_is_saved_and_keeps_its_action(self, client):
        response = _save(client, RETRACT)
        assert response.status_code == 200, response.text
        saved = next(r for r in _rules(client) if r["name"] == RETRACT["name"])
        assert saved["action"] == "retract"
        assert saved["consequent"] == RETRACT["consequent"]

    def test_rule_without_action_is_still_accepted(self, client):
        assert _save(client, ASSERT).status_code == 200
        saved = next(r for r in _rules(client) if r["name"] == ASSERT["name"])
        assert saved.get("action") in (None, "assert")

    def test_class_membership_in_a_retraction_is_saved(self, client):
        rule = {**RETRACT, "name": "RouteDropMembership", "consequent": "Customer(?c)"}
        response = _save(client, rule)
        assert response.status_code == 200, response.text
        saved = next(r for r in _rules(client) if r["name"] == "RouteDropMembership")
        assert saved["action"] == "retract"
        assert saved["consequent"] == "Customer(?c)"

    def test_unbound_variable_in_a_retraction_is_rejected(self, client):
        rule = {**RETRACT, "name": "RouteBadVar", "consequent": "meterOf(?m, ?z)"}
        response = _save(client, rule)
        assert response.status_code in (400, 422)
        assert "'?z' must be bound" in response.json().get("message", "")

    def test_unknown_action_is_rejected(self, client):
        rule = {**RETRACT, "name": "RouteBadAction", "action": "delete"}
        response = _save(client, rule)
        assert response.status_code in (400, 422)
        assert "action must be" in response.json().get("message", "")

    def test_toggling_a_retraction_keeps_its_action(self, client):
        _save(client, {**RETRACT, "name": "RouteToggle"})
        rules = _rules(client)
        index = next(i for i, r in enumerate(rules) if r["name"] == "RouteToggle")
        toggled = {**rules[index], "enabled": False}
        assert _save(client, toggled, index).status_code == 200
        saved = next(r for r in _rules(client) if r["name"] == "RouteToggle")
        assert saved["action"] == "retract"
        assert saved["enabled"] is False


class TestPreview:
    def test_retraction_previews_as_a_delete(self, client):
        response = client.post(
            "/ontology/shacl-rules/preview",
            json={
                "name": "r",
                "antecedent": RETRACT["antecedent"],
                "consequent": RETRACT["consequent"],
                "action": "retract",
            },
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["rule_type"] == "retract:DeleteWhere"
        assert body["construct_sparql"].startswith("DELETE {")
        assert "sh:rule" not in body["turtle"]

    def test_preview_defaults_to_an_assertion(self, client):
        response = client.post(
            "/ontology/shacl-rules/preview",
            json={"name": "r", "antecedent": "A(?x)", "consequent": "B(?x)"},
        )
        assert response.json()["rule_type"] in ("sh:TripleRule", "sh:SPARQLRule")


class TestShaclExport:
    def test_text_omits_retractions_and_says_so(self, client):
        _save(client, {**RETRACT, "name": "RouteExportRetract"})
        _save(client, {**ASSERT, "name": "RouteExportAssert"})
        text = client.get("/ontology/shacl-rules/text").json()["text"]
        assert "rule_RouteExportAssert" in text
        assert "rule_RouteExportRetract" not in text
        assert "retraction rule(s) omitted" in text


class TestRulesPackage:
    def test_export_then_import_preserves_the_action(self, client):
        _save(client, {**RETRACT, "name": "RoutePkgRetract"})
        exported = json.loads(client.get("/ontology/rules/export").text)
        rule = next(r for r in exported["if_then_rules"] if r["name"] == "RoutePkgRetract")
        assert rule["action"] == "retract"

        body = client.post("/ontology/rules/import", json=exported).json()
        assert body["rejected"] == []
        saved = next(r for r in _rules(client) if r["name"] == "RoutePkgRetract")
        assert saved["action"] == "retract"

    def test_import_rejects_an_invalid_retraction_and_keeps_the_valid_one(self, client):
        payload = {
            "format": "ontobricks-rules",
            "version": 1,
            "if_then_rules": [
                {**RETRACT, "name": "RoutePkgGood"},
                {**RETRACT, "name": "RoutePkgBad", "consequent": "meterOf(?m, ?c, ?m)"},
            ],
            "sparql_rules": [],
        }
        body = client.post("/ontology/rules/import", json=payload).json()
        assert "RoutePkgGood" in body["added"]
        assert [r["name"] for r in body["rejected"]] == ["RoutePkgBad"]
        assert "has 3 arguments" in body["rejected"][0]["reason"]
